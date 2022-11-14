/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.base.Throwables
import io.netty.buffer.ByteBuf

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.metrics.source.RPCSource
import org.apache.celeborn.common.network.buffer.{NettyManagedBuffer, NioManagedBuffer}
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.protocol.{PushData, PushMergedData, RequestMessage, RpcFailure, RpcResponse}
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.service.deploy.worker.storage.{FileWriter, LocalFlusher}

class PushDataHandler extends BaseMessageHandler with Logging {

  var workerSource: WorkerSource = _
  var rpcSource: RPCSource = _
  var partitionLocationInfo: PartitionLocationInfo = _
  var shuffleMapperAttempts: ConcurrentHashMap[String, Array[Int]] = _
  var replicateThreadPool: ThreadPoolExecutor = _
  var unavailablePeers: ConcurrentHashMap[WorkerInfo, Long] = _
  var pushClientFactory: TransportClientFactory = _
  var registered: AtomicBoolean = _
  var workerInfo: WorkerInfo = _
  var diskReserveSize: Long = _
  var partitionSplitMinimumSize: Long = _

  def init(worker: Worker): Unit = {
    workerSource = worker.workerSource
    rpcSource = worker.rpcSource
    partitionLocationInfo = worker.partitionLocationInfo
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    replicateThreadPool = worker.replicateThreadPool
    unavailablePeers = worker.unavailablePeers
    pushClientFactory = worker.pushClientFactory
    registered = worker.registered
    workerInfo = worker.workerInfo
    diskReserveSize = worker.conf.diskReserveSize
    partitionSplitMinimumSize = worker.conf.partitionSplitMinimumSize

    logInfo(s"diskReserveSize $diskReserveSize")
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit =
    msg match {
      case pushData: PushData =>
        handleCore(
          client,
          "PushData",
          pushData,
          pushData.requestId,
          (action: String, requestId: Long) =>
            handlePushData(
              pushData,
              new SimpleRpcResponseCallback(
                action,
                client,
                requestId,
                pushData.shuffleKey,
                pushData.partitionUniqueId)))
      case pushMergedData: PushMergedData => {
        handleCore(
          client,
          "PushMergedData",
          pushMergedData,
          pushMergedData.requestId,
          (action: String, requestId: Long) =>
            handlePushMergedData(
              pushMergedData,
              new SimpleRpcResponseCallback(
                action,
                client,
                requestId,
                pushMergedData.shuffleKey,
                pushMergedData.partitionUniqueIds.mkString(","))))
      }
    }

  def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.MASTER
    val action = "PushData"

    // find FileWriter responsible for the data
    val location =
      if (isMaster) {
        partitionLocationInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
      } else {
        partitionLocationInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
      }

    val softSplit = new AtomicBoolean(false)
    val wrappedCallback = createWrappedRpcResponseCallback(
      action,
      isMaster,
      pushData.requestId,
      callback,
      softSplit,
      location)

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      if (shuffleMapperAttempts.containsKey(shuffleKey) &&
        -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
        // partition data has already been committed
        logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
          s" attempt $attemptId), but this mapper has already been ended.")
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
      } else {
        val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
          s"attempt $attemptId, uniqueId ${pushData.partitionUniqueId})."
        logWarning(s"[handlePushData] $msg")
        callback.onFailure(
          new Exception(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getMessage()))
      }
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      logWarning(s"[handlePushData] fileWriter $fileWriter has Exception $exception")
      val message =
        if (isMaster) {
          StatusCode.PUSH_DATA_FAIL_MASTER.getMessage()
        } else {
          StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage()
        }
      callback.onFailure(new Exception(message, exception))
      return
    }
    val diskFull = workerInfo.diskInfos
      .get(fileWriter.flusher.asInstanceOf[LocalFlusher].mountPoint)
      .actualUsableSpace < diskReserveSize
    if ((diskFull && fileWriter.getFileInfo.getFileLength > partitionSplitMinimumSize) ||
      (isMaster && fileWriter.getFileInfo.getFileLength > fileWriter.getSplitThreshold())) {
      if (fileWriter.getSplitMode == PartitionSplitMode.SOFT) {
        softSplit.set(true)
      } else {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        return
      }
    }
    fileWriter.incrementPendingWrites()

    // for master, send data to slave
    if (location.getPeer != null && isMaster) {
      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(
            peer.getHost,
            peer.getRpcPort,
            peer.getPushPort,
            peer.getFetchPort,
            peer.getReplicatePort)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client =
              pushClientFactory.createClient(peer.getHost, peer.getReplicatePort, location.getId)
            val newPushData = new PushData(
              PartitionLocation.Mode.SLAVE.mode(),
              shuffleKey,
              pushData.partitionUniqueId,
              pushData.body)
            client.pushData(newPushData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    try {
      fileWriter.write(body)
    } catch {
      case e: AlreadyClosedException =>
        fileWriter.decrementPendingWrites()
        val (mapId, attemptId) = getMapAttempt(body)
        val endedAttempt =
          if (shuffleMapperAttempts.containsKey(shuffleKey)) {
            shuffleMapperAttempts.get(shuffleKey)(mapId)
          } else -1
        // TODO just info log for ended attempt
        logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
          s" $attemptId), caused by ${e.getMessage}")
      case e: Exception =>
        logError("Exception encountered when write.", e)
    }
  }

  def handlePushMergedData(
      pushMergedData: PushMergedData,
      callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.MASTER
    val action = "PushMergeData"

    val wrappedCallback = createWrappedRpcResponseCallback(
      action,
      isMaster,
      pushMergedData.requestId,
      callback,
      null,
      null)

    // find FileWriters responsible for the data
    val locations = pushMergedData.partitionUniqueIds.map { id =>
      val loc =
        if (isMaster) {
          partitionLocationInfo.getMasterLocation(shuffleKey, id)
        } else {
          partitionLocationInfo.getSlaveLocation(shuffleKey, id)
        }
      if (loc == null) {
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)
          && -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          val msg = s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId), but this mapper has already been ended."
          logInfo(msg)
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId, uniqueId $id)."
          logWarning(s"[handlePushMergedData] $msg")
          wrappedCallback.onFailure(new Exception(msg))
        }
        return
      }
      loc
    }

    val fileWriters = locations.map(_.asInstanceOf[WorkingPartition].getFileWriter)
    val fileWriterWithException = fileWriters.find(_.getException != null)
    if (fileWriterWithException.nonEmpty) {
      val exception = fileWriterWithException.get.getException
      logDebug(s"[handlePushMergedData] fileWriter ${fileWriterWithException}" +
        s" has Exception $exception")
      val message =
        if (isMaster) {
          StatusCode.PUSH_DATA_FAIL_MASTER.getMessage()
        } else {
          StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage()
        }
      callback.onFailure(new Exception(message, exception))
      return
    }
    fileWriters.foreach(_.incrementPendingWrites())

    // for master, send data to slave
    if (locations.head.getPeer != null && isMaster) {
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val location = locations.head
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(
            peer.getHost,
            peer.getRpcPort,
            peer.getPushPort,
            peer.getFetchPort,
            peer.getReplicatePort)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushMergedData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(
              peer.getHost,
              peer.getReplicatePort,
              location.getId)
            val newPushMergedData = new PushMergedData(
              PartitionLocation.Mode.SLAVE.mode(),
              shuffleKey,
              pushMergedData.partitionUniqueIds,
              batchOffsets,
              pushMergedData.body)
            client.pushMergedData(newPushMergedData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushMergedData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    var index = 0
    var fileWriter: FileWriter = null
    var alreadyClosed = false
    while (index < fileWriters.length) {
      fileWriter = fileWriters(index)
      val offset = body.readerIndex() + batchOffsets(index)
      val length =
        if (index == fileWriters.length - 1) {
          body.readableBytes() - batchOffsets(index)
        } else {
          batchOffsets(index + 1) - batchOffsets(index)
        }
      val batchBody = body.slice(offset, length)

      try {
        if (!alreadyClosed) {
          fileWriter.write(batchBody)
        } else {
          fileWriter.decrementPendingWrites()
        }
      } catch {
        case e: AlreadyClosedException =>
          fileWriter.decrementPendingWrites()
          alreadyClosed = true
          val (mapId, attemptId) = getMapAttempt(body)
          val endedAttempt =
            if (shuffleMapperAttempts.containsKey(shuffleKey)) {
              shuffleMapperAttempts.get(shuffleKey)(mapId)
            } else -1
          // TODO just info log for ended attempt
          logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
            s" $attemptId), caused by ${e.getMessage}")
        case e: Exception =>
          logError("Exception encountered when write.", e)
      }
      index += 1
    }
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
    val attemptId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4)
    (mapId, attemptId)
  }

  override def checkRegistered(): Boolean = registered.get()

  class SimpleRpcResponseCallback(
      action: String,
      client: TransportClient,
      requestId: Long,
      shuffleKey: String,
      partitionUniqueId: String)
    extends RpcResponseCallback {
    override def onSuccess(response: ByteBuffer): Unit = {
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(response)))
    }

    override def onFailure(e: Throwable): Unit = {
      logError(
        s"[process$action] Process $action onFailure! ShuffleKey:$shuffleKey , partitionUniqueId: $partitionUniqueId",
        e)
      client.getChannel.writeAndFlush(new RpcFailure(requestId, e.getMessage))
    }
  }

  private def handleCore(
      client: TransportClient,
      action: String,
      message: RequestMessage,
      requestId: Long,
      handler: (String, Long) => Unit): Unit = {
    try {
      rpcSource.updateMessageMetrics(message, message.body().size())
      handler(action, requestId)
    } catch {
      case e: Exception =>
        logError(s"Error while handle$action $message", e);
        client.getChannel.writeAndFlush(new RpcFailure(
          requestId,
          Throwables.getStackTraceAsString(e)));
    } finally {
      message.body().release()
    }
  }

  class WrappedRpcResponseCallback(
      action: String,
      isMaster: Boolean,
      requestId: Long,
      softSplit: AtomicBoolean,
      location: PartitionLocation,
      workerSourceTimeMaster: String,
      workerSourceTimeSlave: String,
      callback: RpcResponseCallback)
    extends RpcResponseCallback {
    override def onSuccess(response: ByteBuffer): Unit = {
      if (isMaster) {
        workerSource.stopTimer(workerSourceTimeMaster, s"$requestId")
        if (response.remaining() > 0) {
          val resp = ByteBuffer.allocate(response.remaining())
          resp.put(response)
          resp.flip()
          callback.onSuccess(resp)
        } else if (softSplit != null && softSplit.get()) {
          callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))
        } else {
          callback.onSuccess(response)
        }
      } else {
        workerSource.stopTimer(workerSourceTimeSlave, s"$requestId")
        callback.onSuccess(response)
      }
    }
    override def onFailure(e: Throwable): Unit = {
      if (location != null) {
        logError(s"[handle$action.onFailure] partitionLocation: $location")
      }
      workerSource.incCounter(WorkerSource.PushDataFailCount)
      callback.onFailure(new Exception(StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage, e))
    }
  }

  private def createWrappedRpcResponseCallback(
      action: String,
      isMaster: Boolean,
      requestId: Long,
      callback: RpcResponseCallback,
      softSplit: AtomicBoolean,
      location: PartitionLocation): RpcResponseCallback = {
    setWorkerSource(
      WorkerSource.MasterPushDataTime,
      WorkerSource.SlavePushDataTime,
      isMaster,
      requestId)
    new WrappedRpcResponseCallback(
      action,
      isMaster,
      requestId,
      softSplit,
      location,
      WorkerSource.MasterPushDataTime,
      WorkerSource.SlavePushDataTime,
      callback)
  }

  private def setWorkerSource(
      workSourceTimeMaster: String,
      workSourceTimeSlave: String,
      isMaster: Boolean,
      requestId: Long): Unit = {
    isMaster match {
      case true => workerSource.startTimer(workSourceTimeMaster, "$requestId")
      case false => workerSource.startTimer(workSourceTimeSlave, "$requestId")
    }
  }
}
