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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicIntegerArray}

import com.google.common.base.Throwables
import io.netty.buffer.ByteBuf

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.metrics.source.RPCSource
import org.apache.celeborn.common.network.buffer.{NettyManagedBuffer, NioManagedBuffer}
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import org.apache.celeborn.common.network.protocol.{Message, PushData, PushDataHandShake, PushMergedData, RegionFinish, RegionStart, RequestMessage, RpcFailure, RpcRequest, RpcResponse}
import org.apache.celeborn.common.network.protocol.Message.Type
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.PackedPartitionId
import org.apache.celeborn.service.deploy.worker.storage.{FileWriter, LocalFlusher, StorageManager}

class PushDataHandler extends BaseMessageHandler with Logging {

  var workerSource: WorkerSource = _
  var rpcSource: RPCSource = _
  var partitionLocationInfo: PartitionLocationInfo = _
  var shuffleMapperAttempts: ConcurrentHashMap[String, AtomicIntegerArray] = _
  var shufflePartitionType: ConcurrentHashMap[String, PartitionType] = _
  var replicateThreadPool: ThreadPoolExecutor = _
  var unavailablePeers: ConcurrentHashMap[WorkerInfo, Long] = _
  var pushClientFactory: TransportClientFactory = _
  var registered: AtomicBoolean = new AtomicBoolean(false)
  var workerInfo: WorkerInfo = _
  var diskReserveSize: Long = _
  var partitionSplitMinimumSize: Long = _
  var shutdown: AtomicBoolean = _
  var storageManager: StorageManager = _
  var conf: CelebornConf = _
  @volatile var pushDataTimeoutTested = false

  def init(worker: Worker): Unit = {
    workerSource = worker.workerSource
    rpcSource = worker.rpcSource
    partitionLocationInfo = worker.partitionLocationInfo
    shufflePartitionType = worker.shufflePartitionType
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    replicateThreadPool = worker.replicateThreadPool
    unavailablePeers = worker.unavailablePeers
    pushClientFactory = worker.pushClientFactory
    registered = worker.registered
    workerInfo = worker.workerInfo
    diskReserveSize = worker.conf.diskReserveSize
    partitionSplitMinimumSize = worker.conf.partitionSplitMinimumSize
    storageManager = worker.storageManager
    shutdown = worker.shutdown
    conf = worker.conf

    logInfo(s"diskReserveSize $diskReserveSize")
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit =
    msg match {
      case pushData: PushData =>
        handleCore(
          client,
          pushData,
          pushData.requestId,
          () =>
            handlePushData(
              pushData,
              new SimpleRpcResponseCallback(
                Type.PUSH_DATA,
                client,
                pushData.requestId,
                pushData.shuffleKey,
                pushData.partitionUniqueId)))
      case pushMergedData: PushMergedData => {
        handleCore(
          client,
          pushMergedData,
          pushMergedData.requestId,
          () =>
            handlePushMergedData(
              pushMergedData,
              new SimpleRpcResponseCallback(
                Type.PUSH_MERGED_DATA,
                client,
                pushMergedData.requestId,
                pushMergedData.shuffleKey,
                pushMergedData.partitionUniqueIds.mkString(","))))
      }
      case rpcRequest: RpcRequest => handleRpcRequest(client, rpcRequest)
    }

  def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.MASTER

    // For test
    if (conf.testPushDataTimeout && !pushDataTimeoutTested) {
      pushDataTimeoutTested = true
      return
    }

    val key = s"${pushData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    // find FileWriter responsible for the data
    val location =
      if (isMaster) {
        partitionLocationInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
      } else {
        partitionLocationInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
      }

    val softSplit = new AtomicBoolean(false)
    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else if (softSplit.get()) {
            callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"[handlePushData.onFailure] partitionLocation: $location")
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage(), e))
      }
    }

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      // MapperAttempts for a shuffle exists after any CommitFiles request succeeds.
      // A shuffle can trigger multiple CommitFiles requests, for reasons like: Hard-Split happens, StageEnd.
      // If MapperAttempts but the value is -1 for the mapId(-1 means the map has not yet finished),
      // it's probably because commitFiles for Had-Split happens.
      if (shuffleMapperAttempts.containsKey(shuffleKey)) {
        if (-1 != shuffleMapperAttempts.get(shuffleKey).get(mapId)) {
          // partition data has already been committed
          logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
            s" attempt $attemptId), but this mapper has already been ended.")
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
        } else {
          logInfo(
            s"Receive push data for committed hard split partition of (shuffle $shuffleKey, " +
              s"map $mapId attempt $attemptId)")
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        }
      } else {
        if (storageManager.shuffleKeySet().contains(shuffleKey)) {
          // If there is no shuffle key in shuffleMapperAttempts but there is shuffle key
          // in StorageManager. This partition should be HARD_SPLIT partition and
          // after worker restart, some task still push data to this HARD_SPLIT partition.
          logInfo(
            s"Receive push data for committed hard split partition of (shuffle $shuffleKey, " +
              s"map $mapId attempt $attemptId)")
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
            s"attempt $attemptId, uniqueId ${pushData.partitionUniqueId})."
          logWarning(s"[handlePushData] $msg")
          callback.onFailure(
            new Exception(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getMessage()))
        }
      }
      return
    }

    // During worker shutdown, worker will return HARD_SPLIT for all existed partition.
    // This should before return exception to make current push data can revive and retry.
    if (shutdown.get()) {
      logInfo(s"Push data return HARD_SPLIT for shuffle $shuffleKey since worker shutdown.")
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
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
            shuffleMapperAttempts.get(shuffleKey).get(mapId)
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

    val key = s"${pushMergedData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    // For test
    if (conf.testPushDataTimeout && !PushDataHandler.pushDataTimeoutTested) {
      PushDataHandler.pushDataTimeoutTested = true
      return
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage, e))
      }
    }

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
        // MapperAttempts for a shuffle exists after any CommitFiles request succeeds.
        // A shuffle can trigger multiple CommitFiles requests, for reasons like: Hard-Split happens, StageEnd.
        // If MapperAttempts but the value is -1 for the mapId(-1 means the map has not yet finished),
        // it's probably because commitFiles for Had-Split happens.
        if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          if (-1 != shuffleMapperAttempts.get(shuffleKey).get(mapId)) {
            val msg =
              s"Receive push merged data from speculative task(shuffle $shuffleKey, map $mapId," +
                s" attempt $attemptId), but this mapper has already been ended."
            logInfo(msg)
            wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
          } else {
            logInfo(
              s"Receive push merged data for committed hard split partition of (shuffle $shuffleKey, " +
                s"map $mapId attempt $attemptId)")
            wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
          }
        } else {
          if (storageManager.shuffleKeySet().contains(shuffleKey)) {
            // If there is no shuffle key in shuffleMapperAttempts but there is shuffle key
            // in StorageManager. This partition should be HARD_SPLIT partition and
            // after worker restart, some task still push data to this HARD_SPLIT partition.
            logInfo(
              s"Receive push merged data for committed hard split partition of (shuffle $shuffleKey, " +
                s"map $mapId attempt $attemptId)")
            wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
          } else {
            val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
              s" attempt $attemptId, uniqueId $id)."
            logWarning(s"[handlePushMergedData] $msg")
            callback.onFailure(
              new Exception(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getMessage()))
          }
        }
        return
      }
      loc
    }

    // During worker shutdown, worker will return HARD_SPLIT for all existed partition.
    // This should before return exception to make current push data can revive and retry.
    if (shutdown.get()) {
      callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
      return
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
              shuffleMapperAttempts.get(shuffleKey).get(mapId)
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
      messageType: Message.Type,
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
        s"[process$messageType] Process $messageType onFailure! ShuffleKey:$shuffleKey , partitionUniqueId: $partitionUniqueId",
        e)
      client.getChannel.writeAndFlush(new RpcFailure(requestId, e.getMessage))
    }
  }

  private def handleCore(
      client: TransportClient,
      message: RequestMessage,
      requestId: Long,
      handler: () => Unit): Unit = {
    try {
      rpcSource.updateMessageMetrics(message, message.body().size())
      handler()
    } catch {
      case e: Exception =>
        logError(s"Error while handle${message.`type`()} $message", e)
        client.getChannel.writeAndFlush(new RpcFailure(
          requestId,
          Throwables.getStackTraceAsString(e)));
    } finally {
      message.body().release()
    }
  }

  private def handleRpcRequest(client: TransportClient, rpcRequest: RpcRequest): Unit = {
    val msg = Message.decode(rpcRequest.body().nioByteBuffer())
    val requestId = rpcRequest.requestId
    val (mode, shuffleKey, partitionUniqueId, isCheckSplit) = msg match {
      case p: PushDataHandShake => (p.mode, p.shuffleKey, p.partitionUniqueId, false)
      case rs: RegionStart => (rs.mode, rs.shuffleKey, rs.partitionUniqueId, true)
      case rf: RegionFinish => (rf.mode, rf.shuffleKey, rf.partitionUniqueId, false)
    }
    handleCore(
      client,
      rpcRequest,
      requestId,
      () =>
        handleRpcRequestCore(
          mode,
          msg,
          shuffleKey,
          partitionUniqueId,
          requestId,
          isCheckSplit,
          new SimpleRpcResponseCallback(
            msg.`type`(),
            client,
            requestId,
            shuffleKey,
            partitionUniqueId)))

  }

  private def handleRpcRequestCore(
      mode: Byte,
      message: Message,
      shuffleKey: String,
      partitionUniqueId: String,
      requestId: Long,
      isCheckSplit: Boolean,
      callback: RpcResponseCallback): Unit = {
    val isMaster = mode == PartitionLocation.Mode.MASTER
    val messageType = message.`type`()
    log.info(s"requestId:$requestId, pushdata rpc:$messageType, mode:$mode, shuffleKey:$shuffleKey, partitionUniqueId:$partitionUniqueId")
    val (workerSourceMaster, workerSourceSlave) =
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE =>
          (WorkerSource.MasterPushDataHandshakeTime, WorkerSource.SlavePushDataHandshakeTime)
        case Type.REGION_START =>
          (WorkerSource.MasterRegionStartTime, WorkerSource.SlaveRegionStartTime)
        case Type.REGION_FINISH =>
          (WorkerSource.MasterRegionFinishTime, WorkerSource.SlaveRegionFinishTime)
      }

    val location = isMaster match {
      case true => partitionLocationInfo.getMasterLocation(shuffleKey, partitionUniqueId)
      case false => partitionLocationInfo.getSlaveLocation(shuffleKey, partitionUniqueId)
    }
    workerSource.startTimer(if (isMaster) workerSourceMaster else workerSourceSlave, s"$requestId")
    val wrappedCallback =
      new WrappedRpcResponseCallback(
        messageType,
        isMaster,
        requestId,
        null,
        location,
        if (isMaster) workerSourceMaster else workerSourceSlave,
        callback)

    if (checkLocationNull(
        messageType,
        shuffleKey,
        partitionUniqueId,
        null,
        location,
        callback,
        wrappedCallback)) return

    val fileWriter =
      getFileWriterAndCheck(messageType, location, isMaster, callback) match {
        case (true, _) => return
        case (false, f: FileWriter) => f
      }

    if (isCheckSplit && checkDiskFullAndSplit(fileWriter, isMaster, null, callback)) return

    try {
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE => {
          fileWriter.pushDataHandShake(message.asInstanceOf[PushDataHandShake].numPartitions)
        }
        case Type.REGION_START => {
          fileWriter.regionStart(
            message.asInstanceOf[RegionStart].currentRegionIndex,
            message.asInstanceOf[RegionStart].isBroadcast)
        }
        case Type.REGION_FINISH => {
          fileWriter.regionFinish()
        }
      }
      // for master, send data to slave
      if (location.getPeer != null && isMaster) {
        // to do replica
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
      } else {
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
      }
    } catch {
      case t: Throwable =>
        callback.onFailure(new Exception(s"$messageType failed", t))
    }
  }

  class WrappedRpcResponseCallback(
      messageType: Message.Type,
      isMaster: Boolean,
      requestId: Long,
      softSplit: AtomicBoolean,
      location: PartitionLocation,
      workerSourceTime: String,
      callback: RpcResponseCallback)
    extends RpcResponseCallback {
    override def onSuccess(response: ByteBuffer): Unit = {
      workerSource.stopTimer(workerSourceTime, s"$requestId")
      if (isMaster) {
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
        callback.onSuccess(response)
      }
    }

    override def onFailure(e: Throwable): Unit = {
      if (location != null) {
        logError(s"[handle$messageType.onFailure] partitionLocation: $location")
      }
      messageType match {
        case Type.PUSH_DATA_HAND_SHAKE =>
          workerSource.incCounter(WorkerSource.PushDataHandshakeFailCount)
          callback.onFailure(new Exception(
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_SLAVE.getMessage,
            e))
        case Type.REGION_START =>
          workerSource.incCounter(WorkerSource.RegionStartFailCount)
          callback.onFailure(new Exception(StatusCode.REGION_START_FAIL_SLAVE.getMessage, e))
        case Type.REGION_FINISH =>
          workerSource.incCounter(WorkerSource.RegionFinishFailCount)
          callback.onFailure(new Exception(StatusCode.REGION_FINISH_FAIL_SLAVE.getMessage, e))
        case _ =>
          workerSource.incCounter(WorkerSource.PushDataFailCount)
          callback.onFailure(new Exception(StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage, e))
      }
    }
  }

  private def checkLocationNull(
      messageType: Message.Type,
      shuffleKey: String,
      partitionUniqueId: String,
      body: ByteBuf,
      location: PartitionLocation,
      callback: RpcResponseCallback,
      wrappedCallback: RpcResponseCallback): Boolean = {
    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body, shuffleKey, partitionUniqueId)
      if (shuffleMapperAttempts.containsKey(shuffleKey) &&
        -1 != shuffleMapperAttempts.get(shuffleKey).get(mapId)) {
        // partition data has already been committed
        logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
          s" attempt $attemptId), but this mapper has already been ended.")
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
      } else {
        val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
          s"attempt $attemptId, uniqueId $partitionUniqueId)."
        logWarning(s"[handle$messageType] $msg")
        messageType match {
          case Type.PUSH_MERGED_DATA => callback.onFailure(new Exception(msg))
          case _ => callback.onFailure(
              new Exception(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getMessage()))
        }
      }
      return true
    }
    false
  }

  private def checkFileWriterException(
      messageType: Message.Type,
      isMaster: Boolean,
      fileWriter: FileWriter,
      callback: RpcResponseCallback): Unit = {
    logWarning(
      s"[handle$messageType] fileWriter $fileWriter has Exception ${fileWriter.getException}")

    val (messageMaster, messageSlave) =
      messageType match {
        case Type.PUSH_DATA | Type.PUSH_DATA_HAND_SHAKE =>
          (
            StatusCode.PUSH_DATA_FAIL_MASTER.getMessage(),
            StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage())
        case Type.PUSH_DATA_HAND_SHAKE => (
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_MASTER.getMessage,
            StatusCode.PUSH_DATA_HANDSHAKE_FAIL_SLAVE.getMessage)
        case Type.REGION_START => (
            StatusCode.REGION_START_FAIL_MASTER.getMessage,
            StatusCode.REGION_START_FAIL_SLAVE.getMessage)
        case Type.REGION_FINISH => (
            StatusCode.REGION_FINISH_FAIL_MASTER.getMessage,
            StatusCode.REGION_FINISH_FAIL_SLAVE.getMessage)
      }
    callback.onFailure(new Exception(
      if (isMaster) messageMaster else messageSlave,
      fileWriter.getException))
  }

  private def getFileWriterAndCheck(
      messageType: Message.Type,
      location: PartitionLocation,
      isMaster: Boolean,
      callback: RpcResponseCallback): (Boolean, FileWriter) = {
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      checkFileWriterException(messageType, isMaster, fileWriter, callback)
      return (true, fileWriter)
    }
    (false, fileWriter)
  }

  private def checkDiskFull(fileWriter: FileWriter): Boolean = {
    val diskFull = workerInfo.diskInfos
      .get(fileWriter.flusher.asInstanceOf[LocalFlusher].mountPoint)
      .actualUsableSpace < diskReserveSize
    diskFull
  }

  private def checkDiskFullAndSplit(
      fileWriter: FileWriter,
      isMaster: Boolean,
      softSplit: AtomicBoolean,
      callback: RpcResponseCallback): Boolean = {
    val diskFull = checkDiskFull(fileWriter)
    if ((diskFull && fileWriter.getFileInfo.getFileLength > partitionSplitMinimumSize) ||
      (isMaster && fileWriter.getFileInfo.getFileLength > fileWriter.getSplitThreshold())) {
      if (softSplit != null && fileWriter.getSplitMode == PartitionSplitMode.SOFT) {
        softSplit.set(true)
      } else {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
        return true
      }
    }
    false
  }

  private def getMapAttempt(
      body: ByteBuf,
      shuffleKey: String,
      partitionUniqueId: String): (Int, Int) = {
    shufflePartitionType.get(shuffleKey) match {
      case PartitionType.MAP => {
        val id = partitionUniqueId.split("-")(0).toInt
        (PackedPartitionId.getRawPartitionId(id), PackedPartitionId.getAttemptId(id))
      }
      case PartitionType.REDUCE => getMapAttempt(body)
    }
  }
}

object PushDataHandler {
  @volatile var pushDataTimeoutTested = false
}
