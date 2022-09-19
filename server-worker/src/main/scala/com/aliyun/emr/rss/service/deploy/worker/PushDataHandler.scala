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

package com.aliyun.emr.rss.service.deploy.worker

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ListBuffer

import com.google.common.base.Throwables
import io.netty.buffer.ByteBuf

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.exception.AlreadyClosedException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.source.RPCSource
import com.aliyun.emr.rss.common.network.buffer.{NettyManagedBuffer, NioManagedBuffer}
import com.aliyun.emr.rss.common.network.client.{RpcResponseCallback, TransportClient, TransportClientFactory}
import com.aliyun.emr.rss.common.network.protocol.{PushData, PushMergedData, RequestMessage, RpcFailure, RpcResponse}
import com.aliyun.emr.rss.common.network.server.BaseMessageHandler
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode}
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.unsafe.Platform
import com.aliyun.emr.rss.service.deploy.worker.storage.{FileWriter, LocalFlusher}

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
  var diskMinimumReserveSize: Long = _
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
    diskMinimumReserveSize = RssConf.diskMinimumReserveSize(worker.conf)
    partitionSplitMinimumSize = RssConf.partitionSplitMinimumSize(worker.conf)

    logInfo(s"diskMinimumReserveSize $diskMinimumReserveSize")
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit =
    msg match {
      case pushData: PushData =>
        try {
          rpcSource.updateMessageMetrics(pushData, pushData.body().size())
          handlerPush(pushData, client)
        } catch {
          case e: Exception =>
            logError(s"Error while handlePushData $pushData", e);
            client.getChannel.writeAndFlush(new RpcFailure(
              pushData.requestId,
              Throwables.getStackTraceAsString(e)));
        } finally {
          pushData.body().release()
        }
      case pushMergedData: PushMergedData =>
        try {
          rpcSource.updateMessageMetrics(pushMergedData, pushMergedData.body().size())
          handlerPush(pushMergedData, client)
        } catch {
          case e: Exception =>
            logError(s"Error while handlePushMergedData $pushMergedData", e);
            client.getChannel.writeAndFlush(new RpcFailure(
              pushMergedData.requestId,
              Throwables.getStackTraceAsString(e)));
        } finally {
          pushMergedData.body().release()
        }
    }

  def handlerPush(pushRequest: RequestMessage, client: TransportClient): Unit = {
    val (shuffleKey, mode, body, requestId, partitionUniqueIds, batchOffsets) = pushRequest match {
      case pushData: PushData =>
        (
          pushData.shuffleKey,
          PartitionLocation.getMode(pushData.mode),
          pushData.body.asInstanceOf[NettyManagedBuffer].getBuf,
          pushData.requestId,
          Array(pushData.partitionUniqueId),
          Array(0))
      case pushMergedData: PushMergedData =>
        (
          pushMergedData.shuffleKey,
          PartitionLocation.getMode(pushMergedData.mode),
          pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf,
          pushMergedData.requestId,
          pushMergedData.partitionUniqueIds,
          pushMergedData.batchOffsets)
    }

    val isMaster = mode == PartitionLocation.Mode.MASTER
    val key = s"$requestId"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    val softSplit = new AtomicBoolean(false)
    val callback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
        }
        if (isMaster && response.remaining() == 0 && softSplit.get()) {
          client.getChannel.writeAndFlush(new RpcResponse(
            requestId,
            new NioManagedBuffer(ByteBuffer.wrap(Array[Byte](StatusCode.SOFT_SPLIT.getValue)))))
        } else {
          client.getChannel.writeAndFlush(new RpcResponse(
            requestId,
            new NioManagedBuffer(response)))
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError(
          "[handlePushData.onFailure] Process pushData onFailure! ShuffleKey: "
            + shuffleKey + ", partitionUniqueId: " + partitionUniqueIds.mkString(","),
          e)
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        client.getChannel.writeAndFlush(new RpcFailure(requestId, e.getMessage))
      }
    }

    // find FileWriters responsible for the data
    val locations = ListBuffer.empty[WorkingPartition]
    partitionUniqueIds.find { id =>
      val loc =
        if (isMaster) {
          partitionLocationInfo.getMasterLocation(shuffleKey, id)
        } else {
          partitionLocationInfo.getSlaveLocation(shuffleKey, id)
        }
      if (loc.isDefined) {
        locations += loc.get.asInstanceOf[WorkingPartition]
      }
      loc.isEmpty
    } match {
      case Some(id) =>
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)
          && -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          val msg = s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId), but this mapper has already been ended."
          logInfo(msg)
          callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.STAGE_ENDED.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId, uniqueId $id."
          logWarning(s"[handlePushMergedData] $msg")
          callback.onFailure(
            new Exception(StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND.getMessage))
        }
      case None =>
        val fileWriters = locations.map(_.getFileWriter)
        fileWriters.find(_.getException != null) match {
          case Some(fileWriterWithException) =>
            val exception = fileWriterWithException.getException
            logWarning(s"[handlePushData] fileWriter $fileWriterWithException" +
              s" has Exception $exception")
            val message =
              if (isMaster) {
                StatusCode.PUSH_DATA_FAIL_MAIN.getMessage
              } else {
                StatusCode.PUSH_DATA_FAIL_SLAVE.getMessage
              }
            callback.onFailure(new Exception(message, exception))

          case None =>
            val shouldFlush = pushRequest match {
              case _: PushData =>
                val fileWriter = fileWriters.head
                val diskFull = workerInfo.diskInfos
                  .get(fileWriter.flusher.asInstanceOf[LocalFlusher].mountPoint)
                  .actualUsableSpace < diskMinimumReserveSize
                if ((diskFull && fileWriter.getFileInfo.getFileLength > partitionSplitMinimumSize) ||
                  (isMaster && fileWriter.getFileInfo.getFileLength > fileWriter.getSplitThreshold)) {
                  if (fileWriter.getSplitMode == PartitionSplitMode.HARD) {
                    callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HARD_SPLIT.getValue)))
                    false
                  } else {
                    logInfo("SoftSplit success")
                    softSplit.set(true)
                    true
                  }
                } else {
                  true
                }
              case _ => true
            }

            if (shouldFlush) {
              fileWriters.foreach(_.incrementPendingWrites())
              // for master, send data to slave
              if (locations.head.getPeer != null && isMaster) {
                pushRequest.body().retain()
                replicateData(shuffleKey, locations.head.getPeer, pushRequest, callback)
              } else {
                logInfo("push success")
                callback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
              }
              var alreadyClosed = false
              var fileWriter: FileWriter = null
              for (index <- fileWriters.indices) {
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
                    logWarning(
                      s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
                        s" $attemptId), caused by ${e.getMessage}")
                  case e: Exception =>
                    logError("Exception encountered when write.", e)
                }
              }
            }
        }
    }
  }

  private def replicateData(
      shuffleKey: String,
      peer: PartitionLocation,
      pushRequest: RequestMessage,
      callback: RpcResponseCallback): Unit = {
    replicateThreadPool.submit(new Runnable {
      val peerWorker = new WorkerInfo(
        peer.getHost,
        peer.getRpcPort,
        peer.getPushPort,
        peer.getFetchPort,
        peer.getReplicatePort)

      override def run(): Unit = {
        if (unavailablePeers.containsKey(peerWorker)) {
          pushRequest.body().release()
          callback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
        } else {
          try {
            val client =
              pushClientFactory.createClient(peer.getHost, peer.getReplicatePort, peer.getId)
            pushRequest match {
              case pushData: PushData =>
                val newPushData = new PushData(
                  PartitionLocation.Mode.SLAVE.mode(),
                  shuffleKey,
                  pushData.partitionUniqueId,
                  pushData.body())
                client.pushData(newPushData, callback)
              case pushMergedData: PushMergedData =>
                val newPushMergedData = new PushMergedData(
                  PartitionLocation.Mode.SLAVE.mode(),
                  shuffleKey,
                  pushMergedData.partitionUniqueIds,
                  pushMergedData.batchOffsets,
                  pushMergedData.body())
                client.pushMergedData(newPushMergedData, callback)
            }
          } catch {
            case e: Exception =>
              pushRequest.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              callback.onFailure(e)
          }
        }
      }
    })
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
}
