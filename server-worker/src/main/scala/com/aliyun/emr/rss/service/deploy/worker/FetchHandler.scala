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

import java.io.FileNotFoundException
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.base.Throwables
import io.netty.util.concurrent.{Future, GenericFutureListener}

import com.aliyun.emr.rss.common.exception.RssException
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{FileInfo, FileManagedBuffers}
import com.aliyun.emr.rss.common.metrics.source.RPCSource
import com.aliyun.emr.rss.common.network.buffer.NioManagedBuffer
import com.aliyun.emr.rss.common.network.client.TransportClient
import com.aliyun.emr.rss.common.network.protocol._
import com.aliyun.emr.rss.common.network.server.BaseMessageHandler
import com.aliyun.emr.rss.common.network.server.OneForOneStreamManager
import com.aliyun.emr.rss.common.network.util.NettyUtils
import com.aliyun.emr.rss.common.network.util.TransportConf
import com.aliyun.emr.rss.service.deploy.worker.storage.{PartitionFilesSorter, StorageManager}

class FetchHandler(val conf: TransportConf) extends BaseMessageHandler with Logging {
  var streamManager = new OneForOneStreamManager()
  var workerSource: WorkerSource = _
  var rpcSource: RPCSource = _
  var storageManager: StorageManager = _
  var partitionsSorter: PartitionFilesSorter = _
  var registered: AtomicBoolean = _

  def init(worker: Worker): Unit = {
    this.workerSource = worker.workerSource
    this.rpcSource = worker.rpcSource
    this.storageManager = worker.storageManager
    this.partitionsSorter = worker.partitionsSorter
    this.registered = worker.registered
  }

  def openStream(
      shuffleKey: String,
      fileName: String,
      startMapIndex: Int,
      endMapIndex: Int): FileInfo = {
    // find FileWriter responsible for the data
    val fileInfo = storageManager.getFileInfo(shuffleKey, fileName)
    if (fileInfo == null) {
      logWarning(s"File $fileName for $shuffleKey was not found!")
      null
    } else {
      partitionsSorter.openStream(shuffleKey, fileName, fileInfo, startMapIndex, endMapIndex)
    }
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit = {
    msg match {
      case r: ChunkFetchRequest =>
        handleChunkFetchRequest(client, r)
      case r: RpcRequest =>
        handleOpenStream(client, r)
    }
  }

  def handleOpenStream(client: TransportClient, request: RpcRequest): Unit = {
    val msg = Message.decode(request.body().nioByteBuffer())
    val openBlocks = msg.asInstanceOf[OpenStream]
    val shuffleKey = new String(openBlocks.shuffleKey, StandardCharsets.UTF_8)
    val fileName = new String(openBlocks.fileName, StandardCharsets.UTF_8)
    val startMapIndex = openBlocks.startMapIndex
    val endMapIndex = openBlocks.endMapIndex
    // metrics start
    workerSource.startTimer(WorkerSource.OpenStreamTime, shuffleKey)
    val fileInfo = openStream(shuffleKey, fileName, startMapIndex, endMapIndex)

    if (fileInfo != null) {
      logDebug(s"Received chunk fetch request $shuffleKey $fileName " +
        s"$startMapIndex $endMapIndex get file info $fileInfo")
      try {
        val buffers = new FileManagedBuffers(fileInfo, conf)
        val streamId = streamManager.registerStream(buffers, client.getChannel)
        val streamHandle = new StreamHandle(streamId, fileInfo.numChunks())
        if (fileInfo.numChunks() == 0) {
          logDebug(s"StreamId $streamId fileName $fileName startMapIndex" +
            s" $startMapIndex endMapIndex $endMapIndex is empty.")
        }
        client.getChannel.writeAndFlush(new RpcResponse(request.requestId,
          new NioManagedBuffer(streamHandle.toByteBuffer)))
      } catch {
        case e: IOException =>
          client.getChannel.writeAndFlush(new RpcFailure(request.requestId,
            Throwables.getStackTraceAsString(new RssException("Chunk offsets meta exception ", e))))
      } finally {
        // metrics end
        workerSource.stopTimer(WorkerSource.OpenStreamTime, shuffleKey)
        request.body().release()
      }
    } else {
      workerSource.stopTimer(WorkerSource.OpenStreamTime, shuffleKey)
      client.getChannel.writeAndFlush(new RpcFailure(request.requestId,
        Throwables.getStackTraceAsString(new FileNotFoundException)))
    }
  }

  def handleChunkFetchRequest(client: TransportClient, req: ChunkFetchRequest): Unit = {
    workerSource.startTimer(WorkerSource.FetchChunkTime, req.toString)
    rpcSource.updateMessageMetrics(req, 0)
    logTrace(s"Received req from ${NettyUtils.getRemoteAddress(client.getChannel)}" +
      s" to fetch block ${req.streamChunkSlice}")

    val chunksBeingTransferred = streamManager.chunksBeingTransferred
    if (chunksBeingTransferred >= conf.maxChunksBeingTransferred) {
      logError(s"The number of chunks being transferred $chunksBeingTransferred" +
        s"is above ${conf.maxChunksBeingTransferred()}.")
      workerSource.stopTimer(WorkerSource.FetchChunkTime, req.toString)
    } else {
      try {
        val buf = streamManager.getChunk(req.streamChunkSlice.streamId,
          req.streamChunkSlice.chunkIndex, req.streamChunkSlice.offset, req.streamChunkSlice.len)
        streamManager.chunkBeingSent(req.streamChunkSlice.streamId)
        client.getChannel.writeAndFlush(new ChunkFetchSuccess(req.streamChunkSlice, buf))
          .addListener(new GenericFutureListener[Future[_ >: Void]] {
            override def operationComplete(future: Future[_ >: Void]): Unit = {
              streamManager.chunkSent(req.streamChunkSlice.streamId)
              workerSource.stopTimer(WorkerSource.FetchChunkTime, req.toString)
            }
          })
      } catch {
        case e: Exception =>
          logError(String.format(s"Error opening block ${req.streamChunkSlice} for request from" +
            s" ${NettyUtils.getRemoteAddress(client.getChannel)}"), e)
          client.getChannel.writeAndFlush(new ChunkFetchFailure(req.streamChunkSlice,
            Throwables.getStackTraceAsString(e)))
          workerSource.stopTimer(WorkerSource.FetchChunkTime, req.toString)
      }
    }
  }

  override def checkRegistered: Boolean = registered.get

  override def channelInactive(client: TransportClient): Unit = {
    logDebug("channel Inactive " + client.getSocketAddress)
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    logDebug("exception caught " + cause + " " + client.getSocketAddress)
  }
}
