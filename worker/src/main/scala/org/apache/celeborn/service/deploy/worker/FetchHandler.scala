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

import java.{lang, util}
import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import com.google.common.base.Throwables
import io.netty.util.concurrent.{Future, GenericFutureListener}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.MAX_CHUNKS_BEING_TRANSFERRED
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{FileInfo, FileManagedBuffers}
import org.apache.celeborn.common.network.buffer.NioManagedBuffer
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol._
import org.apache.celeborn.common.network.protocol.Message.Type
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{MessageType, PartitionType, PbOpenStream, PbStreamHandler}
import org.apache.celeborn.common.util.{ExceptionUtils, Utils}
import org.apache.celeborn.service.deploy.worker.storage.{ChunkStreamManager, CreditStreamManager, PartitionFilesSorter, StorageManager}

class FetchHandler(val conf: CelebornConf, val transportConf: TransportConf)
  extends BaseMessageHandler with Logging {

  val chunkStreamManager = new ChunkStreamManager()
  val maxChunkBeingTransferred: Option[Long] = conf.shuffleIoMaxChunksBeingTransferred

  val creditStreamManager = new CreditStreamManager(
    conf.partitionReadBuffersMin,
    conf.partitionReadBuffersMax,
    conf.creditStreamThreadsPerMountpoint,
    conf.readBuffersToTriggerReadMin)
  var workerSource: WorkerSource = _
  var storageManager: StorageManager = _
  var partitionsSorter: PartitionFilesSorter = _
  var registered: AtomicBoolean = new AtomicBoolean(false)

  def init(worker: Worker): Unit = {
    this.workerSource = worker.workerSource

    workerSource.addGauge(WorkerSource.CREDIT_STREAM_COUNT) { () =>
      creditStreamManager.getStreamsCount
    }

    workerSource.addGauge(WorkerSource.ACTIVE_MAP_PARTITION_COUNT) { () =>
      creditStreamManager.getActiveMapPartitionCount
    }

    this.storageManager = worker.storageManager
    this.partitionsSorter = worker.partitionsSorter
    this.registered = worker.registered
  }

  def getRawFileInfo(
      shuffleKey: String,
      fileName: String): FileInfo = {
    // find FileWriter responsible for the data
    val fileInfo = storageManager.getFileInfo(shuffleKey, fileName)
    if (fileInfo == null) {
      val errMsg = s"Could not find file $fileName for $shuffleKey."
      logWarning(errMsg)
      throw new FileNotFoundException(errMsg)
    }
    fileInfo
  }

  override def receive(client: TransportClient, msg: RequestMessage): Unit = {
    msg match {
      case r: BufferStreamEnd =>
        handleEndStreamFromClient(r)
      case r: ReadAddCredit =>
        handleReadAddCredit(r)
      case r: ChunkFetchRequest =>
        handleChunkFetchRequest(client, r)
      case r: RpcRequest =>
        // 1. parse transportMessage
        try {
          val pbMsg = TransportMessage.fromByteBuffer(r.body().nioByteBuffer())
          val pbOpenStream = pbMsg.getParsedPayload[PbOpenStream]
          handleOpenStream(client, r.requestId, pbOpenStream, () => r.body().release())
        } catch {
          case _: Exception =>
            // 2. parse old rpc message to keep client compatible
            logDebug("use legacy format")
            val msg = Message.decode(r.body().nioByteBuffer())
            handleLegacyOpenStream(client, r, msg)
        }
      case unknown: RequestMessage =>
        throw new IllegalArgumentException(s"Unknown message type id: ${unknown.`type`.id}")
    }
  }

  private def handleOpenStreamInternal(
      client: TransportClient,
      shuffleKey: String,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      initialCredit: Int,
      creditStreamHandler: Consumer[java.lang.Long]): (Long, Int) = {
    workerSource.startTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    var fileInfo = getRawFileInfo(shuffleKey, fileName)
    fileInfo.getPartitionType match {
      case PartitionType.REDUCE =>
        if (endIndex != Integer.MAX_VALUE) {
          fileInfo = partitionsSorter.getSortedFileInfo(
            shuffleKey,
            fileName,
            fileInfo,
            startIndex,
            endIndex)
        }
        logDebug(s"Received chunk fetch request $shuffleKey $fileName " +
          s"$startIndex $endIndex get file info $fileInfo")
        if (fileInfo.isHdfs) {
          (0, 0)
        } else {
          val buffers = new FileManagedBuffers(fileInfo, transportConf)
          val fetchTimeMetrics = storageManager.getFetchTimeMetric(fileInfo.getFile)
          val streamId = chunkStreamManager.registerStream(
            shuffleKey,
            buffers,
            fetchTimeMetrics)
          (streamId, fileInfo.numChunks())
        }
      case PartitionType.MAP =>
        creditStreamManager.registerStream(
          creditStreamHandler,
          client.getChannel,
          initialCredit,
          startIndex,
          endIndex,
          fileInfo)
        (-1, -1)
      case PartitionType.MAPGROUP =>
        (-1, -1)
    }
  }

  def handleOpenStream(
      client: TransportClient,
      requestId: Long,
      msg: PbOpenStream,
      releaseFunc: () => Unit): Unit = {
    val (shuffleKey, fileName, startIndex, endIndex, initialCredit) =
      (msg.getShuffleKey, msg.getFileName, msg.getStartIndex, msg.getEndIndex, msg.getInitialCredit)
    try {
      val (streamId, numChunks) = handleOpenStreamInternal(
        client,
        shuffleKey,
        fileName,
        startIndex,
        endIndex,
        initialCredit,
        new Consumer[lang.Long] {
          override def accept(streamId: lang.Long): Unit = {
            val streamHandler = PbStreamHandler.newBuilder().setStreamId(streamId).build()
            replyStreamHandler(client, requestId, streamHandler)
          }
        })
      replyStreamHandler(client, requestId, fileName, startIndex, endIndex, streamId, numChunks)
    } catch {
      case e: IOException =>
        handleRpcIOException(client, requestId, e)
    } finally {
      workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
      releaseFunc()
    }
  }

  private def replyStreamHandler(
      client: TransportClient,
      requestId: Long,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      streamId: Long,
      numChunks: Int,
      legacy: Boolean = false): Unit = {
    var streamHandler: PbStreamHandler = null
    var legacyStreamHandler: StreamHandle = null
    if (streamId == 0) {
      // read from HDFS return empty stream handler
      if (legacy) {
        legacyStreamHandler = new StreamHandle(0, 0)
      } else {
        streamHandler = PbStreamHandler.newBuilder().build();
      }
    } else if (streamId == -1) {
      // creditStream will handle this open stream
    } else {
      if (numChunks == 0)
        logDebug(s"StreamId $streamId fileName $fileName startMapIndex" +
          s" $startIndex endMapIndex $endIndex is empty.")
      else logDebug(
        s"StreamId $streamId fileName $fileName numChunks ${numChunks} " +
          s"startMapIndex $startIndex endMapIndex $endIndex")
      if (legacy) {
        legacyStreamHandler = new StreamHandle(streamId, numChunks)
      } else {
        streamHandler = PbStreamHandler.newBuilder().setStreamId(streamId).setNumChunks(
          numChunks).build()
      }
    }
    if (streamHandler != null) {
      replyStreamHandler(client, requestId, streamHandler)
    }
    if (legacyStreamHandler != null) {
      replyLegacyStreamHandler(client, requestId, legacyStreamHandler)
    }
  }

  private def replyStreamHandler(
      client: TransportClient,
      requestId: Long,
      streamHandle: PbStreamHandler): Unit = {
    client.getChannel.writeAndFlush(new RpcResponse(
      requestId,
      new NioManagedBuffer(new TransportMessage(
        MessageType.STREAM_HANDLER,
        streamHandle.toByteArray).toByteBuffer)))
  }

  @deprecated
  private def replyLegacyStreamHandler(
      client: TransportClient,
      requestId: Long,
      streamHandle: StreamHandle): Unit = {
    client.getChannel.writeAndFlush(new RpcResponse(
      requestId,
      new NioManagedBuffer(streamHandle.toByteBuffer)))
  }

  // Here are BackLogAnnouncement,OpenStream and OpenStreamWithCredit RPCs to handle
  // Do not add new logic here. This method is reserved for compatible reason.
  @deprecated
  def handleLegacyOpenStream(client: TransportClient, request: RpcRequest, msg: Message): Unit = {
    val (shuffleKey, fileName) =
      if (msg.`type`() == Type.OPEN_STREAM) {
        val openStream = msg.asInstanceOf[OpenStream]
        (
          new String(openStream.shuffleKey, StandardCharsets.UTF_8),
          new String(openStream.fileName, StandardCharsets.UTF_8))
      } else {
        val openStreamWithCredit = msg.asInstanceOf[OpenStreamWithCredit]
        (
          new String(openStreamWithCredit.shuffleKey, StandardCharsets.UTF_8),
          new String(openStreamWithCredit.fileName, StandardCharsets.UTF_8))
      }
    var startIndex = 0
    var endIndex = 0
    var initialCredit = 0
    val creditStreamHandler = new Consumer[java.lang.Long] {
      override def accept(streamId: java.lang.Long): Unit = {
        replyLegacyStreamHandler(client, request.requestId, new StreamHandle(streamId, 0))
      }
    }

    // metrics start
    getRawFileInfo(shuffleKey, fileName).getPartitionType match {
      case PartitionType.REDUCE =>
        startIndex = msg.asInstanceOf[OpenStream].startMapIndex
        endIndex = msg.asInstanceOf[OpenStream].endMapIndex
      case PartitionType.MAP =>
        initialCredit = msg.asInstanceOf[OpenStreamWithCredit].initialCredit
        startIndex = msg.asInstanceOf[OpenStreamWithCredit].startIndex
        endIndex = msg.asInstanceOf[OpenStreamWithCredit].endIndex
      case PartitionType.MAPGROUP =>
    }
    try {
      val (streamId, numChunks) = handleOpenStreamInternal(
        client,
        shuffleKey,
        fileName,
        startIndex,
        endIndex,
        initialCredit,
        creditStreamHandler)
      replyStreamHandler(
        client,
        request.requestId,
        fileName,
        startIndex,
        endIndex,
        streamId,
        numChunks,
        true)
    } catch {
      case e: IOException =>
        handleRpcIOException(client, request.requestId, e)
    } finally {
      workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
      request.body().release()
    }
  }

  private def handleRpcIOException(
      client: TransportClient,
      requestId: Long,
      ioe: IOException): Unit = {
    // if open stream rpc failed, this IOException actually should be FileNotFoundException
    // we wrapper this IOException(Other place may have other exception like FileCorruptException) unify to
    // PartitionUnRetryableException for reader can give up this partition and choose to regenerate the partition data
    client.getChannel.writeAndFlush(new RpcFailure(
      requestId,
      Throwables.getStackTraceAsString(ExceptionUtils.wrapIOExceptionToUnRetryable(ioe, false))))
  }

  def handleEndStreamFromClient(req: BufferStreamEnd): Unit = {
    creditStreamManager.notifyStreamEndByClient(req.getStreamId)
  }

  def handleReadAddCredit(req: ReadAddCredit): Unit = {
    creditStreamManager.addCredit(req.getCredit, req.getStreamId)
  }

  def handleChunkFetchRequest(client: TransportClient, req: ChunkFetchRequest): Unit = {
    logTrace(s"Received req from ${NettyUtils.getRemoteAddress(client.getChannel)}" +
      s" to fetch block ${req.streamChunkSlice}")

    maxChunkBeingTransferred.foreach { threshold =>
      val chunksBeingTransferred = chunkStreamManager.chunksBeingTransferred // take high cpu usage
      if (chunksBeingTransferred > threshold) {
        val message = "Worker is too busy. The number of chunks being transferred " +
          s"$chunksBeingTransferred exceeds ${MAX_CHUNKS_BEING_TRANSFERRED.key} " +
          s"${Utils.bytesToString(threshold)}."
        logError(message)
        client.getChannel.writeAndFlush(new ChunkFetchFailure(req.streamChunkSlice, message))
        return
      }
    }

    workerSource.startTimer(WorkerSource.FETCH_CHUNK_TIME, req.toString)
    val fetchTimeMetric = chunkStreamManager.getFetchTimeMetric(req.streamChunkSlice.streamId)
    val fetchBeginTime = System.nanoTime()
    try {
      val buf = chunkStreamManager.getChunk(
        req.streamChunkSlice.streamId,
        req.streamChunkSlice.chunkIndex,
        req.streamChunkSlice.offset,
        req.streamChunkSlice.len)
      chunkStreamManager.chunkBeingSent(req.streamChunkSlice.streamId)
      client.getChannel.writeAndFlush(new ChunkFetchSuccess(req.streamChunkSlice, buf))
        .addListener(new GenericFutureListener[Future[_ >: Void]] {
          override def operationComplete(future: Future[_ >: Void]): Unit = {
            chunkStreamManager.chunkSent(req.streamChunkSlice.streamId)
            if (fetchTimeMetric != null) {
              fetchTimeMetric.update(System.nanoTime() - fetchBeginTime)
            }
            workerSource.stopTimer(WorkerSource.FETCH_CHUNK_TIME, req.toString)
          }
        })
    } catch {
      case e: Exception =>
        logError(
          s"Error opening block ${req.streamChunkSlice} for request from " +
            NettyUtils.getRemoteAddress(client.getChannel),
          e)
        client.getChannel.writeAndFlush(new ChunkFetchFailure(
          req.streamChunkSlice,
          Throwables.getStackTraceAsString(e)))
        workerSource.stopTimer(WorkerSource.FETCH_CHUNK_TIME, req.toString)
    }
  }

  override def checkRegistered: Boolean = registered.get

  override def channelInactive(client: TransportClient): Unit = {
    creditStreamManager.connectionTerminated(client.getChannel)
    logDebug(s"channel inactive ${client.getSocketAddress}")
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    logWarning(s"exception caught ${client.getSocketAddress}", cause)
  }

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    chunkStreamManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
  }
}
