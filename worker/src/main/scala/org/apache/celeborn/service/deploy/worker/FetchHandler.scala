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

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.Consumer

import com.google.common.base.Throwables
import com.google.protobuf.GeneratedMessageV3
import io.netty.util.concurrent.{Future, GenericFutureListener}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.MAX_CHUNKS_BEING_TRANSFERRED
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{FileInfo, FileManagedBuffers}
import org.apache.celeborn.common.network.buffer.NioManagedBuffer
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol._
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{MessageType, PartitionType, PbBufferStreamEnd, PbOpenStream, PbStreamHandler, StreamType}
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
        handleRpcRequest(client, r)
      case unknown: RequestMessage =>
        throw new IllegalArgumentException(s"Unknown message type id: ${unknown.`type`.id}")
    }
  }

  private def handleRpcRequest(client: TransportClient, rpcRequest: RpcRequest): Unit = {
    try {
      var message: GeneratedMessageV3 = null
      try {
        message = TransportMessage.fromByteBuffer(rpcRequest.body().nioByteBuffer())
          .getParsedPayload[GeneratedMessageV3]
      } catch {
        case exception: CelebornIOException =>
          logWarning("Handle request with legacy RPCs", exception)
          return handleLegacyRpcMessage(client, rpcRequest)
      }
      message match {
        case openStream: PbOpenStream =>
          handleOpenStreamInternal(
            client,
            openStream.getShuffleKey,
            openStream.getFileName,
            openStream.getStartIndex,
            openStream.getEndIndex,
            openStream.getInitialCredit,
            rpcRequest.requestId,
            isLegacy = false,
            openStream.getReadLocalShuffle)
        case bufferStreamEnd: PbBufferStreamEnd =>
          handleEndStreamFromClient(bufferStreamEnd)
        case message: GeneratedMessageV3 =>
          logError(s"Unknown message $message")
      }
    } finally {
      rpcRequest.body().release()
    }
  }

  private def handleLegacyRpcMessage(client: TransportClient, rpcRequest: RpcRequest): Unit = {
    try {
      val message = Message.decode(rpcRequest.body().nioByteBuffer())
      message.`type`() match {
        case Message.Type.OPEN_STREAM =>
          val openStream = message.asInstanceOf[OpenStream]
          handleOpenStreamInternal(
            client,
            new String(openStream.shuffleKey, StandardCharsets.UTF_8),
            new String(openStream.fileName, StandardCharsets.UTF_8),
            openStream.startMapIndex,
            openStream.endMapIndex,
            initialCredit = 0,
            rpcRequestId = rpcRequest.requestId,
            isLegacy = true,
            // legacy [[OpenStream]] doesn't support read local shuffle
            readLocalShuffle = false)
        case Message.Type.OPEN_STREAM_WITH_CREDIT =>
          val openStreamWithCredit = message.asInstanceOf[OpenStreamWithCredit]
          handleOpenStreamInternal(
            client,
            new String(openStreamWithCredit.shuffleKey, StandardCharsets.UTF_8),
            new String(openStreamWithCredit.fileName, StandardCharsets.UTF_8),
            openStreamWithCredit.startIndex,
            openStreamWithCredit.endIndex,
            openStreamWithCredit.initialCredit,
            rpcRequestId = rpcRequest.requestId,
            isLegacy = true,
            readLocalShuffle = false)
        case _ =>
          logError(s"Received an unknown message type id: ${message.`type`.id}")
      }
    } catch {
      case e: Exception =>
        logError("Catch an error when handle legacy rpc message.", e)
    }

  }

  private def handleOpenStreamInternal(
      client: TransportClient,
      shuffleKey: String,
      fileName: String,
      startIndex: Int,
      endIndex: Int,
      initialCredit: Int,
      rpcRequestId: Long,
      isLegacy: Boolean,
      readLocalShuffle: Boolean = false): Unit = {
    workerSource.startTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    try {
      var fileInfo = getRawFileInfo(shuffleKey, fileName)
      fileInfo.getPartitionType match {
        case PartitionType.REDUCE =>
          val streamId = chunkStreamManager.nextStreamId()
          fileInfo = partitionsSorter.getSortedFileInfo(
            shuffleKey,
            fileName,
            fileInfo,
            startIndex,
            endIndex)
          // non-range openStream request
          if (endIndex == Int.MaxValue && !fileInfo.addStream(streamId)) {
            fileInfo = partitionsSorter.getSortedFileInfo(
              shuffleKey,
              fileName,
              fileInfo,
              startIndex,
              endIndex)
          }
          logDebug(s"Received chunk fetch request $shuffleKey $fileName $startIndex " +
            s"$endIndex get file info $fileInfo from client channel " +
            s"${NettyUtils.getRemoteAddress(client.getChannel)}")
          if (readLocalShuffle) {
            replyStreamHandler(
              client,
              rpcRequestId,
              -1,
              fileInfo.numChunks(),
              isLegacy,
              fileInfo.getChunkOffsets,
              fileInfo.getFilePath)
          } else if (fileInfo.isHdfs) {
            replyStreamHandler(client, rpcRequestId, streamId, numChunks = 0, isLegacy)
          } else {
            val buffers = new FileManagedBuffers(fileInfo, transportConf)
            val fetchTimeMetrics = storageManager.getFetchTimeMetric(fileInfo.getFile)
            chunkStreamManager.registerStream(
              streamId,
              shuffleKey,
              buffers,
              fileName,
              fetchTimeMetrics)
            if (fileInfo.numChunks() == 0)
              logDebug(s"StreamId $streamId, fileName $fileName, mapRange " +
                s"[$startIndex-$endIndex] is empty. Received from client channel " +
                s"${NettyUtils.getRemoteAddress(client.getChannel)}")
            else logDebug(
              s"StreamId $streamId, fileName $fileName, numChunks ${fileInfo.numChunks()}, " +
                s"mapRange [$startIndex-$endIndex]. Received from client channel " +
                s"${NettyUtils.getRemoteAddress(client.getChannel)}")
            replyStreamHandler(client, rpcRequestId, streamId, fileInfo.numChunks(), isLegacy)
          }
        case PartitionType.MAP =>
          val creditStreamHandler =
            new Consumer[java.lang.Long] {
              override def accept(streamId: java.lang.Long): Unit = {
                replyStreamHandler(client, rpcRequestId, streamId, 0, isLegacy)
              }
            }

          creditStreamManager.registerStream(
            creditStreamHandler,
            client.getChannel,
            initialCredit,
            startIndex,
            endIndex,
            fileInfo)
        case PartitionType.MAPGROUP =>
      }
    } catch {
      case e: IOException =>
        handleRpcIOException(client, rpcRequestId, shuffleKey, fileName, e)
    } finally {
      workerSource.stopTimer(WorkerSource.OPEN_STREAM_TIME, shuffleKey)
    }
  }

  private def replyStreamHandler(
      client: TransportClient,
      requestId: Long,
      streamId: Long,
      numChunks: Int,
      isLegacy: Boolean,
      offsets: util.List[java.lang.Long] = null,
      filepath: String = ""): Unit = {
    if (isLegacy) {
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(new StreamHandle(streamId, numChunks).toByteBuffer)))
    } else {
      val pbStreamHandlerBuilder = PbStreamHandler.newBuilder.setStreamId(streamId).setNumChunks(
        numChunks)
      if (offsets != null) {
        pbStreamHandlerBuilder.addAllChunkOffsets(offsets)
      }
      if (filepath.nonEmpty) {
        pbStreamHandlerBuilder.setFullPath(filepath)
      }
      val pbStreamHandler = pbStreamHandlerBuilder.build()
      client.getChannel.writeAndFlush(new RpcResponse(
        requestId,
        new NioManagedBuffer(new TransportMessage(
          MessageType.STREAM_HANDLER,
          pbStreamHandler.toByteArray).toByteBuffer)))
    }
  }

  private def handleRpcIOException(
      client: TransportClient,
      requestId: Long,
      shuffleKey: String,
      fileName: String,
      ioe: IOException): Unit = {
    // if open stream rpc failed, this IOException actually should be FileNotFoundException
    // we wrapper this IOException(Other place may have other exception like FileCorruptException) unify to
    // PartitionUnRetryableException for reader can give up this partition and choose to regenerate the partition data
    logError(
      s"Read file: $fileName with shuffleKey: $shuffleKey error from ${NettyUtils.getRemoteAddress(client.getChannel)}",
      ioe)
    client.getChannel.writeAndFlush(new RpcFailure(
      requestId,
      Throwables.getStackTraceAsString(ExceptionUtils.wrapIOExceptionToUnRetryable(ioe))))
  }

  def handleEndStreamFromClient(req: BufferStreamEnd): Unit = {
    creditStreamManager.notifyStreamEndByClient(req.getStreamId)
  }

  def handleEndStreamFromClient(req: PbBufferStreamEnd): Unit = {
    req.getStreamType match {
      case StreamType.ChunkStream =>
        val (shuffleKey, fileName) = chunkStreamManager.getShuffleKeyAndFileName(req.getStreamId)
        getRawFileInfo(shuffleKey, fileName).closeStream(req.getStreamId)
      case StreamType.CreditStream =>
        creditStreamManager.notifyStreamEndByClient(req.getStreamId)
      case _ =>
        logError(s"Received a PbBufferStreamEnd message with unknown type ${req.getStreamType}")
    }
  }

  def handleReadAddCredit(req: ReadAddCredit): Unit = {
    creditStreamManager.addCredit(req.getCredit, req.getStreamId)
  }

  def handleChunkFetchRequest(client: TransportClient, req: ChunkFetchRequest): Unit = {
    logDebug(s"Received req from ${NettyUtils.getRemoteAddress(client.getChannel)}" +
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
            if (future.isSuccess()) {
              if (log.isDebugEnabled) {
                logDebug(
                  s"Sending ChunkFetchSuccess operation succeeded, chunk ${req.streamChunkSlice}")
              }
            } else {
              logError(
                s"Sending ChunkFetchSuccess operation failed, chunk ${req.streamChunkSlice}",
                future.cause())
            }
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

  /** Invoked when the channel associated with the given client is active. */
  override def channelActive(client: TransportClient): Unit = {
    logDebug(s"channel active ${client.getSocketAddress}")
    workerSource.incCounter(WorkerSource.ACTIVE_CONNECTION_COUNT)
    super.channelActive(client)
  }

  override def channelInactive(client: TransportClient): Unit = {
    workerSource.incCounter(WorkerSource.ACTIVE_CONNECTION_COUNT, -1)
    creditStreamManager.connectionTerminated(client.getChannel)
    logDebug(s"channel inactive ${client.getSocketAddress}")
  }

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    logWarning(s"exception caught ${client.getSocketAddress}", cause)
  }

  def cleanupExpiredShuffleKey(expiredShuffleKeys: util.HashSet[String]): Unit = {
    chunkStreamManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
    // maybe null when running unit test.
    if (partitionsSorter != null) {
      partitionsSorter.cleanupExpiredShuffleKey(expiredShuffleKeys)
    }
  }

  def setPartitionsSorter(partitionFilesSorter: PartitionFilesSorter): Unit = {
    this.partitionsSorter = partitionFilesSorter
  }
}
