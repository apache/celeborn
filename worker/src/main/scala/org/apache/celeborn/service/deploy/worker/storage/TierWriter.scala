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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters.asScalaBufferConverter

import io.netty.buffer.{ByteBuf, CompositeByteBuf}
import org.apache.hadoop.fs.FileSystem

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.FileChannelUtils
import org.apache.celeborn.server.common.service.mpu.MultipartUploadHandler
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.congestcontrol.{CongestionController, UserCongestionControlContext}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

abstract class TierWriterBase(
    val conf: CelebornConf,
    val metaHandler: PartitionMetaHandler,
    val numPendingWrites: AtomicInteger,
    val notifier: FlushNotifier,
    val fileInfo: FileInfo,
    val source: AbstractSource,
    val storageType: StorageInfo.Type,
    val filename: String,
    val shuffleKey: String,
    val storageManager: StorageManager) extends Logging {
  val metricsCollectCriticalEnabled: Boolean = conf.metricsCollectCriticalEnabled
  private val WAIT_INTERVAL_MS = 5

  var flushBuffer: CompositeByteBuf = _
  var writerCloseTimeoutMs: Long = conf.workerWriterCloseTimeoutMs
  var flusherBufferSize = 0L
  private val chunkSize: Long = conf.shuffleChunkSize
  val flushLock: AnyRef = new AnyRef

  @volatile var closed: Boolean = false
  @volatile private var destroyed: Boolean = false

  takeBuffer()

  def write(buf: ByteBuf): Unit = {
    ensureNotClosed()
    if (notifier.hasException) {
      handleException()
      return
    }

    flushLock.synchronized {
      metaHandler.beforeWrite(buf)
      ensureNotClosed()
      writeInternal(buf)
    }

    metaHandler.afterWrite(buf.readableBytes())
    numPendingWrites.decrementAndGet()
  }

  def handleException(): Unit

  protected def writeInternal(buf: ByteBuf): Unit

  def needEvict(): Boolean

  def evict(file: TierWriterBase): Unit

  def swapFlushBuffer(inputBuffer: CompositeByteBuf): Unit = {
    if (flushBuffer != null) {
      returnBuffer(false)
    }
    flushBuffer = inputBuffer
  }

  // close and destroy need to be invoked in synchronized blocks
  def close(): Long = {
    ensureNotClosed()
    try {
      waitOnNoPending(numPendingWrites, false)
      closed = true
      finalFlush()
      metaHandler.afterClose()
      waitOnNoPending(notifier.numPendingFlushes, true)
    } finally {
      returnBuffer(false)
      try {
        closeStreams()
      } catch {
        case e: IOException =>
          logWarning(s"close file writer $this failed", e)
      }
    }
    notifyFileCommitted()

    fileInfo.getFileLength
  }

  private def ensureNotClosed(): Unit = {
    if (closed) {
      val msg = getFileAlreadyClosedMsg
      logWarning(msg)
      throw new AlreadyClosedException(msg)
    }
  }

  private def getFileAlreadyClosedMsg: String = {
    s"PartitionDataWriter has already closed! File name:$filename"
  }

  // this method is not used in memory tier writer
  def notifyFileCommitted(): Unit = {}

  // this method is not used in memory tier writer
  def finalFlush(): Unit = {}

  @throws[IOException]
  private def waitOnNoPending(counter: AtomicInteger, failWhenTimeout: Boolean): Unit = {
    var waitTime = writerCloseTimeoutMs
    while (counter.get > 0 && waitTime > 0) {
      try {
        notifier.checkException()
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS)
      } catch {
        case e: InterruptedException =>
          val ioe = new IOException(e)
          notifier.setException(ioe)
          throw ioe
      }
      waitTime -= WAIT_INTERVAL_MS
    }
    if (counter.get > 0 && failWhenTimeout) {
      val ioe = new IOException("Wait pending actions timeout.")
      notifier.setException(ioe)
      throw ioe
    }
    notifier.checkException()
  }

  def closeStreams(): Unit

  def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask

  def flush(finalFlush: Boolean, rebuildChunkOffsets: Boolean = false): Unit = {
    if (flushBuffer != null) {
      val numBytes = flushBuffer.readableBytes()
      var flushTask: FlushTask = null
      if (numBytes != 0) {
        if (rebuildChunkOffsets) {
          val dupBuf = flushBuffer.retainedDuplicate()
          // this flusher buffer is from memory tier writer, so that we can not keep the buffer
          flushTask = genFlushTask(finalFlush, false)
          if (numBytes > chunkSize) {
            val headerBuf = ByteBuffer.allocate(16)
            while (dupBuf.isReadable) {
              headerBuf.rewind
              dupBuf.readBytes(headerBuf)
              val batchHeader = headerBuf.array
              val compressedSize = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12)
              dupBuf.skipBytes(compressedSize)
              metaHandler.afterFlush(compressedSize + 16)
            }
            dupBuf.release
          } else {
            metaHandler.afterFlush(numBytes)
          }
        } else {
          notifier.checkException()
          // if a flush buffer is larger than the chunk size, it might contain data of multiple chunks
          flushTask = genFlushTask(finalFlush, true)
        }
      }
      // task won't be null in real workloads unless it is a memory file
      // task will be null in UT to check chunk size and offset
      if (flushTask != null) {
        addFlushTask(flushTask)
        flushBuffer = null
        if (!rebuildChunkOffsets) {
          metaHandler.afterFlush(numBytes)
        }
        if (!finalFlush) {
          takeBuffer()
        }
      }
    }

  }

  private def takeBuffer(): Unit = {
    var metricsName: String = null
    var fileAbsPath: String = null
    if (metricsCollectCriticalEnabled) {
      metricsName = WorkerSource.TAKE_BUFFER_TIME
      fileAbsPath = fileInfo.getFilePath
      source.startTimer(metricsName, fileAbsPath)
    }

    flushLock.synchronized {
      flushBuffer = takeBufferInternal()
    }

    if (metricsCollectCriticalEnabled) source.stopTimer(metricsName, fileAbsPath)
  }

  def addFlushTask(task: FlushTask): Unit

  def takeBufferInternal(): CompositeByteBuf

  def destroy(ioException: IOException): Unit = {
    if (!closed) {
      closed = true
      if (!notifier.hasException) {
        notifier.setException(ioException)
      }
      metaHandler.beforeDestroy()
      returnBuffer(true)
      closeResource()
    }

    if (!destroyed) {
      destroyed = true
      cleanLocalOrDfsFiles()
    }
  }

  def returnBuffer(destroy: Boolean): Unit = {
    flushLock.synchronized {
      returnBufferInternal(destroy)
    }
  }

  def closeResource(): Unit = {}

  def cleanLocalOrDfsFiles(): Unit = {}

  def returnBufferInternal(destroy: Boolean): Unit

  def getFlusher(): Flusher
}

class MemoryTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWriters: AtomicInteger,
    notifier: FlushNotifier,
    source: AbstractSource,
    fileInfo: MemoryFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWriters,
    notifier,
    fileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {

  val memoryFileStorageMaxFileSize: Long = conf.workerMemoryFileStorageMaxFileSize

  storageManager.registerMemoryPartitionWriter(
    partitionDataWriterContext.getPartitionDataWriter,
    fileInfo)

  override def needEvict(): Boolean = {
    flushBuffer.readableBytes() > memoryFileStorageMaxFileSize && storageManager.localOrDfsStorageAvailable
  }

  override def evict(file: TierWriterBase): Unit = {
    flushLock.synchronized {
      // swap tier writer's flush buffer to memory tier writer's
      // and handle its release
      file.swapFlushBuffer(flushBuffer)
      // close memory file writer after evict happened
      file.flush(false, true)
      val numBytes = flushBuffer.readableBytes()
      logDebug(s"Evict $numBytes from memory to other tier")
      MemoryManager.instance.releaseMemoryFileStorage(numBytes)
      MemoryManager.instance.incrementDiskBuffer(numBytes)
      storageManager.unregisterMemoryPartitionWriterAndFileInfo(fileInfo, shuffleKey, filename)
      storageManager.evictedFileCount.incrementAndGet
    }
  }

  // Memory file won't produce flush task
  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = null

  override def writeInternal(buf: ByteBuf): Unit = {
    buf.retain()
    val numBytes = buf.readableBytes()
    try {
      flushBuffer.addComponent(true, buf)
    } catch {
      case oom: OutOfMemoryError =>
        // memory tier writer will not flush
        // add the bytes into flusher buffer is flush completed
        metaHandler.afterFlush(numBytes)
        MemoryManager.instance.incrementMemoryFileStorage(numBytes)
        throw oom
    }
    // memory tier writer will not flush
    // add the bytes into flusher buffer is flush completed
    metaHandler.afterFlush(numBytes)
    MemoryManager.instance().incrementMemoryFileStorage(numBytes)
  }

  override def closeStreams(): Unit = {
    flushBuffer.consolidate()
    fileInfo.setBuffer(flushBuffer)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    storageManager.storageBufferAllocator.compositeBuffer(Integer.MAX_VALUE)
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (destroy && flushBuffer != null) {
      flushBuffer.removeComponents(0, flushBuffer.numComponents)
      flushBuffer.release
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    // memory tier write does not need flush tasks
  }

  override def getFlusher(): Flusher = {
    null
  }

  override def handleException(): Unit = {}
}

class LocalTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWrites: AtomicInteger,
    notifier: FlushNotifier,
    flusher: Flusher,
    source: AbstractSource,
    diskFileInfo: DiskFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWrites,
    notifier,
    diskFileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {
  flusherBufferSize = conf.workerFlusherBufferSize
  private val flushWorkerIndex: Int = flusher.getWorkerIndex
  val userCongestionControlContext: UserCongestionControlContext =
    if (CongestionController.instance != null)
      CongestionController.instance.getUserCongestionContext(
        partitionDataWriterContext.getUserIdentifier)
    else
      null
  storageManager.registerDiskFilePartitionWriter(
    partitionDataWriterContext.getPartitionDataWriter,
    partitionDataWriterContext.getWorkingDir,
    fileInfo.asInstanceOf[DiskFileInfo])

  private lazy val channel: FileChannel =
    FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath)

  val gatherApiEnabled: Boolean = conf.workerFlusherLocalGatherAPIEnabled

  override def needEvict(): Boolean = {
    false
  }

  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = {
    notifier.numPendingFlushes.incrementAndGet()
    new LocalFlushTask(flushBuffer, channel, notifier, true, gatherApiEnabled)
  }

  override def writeInternal(buf: ByteBuf): Unit = {
    val numBytes = buf.readableBytes()
    val flushBufferReadableBytes = flushBuffer.readableBytes
    if (flushBufferReadableBytes != 0 && flushBufferReadableBytes + numBytes >= flusherBufferSize) {
      flush(false)
    }
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
    } catch {
      case oom: OutOfMemoryError =>
        MemoryManager.instance.incrementDiskBuffer(numBytes)
        if (userCongestionControlContext != null)
          userCongestionControlContext.updateProduceBytes(numBytes)
        throw oom
    }
    MemoryManager.instance.incrementDiskBuffer(numBytes)
    if (userCongestionControlContext != null)
      userCongestionControlContext.updateProduceBytes(numBytes)
  }

  override def evict(file: TierWriterBase): Unit = ???

  override def finalFlush(): Unit = {
    flushLock.synchronized {
      if (flushBuffer != null && flushBuffer.readableBytes() > 0) {
        flush(true)
      }
    }
  }

  override def closeStreams(): Unit = {
    channel.close()
  }

  override def notifyFileCommitted(): Unit =
    storageManager.notifyFileInfoCommitted(shuffleKey, filename, diskFileInfo)

  override def closeResource(): Unit = {
    try if (channel != null) channel.close()
    catch {
      case e: IOException =>
        logWarning(
          s"Close channel failed for file ${diskFileInfo.getFilePath} caused by ${e.getMessage}.")
    }
  }

  override def cleanLocalOrDfsFiles(): Unit = {
    diskFileInfo.deleteAllFiles(null)
    partitionDataWriterContext.getDeviceMonitor.unregisterFileWriter(
      partitionDataWriterContext.getPartitionDataWriter)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    flusher.takeBuffer()
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer, true)
      flushBuffer = null
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    if (!flusher.addTask(task, writerCloseTimeoutMs, flushWorkerIndex)) {
      val e = new IOException("Add flush task timeout.")
      notifier.setException(e)
      throw e
    }
  }

  def getFlusher(): Flusher = {
    flusher
  }

  override def handleException(): Unit = {}
}

class DfsTierWriter(
    conf: CelebornConf,
    metaHandler: PartitionMetaHandler,
    numPendingWrites: AtomicInteger,
    notifier: FlushNotifier,
    flusher: Flusher,
    source: AbstractSource,
    hdfsFileInfo: DiskFileInfo,
    storageType: StorageInfo.Type,
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager)
  extends TierWriterBase(
    conf,
    metaHandler,
    numPendingWrites,
    notifier,
    hdfsFileInfo,
    source,
    storageType,
    partitionDataWriterContext.getPartitionLocation.getFileName,
    partitionDataWriterContext.getShuffleKey,
    storageManager) {
  flusherBufferSize = conf.workerHdfsFlusherBufferSize
  private val flushWorkerIndex: Int = flusher.getWorkerIndex
  val hadoopFs: FileSystem = StorageManager.hadoopFs.get(storageType)
  var deleted = false
  private var s3MultipartUploadHandler: MultipartUploadHandler = _
  private var ossMultipartUploadHandler: MultipartUploadHandler = _
  var partNumber: Int = 1

  this.flusherBufferSize =
    if (hdfsFileInfo.isS3()) {
      conf.workerS3FlusherBufferSize
    } else if (hdfsFileInfo.isOSS()) {
      conf.workerOssFlusherBufferSize
    } else {
      conf.workerHdfsFlusherBufferSize
    }

  try {
    hadoopFs.create(hdfsFileInfo.getDfsPath, true).close()
    if (hdfsFileInfo.isS3) {
      val uri = hadoopFs.getUri
      val bucketName = uri.getHost
      val index = hdfsFileInfo.getFilePath.indexOf(bucketName)
      val key = hdfsFileInfo.getFilePath.substring(index + bucketName.length + 1)

      this.s3MultipartUploadHandler = TierWriterHelper.getS3MultipartUploadHandler(
        hadoopFs,
        bucketName,
        key,
        conf.s3MultiplePartUploadMaxRetries)
      s3MultipartUploadHandler.startUpload()
    } else if (hdfsFileInfo.isOSS) {
      val configuration = hadoopFs.getConf
      val ossEndpoint = configuration.get("fs.oss.endpoint")
      val ossAccessKey = configuration.get("fs.oss.accessKeyId")
      val ossSecretKey = configuration.get("fs.oss.accessKeySecret")

      val uri = hadoopFs.getUri
      val bucketName = uri.getHost
      val index = hdfsFileInfo.getFilePath.indexOf(bucketName)
      val key = hdfsFileInfo.getFilePath.substring(index + bucketName.length + 1)

      this.ossMultipartUploadHandler = TierWriterHelper.getOssMultipartUploadHandler(
        ossEndpoint,
        bucketName,
        ossAccessKey,
        ossSecretKey,
        key)
      ossMultipartUploadHandler.startUpload()
    }
  } catch {
    case _: IOException =>
      try
      // If create file failed, wait 10 ms and retry
      Thread.sleep(10)
      catch {
        case ex: InterruptedException =>
          throw new RuntimeException(ex)
      }
      hadoopFs.create(hdfsFileInfo.getDfsPath, true).close()
  }

  storageManager.registerDiskFilePartitionWriter(
    partitionDataWriterContext.getPartitionDataWriter,
    partitionDataWriterContext.getWorkingDir,
    fileInfo.asInstanceOf[DiskFileInfo])

  override def needEvict(): Boolean = {
    false
  }

  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = {
    notifier.numPendingFlushes.incrementAndGet()
    if (hdfsFileInfo.isHdfs) {
      new HdfsFlushTask(flushBuffer, hdfsFileInfo.getDfsPath(), notifier, true)
    } else if (hdfsFileInfo.isOSS) {
      val flushTask = new OssFlushTask(
        flushBuffer,
        notifier,
        true,
        ossMultipartUploadHandler,
        partNumber,
        finalFlush)
      partNumber = partNumber + 1
      flushTask
    } else {
      val flushTask = new S3FlushTask(
        flushBuffer,
        notifier,
        true,
        s3MultipartUploadHandler,
        partNumber,
        finalFlush)
      partNumber = partNumber + 1
      flushTask
    }
  }

  override def writeInternal(buf: ByteBuf): Unit = {
    val numBytes = buf.readableBytes()
    val flushBufferReadableBytes = flushBuffer.readableBytes
    if (flushBufferReadableBytes != 0 && flushBufferReadableBytes + numBytes >= flusherBufferSize) {
      flush(false)
    }
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
    } catch {
      case oom: OutOfMemoryError =>
        MemoryManager.instance.incrementDiskBuffer(numBytes)
        throw oom
    }
    MemoryManager.instance.incrementDiskBuffer(numBytes)
  }

  override def evict(file: TierWriterBase): Unit = ???

  override def finalFlush(): Unit = {
    flushLock.synchronized {
      if (flushBuffer != null && flushBuffer.readableBytes() > 0) {
        flush(true)
      }
    }
  }

  override def closeStreams(): Unit = {
    if (hadoopFs.exists(hdfsFileInfo.getDfsPeerWriterSuccessPath)) {
      hadoopFs.delete(hdfsFileInfo.getDfsPath, false)
      deleted = true
    } else {
      hadoopFs.create(hdfsFileInfo.getDfsWriterSuccessPath).close()
      val indexOutputStream = hadoopFs.create(hdfsFileInfo.getDfsIndexPath)
      indexOutputStream.writeInt(hdfsFileInfo.getReduceFileMeta.getChunkOffsets.size)
      for (offset <- hdfsFileInfo.getReduceFileMeta.getChunkOffsets.asScala) {
        indexOutputStream.writeLong(offset)
      }
      indexOutputStream.close()
    }
    if (s3MultipartUploadHandler != null) {
      s3MultipartUploadHandler.complete()
      s3MultipartUploadHandler.close()
    }
    if (ossMultipartUploadHandler != null) {
      ossMultipartUploadHandler.complete()
      ossMultipartUploadHandler.close()
    }
  }

  override def notifyFileCommitted(): Unit =
    storageManager.notifyFileInfoCommitted(shuffleKey, filename, hdfsFileInfo)

  override def closeResource(): Unit = {}

  override def cleanLocalOrDfsFiles(): Unit = {
    hdfsFileInfo.deleteAllFiles(hadoopFs)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    flusher.takeBuffer()
  }

  override def returnBufferInternal(destroy: Boolean): Unit = {
    if (flushBuffer != null) {
      flusher.returnBuffer(flushBuffer, true)
      flushBuffer = null
    }
  }

  override def addFlushTask(task: FlushTask): Unit = {
    if (!flusher.addTask(task, writerCloseTimeoutMs, flushWorkerIndex)) {
      val e = new IOException("Add flush task timeout.")
      notifier.setException(e)
      throw e
    }
  }

  def getFlusher(): Flusher = {
    flusher
  }

  override def handleException(): Unit = {
    if (s3MultipartUploadHandler != null) {
      logWarning(s"Abort s3 multipart upload for ${fileInfo.getFilePath}")
      s3MultipartUploadHandler.complete()
      s3MultipartUploadHandler.close()
    }
    if (ossMultipartUploadHandler != null) {
      logWarning(s"Abort Oss multipart upload for ${fileInfo.getFilePath}")
      ossMultipartUploadHandler.complete()
      ossMultipartUploadHandler.close()
    }
  }
}
