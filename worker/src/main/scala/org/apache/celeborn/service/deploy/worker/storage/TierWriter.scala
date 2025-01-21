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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.{ByteBuf, ByteBufAllocator, CompositeByteBuf}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{FileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.service.deploy.worker.WorkerSource
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
  val flushLock = new AnyRef
  val WAIT_INTERVAL_MS = 5

  var flushBuffer: CompositeByteBuf = _
  var writerCloseTimeoutMs: Long = conf.workerWriterCloseTimeoutMs
  var flusherBufferSize = 0L
  var chunkSize: Long = conf.shuffleChunkSize

  @volatile var closed = false
  @volatile var destroyed = false

  val memoryFileAllocator: ByteBufAllocator = storageManager.storageBufferAllocator
  var memoryFileStorageMaxFileSize: Long = conf.workerMemoryFileStorageMaxFileSize

  takeBuffer()

  def write(buf: ByteBuf): Unit = {
    ensureNotClosed()
    if (notifier.hasException) return

    flushLock.synchronized {
      metaHandler.beforeWrite(buf)
      ensureNotClosed()
      writerInternal(buf)
    }

    metaHandler.afterWrite(buf.readableBytes())
    numPendingWrites.decrementAndGet()
  }

  protected def writerInternal(buf: ByteBuf): Unit

  def updateMemoryMetric(numBytes: Int): Unit

  def needEvict(): Boolean

  def evict(file: TierWriterBase): Unit

  def swapFlushBuffer(inputBuffer: CompositeByteBuf): Unit = {
    if (flushBuffer != null) {
      returnBuffer(false)
    }
    flushBuffer = inputBuffer
  }

  def close(evict: Boolean = false): Long = {
    try {
      ensureNotClosed()
      waitOnNoPending(numPendingWrites, false)
      closed = true
      finalFlush()

      waitOnNoPending(notifier.numPendingFlushes, true)
      metaHandler.afterClose()
    } finally {
      returnBuffer(false)
      try {
        closeStreams()
      } catch {
        case e: IOException =>
          logWarning(s"close file writer ${this} failed", e)
      }
    }
    if (!evict) {
      notifyFileCommitted()
    }

    fileInfo.getFileLength
  }

  def ensureNotClosed(): Unit = {
    if (closed) {
      val msg = getFileAlreadyClosedMsg
      logWarning(msg)
      throw new AlreadyClosedException(msg)
    }
  }

  def getFileAlreadyClosedMsg: String = {
    s"PartitionDataWriter has already closed! File name:${filename}"
  }

  // this method is not used in memory tier writer
  def notifyFileCommitted(): Unit = {}

  // this method is not used in memory tier writer
  def finalFlush(): Unit = {}

  @throws[IOException]
  protected def waitOnNoPending(counter: AtomicInteger, failWhenTimeout: Boolean): Unit = {
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

  def flush(finalFlush: Boolean, fromEvict: Boolean = false): Unit = {
    if (flushBuffer != null) {
      val numBytes = flushBuffer.readableBytes()
      var flushTask: FlushTask = null
      if (numBytes != 0) {
        if (fromEvict) {
          //          notifier.numPendingFlushes.incrementAndGet()
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
            }
            dupBuf.release
          } else metaHandler.afterFlush(numBytes)
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
        if (!fromEvict) {
          metaHandler.afterFlush(numBytes)
        }
        if (!finalFlush) {
          takeBuffer()
        }
      }
    }

  }

  def takeBuffer() = {
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

  override def needEvict(): Boolean = {
    flushBuffer.readableBytes() > memoryFileStorageMaxFileSize && storageManager.localOrDfsStorageAvailable()
  }

  override def evict(file: TierWriterBase): Unit = {
    flushLock.synchronized {
      // swap tier writer's flush buffer to memory tier writer's
      // and handle its release
      file.swapFlushBuffer(flushBuffer)
      file.flush(false, true)
      val numBytes = flushBuffer.readableBytes()
      MemoryManager.instance.releaseMemoryFileStorage(numBytes)
      MemoryManager.instance.incrementDiskBuffer(numBytes)
      storageManager.unregisterMemoryPartitionWriterAndFileInfo(fileInfo, shuffleKey, filename)
      storageManager.evictedFileCount.incrementAndGet
    }
  }

  // Memory file won't produce flush task
  override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = null

  override def updateMemoryMetric(numBytes: Int): Unit = {
    MemoryManager.instance().incrementMemoryFileStorage(numBytes)
  }

  override def writerInternal(buf: ByteBuf): Unit = {
    buf.retain()
    try {
      flushBuffer.addComponent(true, buf)
    } catch {
      case oom: OutOfMemoryError =>
        MemoryManager.instance.releaseMemoryFileStorage(buf.readableBytes())
        throw oom
    }
    // memory tier writer will not flush
    // add the bytes into flusher buffer is flush completed
    metaHandler.afterFlush(buf.readableBytes())
    updateMemoryMetric(buf.readableBytes)
  }

  override def closeStreams(): Unit = {
    flushBuffer.consolidate()
    fileInfo.setBuffer(flushBuffer)
  }

  override def takeBufferInternal(): CompositeByteBuf = {
    memoryFileAllocator.compositeBuffer(Integer.MAX_VALUE)
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
}
