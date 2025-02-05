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
import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.GeneratedMessageV3
import io.netty.buffer.ByteBuf

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.protocol.{PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterProxy(
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager,
    conf: CelebornConf,
    partitionType: PartitionType) {
  val memoryFileStorageMaxFileSize: Long = conf.workerMemoryFileStorageMaxFileSize
  val notifier = new FlushNotifier
  val numPendingWrites = new AtomicInteger
  @volatile var currentTierWriter: TierWriterBase = _
  val flushLock = new AnyRef

  currentTierWriter =
    storageManager.storagePolicy.createFileWriter(
      partitionDataWriterContext,
      partitionType,
      numPendingWrites,
      notifier,
      flushLock)

  def write(buf: ByteBuf): Unit = this.flushLock.synchronized {
    if (currentTierWriter.needEvict()) {
      evict(false)
    }
    currentTierWriter.write(buf)
  }

  // evict and flush method need to be in a same synchronized block
  // because memory manager may want to evict a file under memory pressure
  def evict(checkClose: Boolean): Unit = this.flushLock.synchronized {
    // close and evict might be invoked concurrently
    // do not evict committed files from memory manager
    // evict memory file info if worker is shutdown gracefully
    if (checkClose) {
      if (currentTierWriter.closed) {
        return
      }
    }
    val nFile =
      storageManager.storagePolicy.getEvictedFileWriter(
        currentTierWriter,
        partitionDataWriterContext,
        partitionType,
        numPendingWrites,
        notifier,
        flushLock)
    currentTierWriter.evict(nFile)
    currentTierWriter = nFile
  }

  def flush(finalFlush: Boolean): Unit = this.flushLock.synchronized {
    currentTierWriter.flush(finalFlush)
  }

  def getCurrentFileInfo(): FileInfo = {
    currentTierWriter.fileInfo
  }

  def needHardSplitForMemoryFile(): Boolean = {
    if (!currentTierWriter.isInstanceOf[MemoryTierWriter]) {
      return false
    }
    !storageManager.localOrDfsStorageAvailable &&
    (currentTierWriter.fileInfo.getFileLength > memoryFileStorageMaxFileSize ||
      !MemoryManager.instance.memoryFileStorageAvailable())
  }

  def getCurrentStorageInfo(): StorageInfo = {
    val storageInfo = {
      currentTierWriter.fileInfo match {
        case diskFileInfo: DiskFileInfo =>
          if (diskFileInfo.isDFS) {
            if (currentTierWriter.asInstanceOf[DfsTierWriter].deleted) {
              return null
            } else if (diskFileInfo.isS3) {
              return new StorageInfo(StorageInfo.Type.S3, true, diskFileInfo.getFilePath)
            } else if (diskFileInfo.isHdfs) {
              return new StorageInfo(StorageInfo.Type.HDFS, true, diskFileInfo.getFilePath)
            }
          }
          val flusher = currentTierWriter.asInstanceOf[LocalTierWriter].getFlusher()
          new StorageInfo(flusher.asInstanceOf[LocalFlusher].diskType, true, "")
        case _: MemoryFileInfo =>
          new StorageInfo(StorageInfo.Type.MEMORY, true, "")
        case _ =>
          // this should not happen
          null
      }
    }

    // this is for the optimize of sort elimination
    if (storageInfo != null && currentTierWriter.fileInfo.getFileMeta.isInstanceOf[ReduceFileMeta]) {
      storageInfo.setFileSize(currentTierWriter.fileInfo.getFileLength)
      storageInfo.setChunkOffsets(
        currentTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets)
    }
    storageInfo
  }

  def destroy(ioException: IOException): Unit = {
    currentTierWriter.destroy(ioException)
  }

  def close(): Long = {
    currentTierWriter.close()
  }

  def isClosed: Boolean = {
    currentTierWriter.closed
  }

  def handleEvents(msg: GeneratedMessageV3): Unit = {
    currentTierWriter.metaHandler.handleEvent(msg)
  }

  def incrementPendingWriters(): Unit = {
    numPendingWrites.incrementAndGet()
  }

  def decrementPendingWriters(): Unit = {
    numPendingWrites.decrementAndGet()
  }

  def getMetaHandler(): PartitionMetaHandler = {
    currentTierWriter.metaHandler
  }

  def getFlusher(): Flusher = {
    currentTierWriter.getFlusher()
  }

  def registerToDeviceMonitor(): Unit = {
    currentTierWriter.registerToDeviceMonitor()
  }
}
