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
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MemoryFileInfo}
import org.apache.celeborn.common.protocol.{PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterProxy(
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager,
    conf: CelebornConf,
    partitionType: PartitionType) {
  val memoryFileStorageMaxFileSize = conf.workerMemoryFileStorageMaxFileSize
  val flushLock = new AnyRef
  val notifier = new FlushNotifier
  val numPendingWrites = new AtomicInteger
  @volatile var currentTierWriter: TierWriterBase = _
  @volatile var fileClosed = false
  var flusher: Flusher = _
  var flushWorkerIndex = 0

  currentTierWriter =
    storageManager.storagePolicy.createFileWriter(
      partitionDataWriterContext,
      partitionType,
      numPendingWrites,
      notifier)

  def write(buf: ByteBuf): Unit = this.synchronized {
    if (currentTierWriter.needEvict()) {
      evict(false)
    }
    currentTierWriter.write(buf)
  }

  def evict(checkClose: Boolean): Unit = this.synchronized {
    if (checkClose) {
      if (fileClosed) {
        return
      }
    }
    val nFile =
      storageManager.storagePolicy.getEvictedFileWriter(
        currentTierWriter,
        partitionDataWriterContext,
        partitionType,
        numPendingWrites,
        notifier)
    currentTierWriter.evict(nFile)
    currentTierWriter = nFile
  }

  def flush(finalFlush: Boolean = false): Unit = this.synchronized {
    currentTierWriter.flush(finalFlush)
  }

  def getCurrentFileInfo(): FileInfo = {
    currentTierWriter.fileInfo
  }

  def needHardSplitForMemoryFile(): Boolean = {
    if (!currentTierWriter.isInstanceOf[MemoryTierWriter]) {
      return false;
    }
    !storageManager.localOrDfsStorageAvailable &&
    (currentTierWriter.fileInfo.getFileLength > memoryFileStorageMaxFileSize ||
      !MemoryManager.instance.memoryFileStorageAvailable())
  }

  def getCurrentStorageInfo(): StorageInfo = {
    if (currentTierWriter.fileInfo.isInstanceOf[DiskFileInfo]) {
      val diskFileInfo = currentTierWriter.fileInfo.asInstanceOf[DiskFileInfo]
      if (diskFileInfo.isDFS) {
        if (currentTierWriter.asInstanceOf[DfsTierWriter].deleted) {
          return null
        } else if (diskFileInfo.isS3) {
          return new StorageInfo(StorageInfo.Type.S3, true, diskFileInfo.getFilePath)
        } else if (diskFileInfo.isHdfs) {
          return new StorageInfo(StorageInfo.Type.HDFS, true, diskFileInfo.getFilePath)
        }
      }
      new StorageInfo(flusher.asInstanceOf[LocalFlusher].diskType, true, "")
    } else if (currentTierWriter.fileInfo.isInstanceOf[MemoryFileInfo]) {
      new StorageInfo(StorageInfo.Type.MEMORY, true, "")
    } else {
      // this should not happen
      null
    }
  }

  def destroy(ioException: IOException): Unit = {
    currentTierWriter.destroy(ioException)
  }

  def isLocalFile(): Boolean = {
    currentTierWriter.isInstanceOf[LocalTierWriter]
  }

  def flushOnMemoryPressure(): Unit = {
    currentTierWriter.flush(false)
  }

  def close(): Long = {
    val len = currentTierWriter.close()
    fileClosed = true
    len
  }

  def isClosed: Boolean = {
    fileClosed
  }

  def handleEvents(msg: GeneratedMessageV3): Unit = {
    currentTierWriter.metaHandler.handleEvent(msg).applyOrElse[Any, Unit](
      msg,
      {
        case _ => throw new IllegalArgumentException(s"Unexpected message type: $msg")
      })
  }

  def incrementPendingWriters(): Unit = {
    numPendingWrites.incrementAndGet()
  }

  def descrmentPendingWriters(): Unit = {
    numPendingWrites.decrementAndGet()
  }

  def getMetaHandler(): PartitionMetaHandler = {
    currentTierWriter.metaHandler
  }
}
