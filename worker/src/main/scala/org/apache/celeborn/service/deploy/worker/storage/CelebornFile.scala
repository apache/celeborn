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

import io.netty.buffer.{ByteBuf, CompositeByteBuf}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo

import java.io.File
import java.nio.channels.FileChannel

abstract class CelebornFile {
  var fileInfo: FileInfo = _
  var flushBuffer: CompositeByteBuf = _
  val flushLock = new AnyRef
  var flusher: Flusher = _
  var flushWorkerIndex: Int = _
  var writerCloseTimeoutMs: Long = _
  var flusherBufferSize = 0L
  var source: AbstractSource = _ // metrics
  var chunkSize: Long = _
  var metricsCollectCriticalEnabled = false
  var storageType: StorageInfo.Type = _

  def write(buf: ByteBuf): Unit

  def needEvict: Boolean

  def evict(file: CelebornFile): Unit

  def close(): Unit
}

class CelebornMemoryFile(
    conf: CelebornConf,
    source: AbstractSource,
    fileInfo: MemoryFileInfo,
    storageType: StorageInfo.Type) extends CelebornFile {

  override def write(buf: ByteBuf): Unit = {}

  override def needEvict: Boolean = ???

  override def evict(file: CelebornFile): Unit = ???

  override def close(): Unit = ???
}

class CelebornDiskFile(
    flusher: Flusher,
    diskFileInfo: DiskFileInfo,
    workingDir: File,
    storageType: StorageInfo.Type) extends CelebornFile {
  private var channel: FileChannel = null

  override def write(buf: ByteBuf): Unit = ???

  override def needEvict: Boolean = ???

  override def evict(file: CelebornFile): Unit = ???

  override def close(): Unit = ???
}

class CelebornDFSFile(flusher: Flusher, hdfsFileInfo: DiskFileInfo, storageType: StorageInfo.Type)
  extends CelebornFile {

  override def write(buf: ByteBuf): Unit = ???

  override def needEvict: Boolean = ???

  override def evict(file: CelebornFile): Unit = ???

  override def close(): Unit = ???
}
