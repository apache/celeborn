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

import io.netty.buffer.ByteBuf

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.StorageInfo

class CelebornFileProxy(
    partitionDataWriterContext: PartitionDataWriterContext,
    storageManager: StorageManager,
    conf: CelebornConf,
    source: AbstractSource) {
  var currentFile: CelebornFile = _
  var flusher: Flusher = null
  var flushWorkerIndex = 0

  currentFile = storageManager.storagePolicy.createFile(partitionDataWriterContext)

  def write(buf: ByteBuf) = {
    this.synchronized {
      currentFile.write(buf)
    }
  }

  def evict(force: Boolean) = {
    if (currentFile.needEvict || force) {
      this.synchronized {
        val nFile =
          storageManager.storagePolicy.getEvictedFile(currentFile, partitionDataWriterContext)
        currentFile.evict(nFile)
        currentFile = nFile
      }
    }
  }

  def close(): Unit = {}

  def isMemoryShuffleFile: Boolean = {
    currentFile.storageType == StorageInfo.Type.MEMORY
  }
}
