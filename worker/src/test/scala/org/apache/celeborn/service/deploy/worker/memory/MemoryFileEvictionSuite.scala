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

package org.apache.celeborn.service.deploy.worker.memory

import org.mockito.{Mockito, MockitoSugar}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.MemoryFileInfo
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager.ServingState
import org.apache.celeborn.service.deploy.worker.storage.{PartitionDataWriter, StorageManager}

class MemoryFileEvictionSuite extends CelebornFunSuite {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    MemoryManager.reset()
  }

  override protected def afterEach(): Unit = {
    MemoryManager.reset()
    super.afterEach()
  }

  test("Keep evicting memory files while paused below the evict target") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE.key, "0.3")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "0")
    val evictRatio = conf.workerMemoryFileStorageEvictRatio

    val memoryWriters = JavaUtils.newConcurrentHashMap[MemoryFileInfo, PartitionDataWriter]()
    def addWriter(length: Long, closed: Boolean): PartitionDataWriter = {
      val fileInfo = Mockito.mock(classOf[MemoryFileInfo])
      Mockito.when(fileInfo.getFileLength).thenReturn(length)
      val writer = Mockito.mock(classOf[PartitionDataWriter])
      Mockito.when(writer.getMemoryFileInfo).thenReturn(fileInfo)
      Mockito.when(writer.isClosed).thenReturn(closed)
      memoryWriters.put(fileInfo, writer)
      writer
    }
    val committed = addWriter(300, closed = true)
    val large = addWriter(200, closed = false)
    val small = addWriter(100, closed = false)
    val storageManager = Mockito.mock(classOf[StorageManager])
    Mockito.when(storageManager.memoryWriters).thenReturn(memoryWriters)

    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf, storageManager, null))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * memoryManager.maxDirectMemory).longValue()
    // memory file storage is below the evict target, so ratio-based eviction has nothing to do
    assert(memoryManager.getMemoryFileStorageCounter == 0)

    // not paused: nothing to evict
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(pushThreshold + 1)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)
    memoryManager.evictMemoryFiles(false, evictRatio)
    Mockito.verify(large, Mockito.never()).evict(true)

    // paused, but evicted data is still being flushed: wait for that to free memory first
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    memoryManager.incrementDiskBuffer(1)
    memoryManager.evictMemoryFiles(false, evictRatio)
    Mockito.verify(large, Mockito.never()).evict(true)

    // still paused with nothing in flight: evict the largest file that is not committed
    memoryManager.releaseDiskBuffer(1)
    memoryManager.evictMemoryFiles(false, evictRatio)
    Mockito.verify(committed, Mockito.never()).evict(true)
    Mockito.verify(large, Mockito.times(1)).evict(true)
    Mockito.verify(small, Mockito.never()).evict(true)
    memoryWriters.remove(large.getMemoryFileInfo)

    // push resumed by pinned memory while servingState stays paused: stop evicting
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    memoryManager.evictMemoryFiles(false, evictRatio)
    Mockito.verify(small, Mockito.never()).evict(true)

    // paused again: keep going, one file per check, until the worker resumes
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(replicateThreshold + 1)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)
    memoryManager.evictMemoryFiles(false, evictRatio)
    Mockito.verify(small, Mockito.times(1)).evict(true)
  }
}
