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

package org.apache.celeborn.service.deploy.worker.storage.storagePolicy

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.UnpooledByteBufAllocator
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, when}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskFileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.storage._

class StoragePolicyCase2 extends CelebornFunSuite {
  val mockedStorageManager: StorageManager = mock[StorageManager]
  val mockedSource: AbstractSource = mock[AbstractSource]
  val mockedPartitionWriterContext: PartitionDataWriterContext = mock[PartitionDataWriterContext]

  val conf = new CelebornConf
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE.key, "0.8")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE.key, "0.9")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_RESUME.key, "0.5")
  conf.set(CelebornConf.WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD.key, "0.6")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER.key, "0.1")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE.key, "0.1")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "10")
  conf.set(CelebornConf.WORKER_DIRECT_MEMORY_REPORT_INTERVAL.key, "10")
  conf.set(CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT.key, "10ms")
  MemoryManager.initialize(conf)

  val mockedCelebornMemoryFile = mock[MemoryFileInfo]
  when(
    mockedStorageManager.createMemoryFileInfo(any(), any(), any(), any(), any(), any())).thenAnswer(
    mockedCelebornMemoryFile)
  when(mockedStorageManager.storageBufferAllocator).thenAnswer(UnpooledByteBufAllocator.DEFAULT)

  val mockedDiskFile = mock[DiskFileInfo]
  val mockedFlusher = mock[LocalFlusher]
  val mockedFile = mock[File]
  when(
    mockedStorageManager.createDiskFile(
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any())).thenAnswer((mockedFlusher, mockedDiskFile, mockedFile))

  val memoryHintPartitionLocation =
    new PartitionLocation(1, 1, "h1", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY)
  val memoryDisabledHintPartitionLocation =
    new PartitionLocation(
      1,
      1,
      "h1",
      1,
      2,
      3,
      4,
      PartitionLocation.Mode.PRIMARY,
      null,
      new StorageInfo("mountpoint", StorageInfo.Type.MEMORY, StorageInfo.LOCAL_DISK_MASK),
      null)
  val localHintPartitionLocation =
    new PartitionLocation(
      1,
      1,
      "h1",
      1,
      2,
      3,
      4,
      PartitionLocation.Mode.PRIMARY,
      null,
      new StorageInfo("mountpoint", StorageInfo.Type.SSD, 0),
      null)

  test("test create file order case2") {
    when(mockedPartitionWriterContext.getPartitionLocation).thenAnswer(localHintPartitionLocation)
    when(mockedPartitionWriterContext.getPartitionType).thenAnswer(PartitionType.REDUCE)
    when(mockedStorageManager.localOrDfsStorageAvailable).thenAnswer(true)
    when(mockedDiskFile.getStorageType).thenAnswer(StorageInfo.Type.HDD)
    val conf = new CelebornConf()
    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "SSD,HDD,HDFS,OSS,S3")
    val storagePolicy = new StoragePolicy(conf, mockedStorageManager, mockedSource)
    val pendingWriters = new AtomicInteger()
    val notifier = new FlushNotifier
    val file = storagePolicy.createFileWriter(
      mockedPartitionWriterContext,
      pendingWriters,
      notifier)
    assert(file.isInstanceOf[LocalTierWriter])
  }

}
