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

class StoragePolicyReplicaSuite extends CelebornFunSuite {
  val mockedStorageManager: StorageManager = mock[StorageManager]
  val mockedSource: AbstractSource = mock[AbstractSource]

  val memoryConf = new CelebornConf
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE.key, "0.8")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE.key, "0.9")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_RESUME.key, "0.5")
  memoryConf.set(CelebornConf.WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD.key, "0.6")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_READ_BUFFER.key, "0.1")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_RATIO_FOR_MEMORY_FILE_STORAGE.key, "0.1")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "10")
  memoryConf.set(CelebornConf.WORKER_DIRECT_MEMORY_REPORT_INTERVAL.key, "10")
  memoryConf.set(CelebornConf.WORKER_READBUFFER_ALLOCATIONWAIT.key, "10ms")
  MemoryManager.initialize(memoryConf)

  when(
    mockedStorageManager.createMemoryFileInfo(any(), any(), any(), any(), any(), any())).thenAnswer(
    mock[MemoryFileInfo])
  when(mockedStorageManager.storageBufferAllocator).thenAnswer(UnpooledByteBufAllocator.DEFAULT)
  when(mockedStorageManager.localOrDfsStorageAvailable).thenAnswer(true)

  val mockedDiskFile: DiskFileInfo = mock[DiskFileInfo]
  when(mockedDiskFile.getStorageType).thenAnswer(StorageInfo.Type.SSD)
  when(
    mockedStorageManager.createDiskFile(
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any(),
      any())).thenAnswer((mock[LocalFlusher], mockedDiskFile, mock[File]))

  private def createWriter(
      mode: PartitionLocation.Mode,
      replicaEnabled: Option[Boolean]): TierWriterBase = {
    val context = mock[PartitionDataWriterContext]
    when(context.getPartitionLocation).thenAnswer(
      new PartitionLocation(1, 1, "h1", 1, 2, 3, 4, mode))
    when(context.getPartitionType).thenAnswer(PartitionType.REDUCE)
    val conf = new CelebornConf()
    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "MEMORY,SSD,HDD,HDFS,OSS,S3")
    replicaEnabled.foreach(v =>
      conf.set(CelebornConf.WORKER_MEMORY_FILE_STORAGE_REPLICA_ENABLED.key, v.toString))
    new StoragePolicy(conf, mockedStorageManager, mockedSource)
      .createFileWriter(context, new AtomicInteger(), new FlushNotifier)
  }

  test("primary partition uses memory file storage") {
    assert(createWriter(PartitionLocation.Mode.PRIMARY, None).isInstanceOf[MemoryTierWriter])
  }

  test("replica partition skips memory file storage by default") {
    assert(createWriter(PartitionLocation.Mode.REPLICA, None).isInstanceOf[LocalTierWriter])
  }

  test("replica partition uses memory file storage when enabled") {
    assert(
      createWriter(PartitionLocation.Mode.REPLICA, Some(true)).isInstanceOf[MemoryTierWriter])
  }
}
