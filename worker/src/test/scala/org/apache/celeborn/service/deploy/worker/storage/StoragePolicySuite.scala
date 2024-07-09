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

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.when

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskFileInfo, MemoryFileInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType, StorageInfo}

class StoragePolicySuite extends CelebornFunSuite {
  val mockedStorageManager: StorageManager = mock[StorageManager]
  val mockedSource: AbstractSource = mock[AbstractSource]
  val mockedPartitionWriterContext: PartitionDataWriterContext = mock[PartitionDataWriterContext]

  val mockedCelebornMemoryFile = mock[MemoryFileInfo]
  when(
    mockedStorageManager.createMemoryFileInfo(any(), any(), any(), any(), any(), any())).thenAnswer(
    mockedCelebornMemoryFile)

  val mockedDiskFile = mock[DiskFileInfo]
  val mockedFlusher = mock[Flusher]
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

  val mockedPartitionLocation =
    new PartitionLocation(1, 1, "h1", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY)
  when(mockedPartitionWriterContext.getPartitionLocation).thenAnswer(mockedPartitionLocation)

  test("test create file order case1") {
    val conf = new CelebornConf()
    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "MEMORY,SSD,HDD,HDFS,OSS,S3")
    val storagePolicy = new StoragePolicy(conf, mockedStorageManager, mockedSource)
    val pendingWriters = new AtomicInteger()
    val notifier = new FlushNotifier
    val file = storagePolicy.createFileWriter(
      mockedPartitionWriterContext,
      PartitionType.REDUCE,
      pendingWriters,
      notifier)
    assert(file.isInstanceOf[LocalTierWriter])
  }

  test("test create file order case2") {
    val conf = new CelebornConf()
    conf.set("celeborn.worker.storage.storagePolicy.createFilePolicy", "SSD,HDD,HDFS,OSS,S3")
    val storagePolicy = new StoragePolicy(conf, mockedStorageManager, mockedSource)
    val pendingWriters = new AtomicInteger()
    val notifier = new FlushNotifier
    val file = storagePolicy.createFileWriter(
      mockedPartitionWriterContext,
      PartitionType.REDUCE,
      pendingWriters,
      notifier)
    assert(file.isInstanceOf[LocalTierWriter])
  }

  test("test getEvicted file case1") {
    val mockedMemoryFile = mock[LocalTierWriter]
    val conf = new CelebornConf()
    val storagePolicy = new StoragePolicy(conf, mockedStorageManager, mockedSource)
    val pendingWriters = new AtomicInteger()
    val notifier = new FlushNotifier
    when(mockedMemoryFile.storageType).thenAnswer(StorageInfo.Type.MEMORY)
    val nFile = storagePolicy.getEvictedFileWriter(
      mockedMemoryFile,
      mockedPartitionWriterContext,
      PartitionType.REDUCE,
      pendingWriters,
      notifier)
    assert(nFile.isInstanceOf[LocalTierWriter])
  }
}
