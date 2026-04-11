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

package org.apache.celeborn.service.deploy.worker

import java.util

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{DiskFileInfo, DiskInfo, MemoryFileInfo, WorkerInfo}
import org.apache.celeborn.common.meta.DiskStatus
import org.apache.celeborn.common.protocol.PartitionSplitMode
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.worker.storage.{LocalFlusher, PartitionDataWriter}

/**
 * Unit tests for [[PushDataHandler.checkDiskFullAndSplit]].
 *
 * The method under test has the following decision tree:
 *   1. If needHardSplitForMemoryShuffleStorage() → HARD_SPLIT
 *   2. If workerPartitionSplitEnabled is false or diskFileInfo is null → NO_SPLIT
 *   3. If disk is full and fileLength > partitionSplitMinimumSize → HARD_SPLIT
 *   4. If isPrimary and fileLength > splitThreshold:
 *      a. SOFT mode and fileLength < partitionSplitMaximumSize → SOFT_SPLIT
 *      b. Otherwise (HARD mode, or fileLength >= partitionSplitMaximumSize) → HARD_SPLIT
 *   5. Otherwise → NO_SPLIT
 */
class PushDataHandlerSuite extends AnyFunSuite {

  // ── helpers ──────────────────────────────────────────────────────────────

  private val splitMinimumSize: Long = 1024L * 1024L // 1 MB
  private val splitMaximumSize: Long = 1024L * 1024L * 10L // 10 MB
  private val splitThreshold: Long = 1024L * 1024L * 2L // 2 MB
  private val mountPoint = "/mnt/disk0"

  /**
   * Build a [[PushDataHandler]] with the given configuration flags, wired up
   * with a minimal [[WorkerInfo]] that contains one disk entry for [[mountPoint]].
   */
  private def buildHandler(
      partitionSplitEnabled: Boolean,
      diskStatus: DiskStatus = DiskStatus.HEALTHY,
      actualUsableSpace: Long = Long.MaxValue): PushDataHandler = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PARTITION_SPLIT_ENABLED.key, partitionSplitEnabled.toString)
    conf.set(CelebornConf.WORKER_PARTITION_SPLIT_MIN_SIZE.key, splitMinimumSize.toString)
    conf.set(CelebornConf.WORKER_PARTITION_SPLIT_MAX_SIZE.key, splitMaximumSize.toString)

    val diskInfo = new DiskInfo(mountPoint, actualUsableSpace, 0L, 0L, 0L)
    diskInfo.setStatus(diskStatus)

    val diskInfoMap = new util.HashMap[String, DiskInfo]()
    diskInfoMap.put(mountPoint, diskInfo)

    val workerInfo = new WorkerInfo("localhost", 9095, 9096, 9097, 9098, -1, diskInfoMap, null)

    val workerSource = mock[WorkerSource]

    val handler = new PushDataHandler(workerSource)
    // Inject fields that checkDiskFullAndSplit depends on via reflection helpers
    // exposed through the init path. We use a minimal mock Worker instead.
    val mockWorker = mock[Worker]
    when(mockWorker.conf).thenReturn(conf)
    when(mockWorker.workerInfo).thenReturn(workerInfo)
    // Provide no-op stubs for all other Worker fields accessed during init
    when(mockWorker.partitionLocationInfo).thenReturn(mock[
      org.apache.celeborn.common.meta.WorkerPartitionLocationInfo])
    when(mockWorker.shufflePartitionType).thenReturn(
      new java.util.concurrent.ConcurrentHashMap[
        String,
        org.apache.celeborn.common.protocol.PartitionType]())
    when(mockWorker.shufflePushDataTimeout).thenReturn(
      new java.util.concurrent.ConcurrentHashMap[String, Long]())
    when(mockWorker.shuffleMapperAttempts).thenReturn(
      new java.util.concurrent.ConcurrentHashMap[
        String,
        java.util.concurrent.atomic.AtomicIntegerArray]())
    when(mockWorker.replicateThreadPool).thenReturn(mock[java.util.concurrent.ThreadPoolExecutor])
    when(mockWorker.unavailablePeers).thenReturn(
      new java.util.concurrent.ConcurrentHashMap[WorkerInfo, Long]())
    when(mockWorker.replicateClientFactory).thenReturn(
      mock[org.apache.celeborn.common.network.client.TransportClientFactory])
    when(mockWorker.storageManager).thenReturn(mock[
      org.apache.celeborn.service.deploy.worker.storage.StorageManager])
    when(mockWorker.shutdown).thenReturn(new java.util.concurrent.atomic.AtomicBoolean(false))
    when(mockWorker.registered).thenReturn(new java.util.concurrent.atomic.AtomicBoolean(true))

    handler.init(mockWorker)
    handler
  }

  /**
   * Build a mock [[PartitionDataWriter]] backed by a [[LocalFlusher]] pointing
   * at [[mountPoint]], with the given file length, split threshold, and split mode.
   */
  private def buildFileWriter(
      fileLength: Long,
      splitMode: PartitionSplitMode = PartitionSplitMode.SOFT,
      needMemoryHardSplit: Boolean = false,
      diskFileInfoNull: Boolean = false): PartitionDataWriter = {
    val fileWriter = mock[PartitionDataWriter]

    when(fileWriter.needHardSplitForMemoryShuffleStorage()).thenReturn(needMemoryHardSplit)

    if (needMemoryHardSplit) {
      val memoryFileInfo = mock[MemoryFileInfo]
      when(memoryFileInfo.getFileLength).thenReturn(fileLength)
      when(fileWriter.getMemoryFileInfo).thenReturn(memoryFileInfo)
    }

    if (diskFileInfoNull) {
      when(fileWriter.getDiskFileInfo).thenReturn(null)
    } else {
      val diskFileInfo = mock[DiskFileInfo]
      when(diskFileInfo.getFileLength).thenReturn(fileLength)
      when(diskFileInfo.getFilePath).thenReturn(s"/data/shuffle-$fileLength.index")
      when(fileWriter.getDiskFileInfo).thenReturn(diskFileInfo)
    }

    val localFlusher = mock[LocalFlusher]
    when(localFlusher.mountPoint).thenReturn(mountPoint)
    when(fileWriter.getFlusher).thenReturn(localFlusher)
    when(fileWriter.getSplitThreshold).thenReturn(splitThreshold)
    when(fileWriter.getSplitMode).thenReturn(splitMode)

    fileWriter
  }

  // ── test cases ────────────────────────────────────────────────────────────

  test("returns HARD_SPLIT when memory shuffle storage needs hard split") {
    val handler = buildHandler(partitionSplitEnabled = true)
    val fileWriter = buildFileWriter(fileLength = 0L, needMemoryHardSplit = true)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.HARD_SPLIT)
  }

  test("returns NO_SPLIT when workerPartitionSplitEnabled is false") {
    val handler = buildHandler(partitionSplitEnabled = false)
    // Even if file is huge, split is disabled globally
    val fileWriter = buildFileWriter(fileLength = splitThreshold * 10)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns NO_SPLIT when diskFileInfo is null (e.g. memory-only writer)") {
    val handler = buildHandler(partitionSplitEnabled = true)
    val fileWriter = buildFileWriter(fileLength = splitThreshold * 10, diskFileInfoNull = true)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns HARD_SPLIT when disk is full and file exceeds partitionSplitMinimumSize") {
    val handler = buildHandler(
      partitionSplitEnabled = true,
      diskStatus = DiskStatus.HIGH_DISK_USAGE)
    // File is larger than the minimum split size
    val fileWriter = buildFileWriter(fileLength = splitMinimumSize + 1)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.HARD_SPLIT)
  }

  test("returns NO_SPLIT when disk is full but file is smaller than partitionSplitMinimumSize") {
    val handler = buildHandler(
      partitionSplitEnabled = true,
      diskStatus = DiskStatus.HIGH_DISK_USAGE)
    // File is smaller than the minimum split size → no split triggered
    val fileWriter = buildFileWriter(fileLength = splitMinimumSize - 1)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns NO_SPLIT when disk usable space is zero and file is below minimum split size") {
    val handler = buildHandler(
      partitionSplitEnabled = true,
      actualUsableSpace = 0L)
    val fileWriter = buildFileWriter(fileLength = splitMinimumSize - 1)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns HARD_SPLIT when disk usable space is zero and file exceeds minimum split size") {
    val handler = buildHandler(
      partitionSplitEnabled = true,
      actualUsableSpace = 0L)
    val fileWriter = buildFileWriter(fileLength = splitMinimumSize + 1)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.HARD_SPLIT)
  }

  test("returns SOFT_SPLIT for primary when file exceeds splitThreshold in SOFT mode and is below maximum size") {
    val handler = buildHandler(partitionSplitEnabled = true)
    // File is between splitThreshold and splitMaximumSize
    val fileLength = splitThreshold + 1
    val fileWriter = buildFileWriter(fileLength = fileLength, splitMode = PartitionSplitMode.SOFT)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.SOFT_SPLIT)
  }

  test("returns HARD_SPLIT for primary when file exceeds splitThreshold in SOFT mode but reaches maximum size") {
    val handler = buildHandler(partitionSplitEnabled = true)
    // File has grown beyond the maximum allowed size → force HARD_SPLIT even in SOFT mode
    val fileLength = splitMaximumSize + 1
    val fileWriter = buildFileWriter(fileLength = fileLength, splitMode = PartitionSplitMode.SOFT)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.HARD_SPLIT)
  }

  test("returns HARD_SPLIT for primary when file exceeds splitThreshold in HARD mode") {
    val handler = buildHandler(partitionSplitEnabled = true)
    val fileLength = splitThreshold + 1
    val fileWriter = buildFileWriter(fileLength = fileLength, splitMode = PartitionSplitMode.HARD)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.HARD_SPLIT)
  }

  test("returns NO_SPLIT for replica even when file exceeds splitThreshold") {
    val handler = buildHandler(partitionSplitEnabled = true)
    // Replica workers never trigger a split — only primary does
    val fileLength = splitThreshold * 5
    val fileWriter = buildFileWriter(fileLength = fileLength, splitMode = PartitionSplitMode.SOFT)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = false)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns NO_SPLIT when file length is exactly at splitThreshold (not exceeded)") {
    val handler = buildHandler(partitionSplitEnabled = true)
    val fileWriter =
      buildFileWriter(fileLength = splitThreshold, splitMode = PartitionSplitMode.SOFT)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }

  test("returns NO_SPLIT when file length is below splitThreshold") {
    val handler = buildHandler(partitionSplitEnabled = true)
    val fileWriter =
      buildFileWriter(fileLength = splitThreshold - 1, splitMode = PartitionSplitMode.SOFT)

    val result = handler.checkDiskFullAndSplit(fileWriter, isPrimary = true)

    assert(result === StatusCode.NO_SPLIT)
  }
}
