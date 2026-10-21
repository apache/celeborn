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

import java.{lang, util}
import java.io.IOException
import java.nio.file.Files
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.mockito.{Mockito, MockitoSugar}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{Answer, Stubber}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_DISK_RESERVE_SIZE, WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_INTERVAL, WORKER_STORAGE_DIRS}
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, DiskInfo, DiskStatus}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, Utils}
import org.apache.celeborn.service.deploy.worker.WorkerSource

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class StorageManagerSuite extends CelebornFunSuite with MockitoHelper {

  test("[CELEBORN-926] saveAllCommittedFileInfosToDB cause IllegalMonitorStateException") {
    val conf = new CelebornConf().set(WORKER_GRACEFUL_SHUTDOWN_ENABLED, true)
      .set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, "/tmp/recover")
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    // should not throw IllegalMonitorStateException exception
    storageManager.saveAllCommittedFileInfosToDB()
  }

  test("[CELEBORN-2495] committedFileInfos should not leak when graceful shutdown is disabled") {
    withDiskFile(gracefulShutdown = false) { (storageManager, fileInfo, _) =>
      storageManager.notifyFileInfoCommitted("app-1-0", "0-0-0", fileInfo)
      assert(storageManager.committedFileInfos.isEmpty)
      storageManager.cleanupExpiredShuffleKey(storageManager.shuffleKeySet())
      assert(storageManager.committedFileInfos.isEmpty)
    }
  }

  test("[CELEBORN-2495] committedFileInfos should be removed after expiry with graceful shutdown enabled") {
    withDiskFile(gracefulShutdown = true) { (storageManager, fileInfo, _) =>
      storageManager.notifyFileInfoCommitted("app-1-0", "0-0-0", fileInfo)
      assert(storageManager.committedFileInfos.get("app-1-0").get("0-0-0") eq fileInfo)
      storageManager.cleanupExpiredShuffleKey(storageManager.shuffleKeySet())
      assert(storageManager.committedFileInfos.isEmpty)
      assert(storageManager.shuffleKeySet().isEmpty)
    }
  }

  test(
    "[CELEBORN-2495] cleanup should remove metadata committed while waiting for a writer to close") {
    withDiskFile(gracefulShutdown = true) { (storageManager, fileInfo, conf) =>
      val manager = spy(storageManager)
      val closing = new CountDownLatch(1)
      val cleanupStarted = new CountDownLatch(1)
      val finishClose = new CountDownLatch(1)
      val tier = mock[TierWriterBase]
      val policy = mock[StoragePolicy]
      val constructorManager = mock[StorageManager]
      when(constructorManager.storagePolicy).thenReturn(policy)
      when(policy.createFileWriter(
        any[PartitionDataWriterContext],
        any[AtomicInteger],
        any[FlushNotifier])).thenReturn(tier)
      Mockito.doAnswer(new Answer[java.lang.Long] {
        override def answer(invocation: InvocationOnMock): java.lang.Long = {
          closing.countDown()
          assert(finishClose.await(30, TimeUnit.SECONDS))
          manager.notifyFileInfoCommitted("app-1-0", "0-0-0", fileInfo)
          0L
        }
      }).when(tier).close()

      // Exercise the real close/destroy monitor while controlling when commit completes.
      val context = new PartitionDataWriterContext(
        Long.MaxValue,
        PartitionSplitMode.HARD,
        false,
        genPartitionLocation(0, Array(0L)),
        "app-1",
        0,
        new UserIdentifier("t1", "u1"),
        PartitionType.REDUCE,
        false,
        false)
      val writer = new PartitionDataWriter(
        constructorManager,
        mock[WorkerSource],
        conf,
        DeviceMonitor.EmptyMonitor(),
        context)
      val writers = new ConcurrentHashMap[String, PartitionDataWriter]()
      writers.put(fileInfo.getFilePath, writer)
      manager.workingDirWriters.put(
        fileInfo.getFile.getParentFile.getParentFile.getParentFile,
        writers)
      Mockito.doAnswer(new Answer[java.lang.Boolean] {
        override def answer(invocation: InvocationOnMock): java.lang.Boolean = {
          cleanupStarted.countDown()
          invocation.callRealMethod().asInstanceOf[java.lang.Boolean]
        }
      }).when(manager).cleanFileInternal("app-1-0", fileInfo)

      val expiredShuffleKeys = manager.shuffleKeySet()
      val pool = Executors.newFixedThreadPool(2)
      try {
        val closeResult = pool.submit(new Callable[java.lang.Long] {
          override def call(): java.lang.Long = writer.close()
        })
        assert(closing.await(30, TimeUnit.SECONDS))
        val cleanupResult = pool.submit(new Runnable {
          override def run(): Unit = manager.cleanupExpiredShuffleKey(expiredShuffleKeys)
        })
        assert(cleanupStarted.await(30, TimeUnit.SECONDS))
        finishClose.countDown()
        closeResult.get(30, TimeUnit.SECONDS)
        cleanupResult.get(30, TimeUnit.SECONDS)
        assert(manager.shuffleKeySet().isEmpty)
        assert(manager.committedFileInfos.isEmpty)
      } finally {
        finishClose.countDown()
        pool.shutdownNow()
        pool.awaitTermination(30, TimeUnit.SECONDS)
      }
    }
  }

  test("updateDiskInfosWithDiskReserveSize") {
    // reserve size set to 5g
    val conf = new CelebornConf().set(WORKER_DISK_RESERVE_SIZE, Utils.byteStringAsBytes("5g"))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)

    val diskInfo = new DiskInfo("/mnt/disk1", List.empty, null, conf)
    diskInfo.setUsableSpace(-1L)

    var diskSetSpace = (0L, 0L)
    doReturn(List(diskInfo)).when(spyStorageManager).localDisksSnapshot()
    doAnswer(diskSetSpace).when(spyStorageManager).getFileSystemReportedSpace(any)

    // disk usable 80g, total 80g, worker config 8EB
    diskSetSpace = (80 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = Long.MaxValue
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 75 * 1024 * 1024 * 1024L)

    // disk usable 80g, total 80g, worker config 50g
    diskInfo.configuredUsableSpace = 50 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 50 * 1024 * 1024 * 1024L)

    // disk usable 10g, total 80g, worker config 20g
    diskSetSpace = (10 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 5 * 1024 * 1024 * 1024L)

    // disk usable 10g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 5 * 1024 * 1024 * 1024L)

    // disk usable 5g, total 80g, worker config 20g
    diskSetSpace = (5 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 5g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 1g, total 80g, worker config 20g
    diskSetSpace = (1 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    diskInfo.configuredUsableSpace = 20 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)

    // disk usable 1g, total 80g, worker config 5g
    diskInfo.configuredUsableSpace = 5 * 1024 * 1024 * 1024L
    diskInfo.setUsableSpace(-1L)
    spyStorageManager.updateDiskInfos()
    assert(diskInfo.actualUsableSpace == 0L)
  }

  test("[CELEBORN-2310] Ensure createFile rejected with disks are full, but status is HEALTHY") {
    val conf = new CelebornConf().set(WORKER_DISK_RESERVE_SIZE, Utils.byteStringAsBytes("5g")).set(
      WORKER_STORAGE_DIRS,
      Seq("/"))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)
    val diskInfo = new DiskInfo("/", List.empty, null, conf)
    diskInfo.setUsableSpace(-1L)
    // Should fail even if the status is HEALTHY
    diskInfo.setStatus(DiskStatus.HEALTHY)
    doReturn(List(diskInfo)).when(spyStorageManager).localDisksSnapshot()

    val partitionLocation = genPartitionLocation(0, Array(0L))

    try {
      val file = storageManager.createDiskFile(
        partitionLocation,
        "myAppId",
        0,
        "myFile",
        new UserIdentifier("t1", "u1"),
        PartitionType.REDUCE,
        partitionSplitEnabled = false)
      fail("Should throw IOException when disks are full")
    } catch {
      case e: IOException =>
        assert(e.getMessage.equals(
          s"No available disks! suggested mountPoint ${partitionLocation.getStorageInfo.getMountPoint}"))
      case e: Throwable =>
        fail(s"Should throw IOException, but got ${e.getClass.getSimpleName}", e)
    }
  }

  private def withDiskFile(gracefulShutdown: Boolean)(
      run: (StorageManager, DiskFileInfo, CelebornConf) => Unit): Unit = {
    val root = Files.createTempDirectory("committed-file-info-")
    val recoveryDir = Files.createDirectory(root.resolve("recovery"))
    val conf = new CelebornConf()
      .set(WORKER_STORAGE_DIRS, Seq(root.resolve("data").toString))
      .set(WORKER_GRACEFUL_SHUTDOWN_ENABLED, gracefulShutdown)
      .set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, recoveryDir.toString)
      .set(WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_INTERVAL, TimeUnit.HOURS.toMillis(1))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    try {
      storageManager.localDisksSnapshot().foreach(_.setUsableSpace(Long.MaxValue))
      val (_, fileInfo, _) = storageManager.createDiskFile(
        genPartitionLocation(0, Array(0L)),
        "app-1",
        0,
        "0-0-0",
        new UserIdentifier("t1", "u1"),
        PartitionType.REDUCE,
        partitionSplitEnabled = false)
      run(storageManager, fileInfo, conf)
    } finally {
      storageManager.cleanupExpiredShuffleKey(storageManager.shuffleKeySet(), cleanDB = false)
      storageManager.committedFileInfos.clear()
      storageManager.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN)
      JavaUtils.deleteRecursively(root.toFile)
    }
  }

  private def genPartitionLocation(epoch: Int, offsets: Array[Long]): PartitionLocation = {
    val location: PartitionLocation =
      new PartitionLocation(0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
    val storageInfo: StorageInfo = new StorageInfo(
      StorageInfo.Type.HDD,
      "/",
      false,
      "filePath",
      StorageInfo.ALL_TYPES_AVAILABLE_MASK,
      offsets(offsets.length - 1),
      new util.ArrayList[lang.Long]())
    location.setStorageInfo(storageInfo)
    location
  }
}
