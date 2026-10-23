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
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import io.netty.buffer.Unpooled
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.RawLocalFileSystem
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.service.deploy.worker.WorkerSource

class StorageManagerMixedStorageSuite extends CelebornFunSuite with MockitoSugar {

  for {
    storageType <- Seq(StorageInfo.Type.HDD, StorageInfo.Type.HDFS)
    memoryState <- Seq("absent", "empty", "other-file")
    gracefulShutdown <- Seq(false, true)
  } {
    val suffix = if (gracefulShutdown) " with graceful shutdown" else ""
    test(
      s"[CELEBORN-2485] cleanFile removes $storageType file with $memoryState memory map$suffix") {
      withStorageManager(gracefulShutdown) { (storageManager, _, _) =>
        val appId = "celeborn-2485-app"
        val shuffleId = 1
        val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
        val fileName = "0-0-0"
        val user = new UserIdentifier("test", "test")
        val location =
          new PartitionLocation(0, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
        location.setStorageInfo(
          new StorageInfo("", storageType, StorageInfo.ALL_TYPES_AVAILABLE_MASK))

        val (_, info, workingDir) = storageManager.createDiskFile(
          location,
          appId,
          shuffleId,
          fileName,
          user,
          PartitionType.REDUCE,
          partitionSplitEnabled = false,
          overrideStorageType = storageType)
        val fs = StorageManager.hadoopFs.get(StorageInfo.Type.HDFS)
        if (storageType == StorageInfo.Type.HDFS) {
          // Exercise the HDFS cleanup branch with real files and no external HDFS service.
          fs.create(info.getDfsPath).close()
        }

        val writer = mock[PartitionDataWriter]
        val writerDestroyed = new AtomicBoolean(false)
        doAnswer { (_: InvocationOnMock) =>
          writerDestroyed.set(true)
          // Isolate lookup/removal from the writer's asynchronous flush and close machinery.
          info.deleteAllFiles(if (info.isHdfs) fs else null)
          ()
        }.when(writer).destroy(any[IOException])
        storageManager.registerDiskFilePartitionWriter(writer, workingDir, info)
        if (gracefulShutdown) {
          storageManager.notifyFileInfoCommitted(shuffleKey, fileName, info)
          assert(storageManager.committedFileInfos.get(shuffleKey).get(fileName) eq info)
        }

        if (memoryState != "absent") {
          val memoryInfo = storageManager.createMemoryFileInfo(
            appId,
            shuffleId,
            "other-memory-file",
            user,
            PartitionType.REDUCE,
            partitionSplitEnabled = false)
          if (memoryState == "empty") {
            // Eviction also leaves the per-shuffle memory map present but empty.
            storageManager.unregisterMemoryPartitionWriterAndFileInfo(
              memoryInfo,
              shuffleKey,
              "other-memory-file")
          }
        }
        val memoryMap = storageManager.memoryFileInfos.get(shuffleKey)
        val otherMemoryInfo =
          if (memoryMap == null) null else memoryMap.get("other-memory-file")
        def dataExists: Boolean =
          if (info.isHdfs) fs.exists(info.getDfsPath) else info.getFile.exists()

        assert(dataExists, "Fixture must contain a real data file before cleanup")
        assert(storageManager.getFileInfo(shuffleKey, fileName) eq info)

        storageManager.cleanFile(shuffleKey, fileName)

        val remainingFileInfo = storageManager.getFileInfo(shuffleKey, fileName)
        val writerRegistered =
          if (info.isHdfs) {
            storageManager.hdfsWriters.containsKey(info.getFilePath)
          } else {
            storageManager.workingDirWriters.get(workingDir).containsKey(info.getFilePath)
          }
        val evidence = s"CELEBORN-2485 storage=$storageType memory=$memoryState " +
          s"writerDestroyed=${writerDestroyed.get()} dataExists=$dataExists " +
          s"fileInfoPresent=${remainingFileInfo != null} writerRegistered=$writerRegistered " +
          s"gracefulShutdown=$gracefulShutdown"
        logInfo(evidence)
        withClue(evidence + ": ") {
          assert(writerDestroyed.get(), "cleanFile must destroy the target writer")
          assert(!dataExists, "cleanFile must delete the target data file")
          assert(remainingFileInfo == null, "cleanFile must remove the target file metadata")
          assert(!writerRegistered, "cleanFile must unregister the target writer")
          if (gracefulShutdown) {
            assert(
              storageManager.committedFileInfos.get(shuffleKey).get(fileName) == null,
              "cleanFile must remove the committed file metadata")
          }
          if (memoryMap != null) {
            assert(storageManager.memoryFileInfos.get(shuffleKey) eq memoryMap)
            assert(memoryMap.get("other-memory-file") eq otherMemoryInfo)
          }
        }
      }
    }
  }

  test("[CELEBORN-2485] cleanFile destroys a real memory writer and preserves other files") {
    withStorageManager(gracefulShutdown = false) { (storageManager, conf, source) =>
      val appId = "celeborn-2485-app"
      val shuffleId = 1
      val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
      val user = new UserIdentifier("test", "test")
      val location =
        new PartitionLocation(0, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
      location.setStorageInfo(
        new StorageInfo("", StorageInfo.Type.MEMORY, StorageInfo.ALL_TYPES_AVAILABLE_MASK))
      val fileName = location.getFileName
      val memoryInfo = storageManager.createMemoryFileInfo(
        appId,
        shuffleId,
        fileName,
        user,
        PartitionType.REDUCE,
        partitionSplitEnabled = false)
      val otherMemoryInfo = storageManager.createMemoryFileInfo(
        appId,
        shuffleId,
        "other-memory-file",
        user,
        PartitionType.REDUCE,
        partitionSplitEnabled = false)
      val (_, diskInfo, _) = storageManager.createDiskFile(
        location,
        appId,
        shuffleId,
        "other-disk-file",
        user,
        PartitionType.REDUCE,
        partitionSplitEnabled = false,
        overrideStorageType = StorageInfo.Type.HDD)
      val context = new PartitionDataWriterContext(
        256 * 1024 * 1024L,
        PartitionSplitMode.HARD,
        false,
        location,
        appId,
        shuffleId,
        user,
        PartitionType.REDUCE,
        false,
        false)
      // Select a real memory tier directly without initializing the global MemoryManager.
      val constructorManager = mock[StorageManager]
      val policy = mock[StoragePolicy]
      when(constructorManager.storagePolicy).thenReturn(policy)
      var tier: MemoryTierWriter = null
      when(policy.createFileWriter(
        any[PartitionDataWriterContext],
        any[AtomicInteger],
        any[FlushNotifier])).thenAnswer {
        (invocation: InvocationOnMock) =>
          tier = new MemoryTierWriter(
            conf,
            new ReducePartitionMetaHandler(false, memoryInfo),
            invocation.getArgument[AtomicInteger](1),
            invocation.getArgument[FlushNotifier](2),
            source,
            memoryInfo,
            StorageInfo.Type.MEMORY,
            context,
            storageManager)
          tier
      }
      val writer = new PartitionDataWriter(
        constructorManager,
        source,
        conf,
        EmptyDeviceMonitor,
        context)
      try {
        // Own real bytes so the cleanup assertion checks actual Netty buffer release.
        val component = Unpooled.buffer(4).writeInt(42)
        tier.flushBuffer.addComponent(true, component)
        memoryInfo.setBuffer(tier.flushBuffer)
        assert(storageManager.getFileInfo(shuffleKey, fileName) eq memoryInfo)
        assert(storageManager.memoryWriters.get(memoryInfo) eq writer)
        assert(memoryInfo.getBuffer.readInt() == 42)

        storageManager.cleanFile(shuffleKey, fileName)

        assert(writer.isClosed)
        assert(tier.flushBuffer.refCnt() == 0)
        assert(component.refCnt() == 0)
        assert(storageManager.getFileInfo(shuffleKey, fileName) == null)
        assert(!storageManager.memoryWriters.containsKey(memoryInfo))
        assert(storageManager.getFileInfo(shuffleKey, "other-memory-file") eq otherMemoryInfo)
        assert(storageManager.getFileInfo(shuffleKey, "other-disk-file") eq diskInfo)
        assert(diskInfo.getFile.exists())
      } finally {
        writer.destroy(new IOException("test cleanup"))
      }
    }
  }

  private def withStorageManager(gracefulShutdown: Boolean)(
      run: (StorageManager, CelebornConf, WorkerSource) => Unit): Unit = {
    val root = Files.createTempDirectory("celeborn-2485-")
    val recoveryDir = Files.createDirectory(root.resolve("recovery"))
    val conf = new CelebornConf()
      .set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "MEMORY,HDD,HDFS")
      .set(CelebornConf.HDFS_DIR.key, root.resolve("hdfs").toUri.toString)
      .set(
        CelebornConf.WORKER_STORAGE_DIRS,
        Seq(s"${root.resolve("local")}:disktype=HDD:flushthread=0"))
      // This cleanup-only fixture never submits a flush task.
      .set(CelebornConf.WORKER_FLUSHER_HDFS_THREADS, 0)
      .set(CelebornConf.WORKER_DISK_MONITOR_ENABLED, false)
      .set(CelebornConf.WORKER_GRACEFUL_SHUTDOWN_ENABLED, gracefulShutdown)
      .set(CelebornConf.WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, recoveryDir.toString)
      .set(
        CelebornConf.WORKER_GRACEFUL_SHUTDOWN_SAVE_COMMITTED_FILEINFO_INTERVAL,
        TimeUnit.HOURS.toMillis(1))
      .set("celeborn.hadoop.fs.file.impl", classOf[RawLocalFileSystem].getName)
      .set("celeborn.hadoop.fs.file.impl.disable.cache", "true")
    val previousHadoopFs = StorageManager.hadoopFs
    var source: WorkerSource = null
    var storageManager: StorageManager = null
    try {
      source = new WorkerSource(conf)
      storageManager = new StorageManager(conf, source)
      // There is no worker heartbeat to initialize available disk space.
      val localDisks = storageManager.localDisksSnapshot()
      assert(localDisks.nonEmpty, "Fixture must discover the temporary local disk")
      localDisks.foreach(_.setUsableSpace(1024L * 1024 * 1024))
      run(storageManager, conf, source)
    } finally {
      try {
        if (storageManager != null) {
          // Unrelated memory metadata has no writer or buffer in this fixture.
          storageManager.memoryFileInfos.clear()
          try {
            storageManager.cleanupExpiredShuffleKey(storageManager.shuffleKeySet(), cleanDB = false)
          } finally {
            if (gracefulShutdown) {
              storageManager.committedFileInfos.clear()
              // This also shuts down the committed-file-info saver before closing its DB.
              storageManager.close(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN)
            } else {
              storageManager.close(CelebornExitKind.EXIT_IMMEDIATELY)
            }
          }
        }
      } finally {
        try {
          if (source != null) {
            source.destroy()
            assert(
              source.metricsCleaner.awaitTermination(10, TimeUnit.SECONDS),
              "WorkerSource metrics cleaner must terminate")
          }
        } finally {
          StorageManager.hadoopFs = previousHadoopFs
          FileUtils.deleteDirectory(root.toFile)
        }
      }
    }
  }
}
