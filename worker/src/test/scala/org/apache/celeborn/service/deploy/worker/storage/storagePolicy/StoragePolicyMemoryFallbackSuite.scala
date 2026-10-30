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

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Function

import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.mockito.{MockedConstruction, Mockito}
import org.mockito.ArgumentMatchers._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, DiskInfo}
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.protocol.StorageInfo.Type
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.storage._

class StoragePolicyMemoryFallbackSuite extends CelebornFunSuite {
  private val remoteTypes = Seq(Type.S3, Type.HDFS, Type.OSS)

  private class Fixture(
      val allowedTypes: Int = StorageInfo.MEMORY_MASK | StorageInfo.S3_MASK,
      val locationType: Type = Type.MEMORY,
      createOrder: String = "MEMORY,S3",
      backends: Set[Type] = Set(Type.S3),
      val memoryAvailable: Boolean = false) {
    val conf = new CelebornConf()
      .set("celeborn.worker.storage.storagePolicy.createFilePolicy", createOrder)
      .set("celeborn.worker.storage.storagePolicy.evictPolicy", createOrder)
    val manager = Mockito.mock(classOf[StorageManager])
    val source = Mockito.mock(classOf[AbstractSource])
    val hdfsFlusher = Mockito.mock(classOf[HdfsFlusher])
    val s3Flusher = Mockito.mock(classOf[S3Flusher])
    val ossFlusher = Mockito.mock(classOf[OssFlusher])
    val files = new ConcurrentHashMap[String, ConcurrentHashMap[String, DiskFileInfo]]()
    val fileSystems = new ConcurrentHashMap[Type, FileSystem]()
    backends.foreach { storageType =>
      val fs = Mockito.mock(classOf[FileSystem])
      Mockito.when(fs.mkdirs(any[Path]())).thenReturn(true)
      Mockito.when(fs.create(any[Path](), anyBoolean()))
        .thenReturn(Mockito.mock(classOf[FSDataOutputStream]))
      fileSystems.put(storageType, fs)
    }

    // Run the real createDiskFile without starting StorageManager background services.
    private def setField(name: String, value: AnyRef): Unit = {
      val field = classOf[StorageManager].getDeclaredField(name)
      field.setAccessible(true)
      field.set(manager, value)
    }
    setField("conf", conf)
    setField("diskFileInfos", files)
    setField(
      "diskFileInfoMapFunc",
      new Function[String, ConcurrentHashMap[String, DiskFileInfo]] {
        override def apply(key: String): ConcurrentHashMap[String, DiskFileInfo] =
          new ConcurrentHashMap[String, DiskFileInfo]()
      })
    Mockito.when(manager.diskInfos).thenReturn(new ConcurrentHashMap[String, DiskInfo]())
    Mockito.when(manager.healthyLocalWorkingDirs()).thenReturn(Nil)
    Mockito.when(manager.hdfsFlusher).thenReturn(Option(hdfsFlusher).filter(_ =>
      backends(Type.HDFS)))
    Mockito.when(manager.s3Flusher).thenReturn(Option(s3Flusher).filter(_ => backends(Type.S3)))
    Mockito.when(manager.ossFlusher).thenReturn(Option(ossFlusher).filter(_ => backends(Type.OSS)))
    Mockito.when(manager.hdfsDir).thenReturn(
      if (backends(Type.HDFS)) "hdfs://test-host/celeborn" else "")
    Mockito.when(manager.s3Dir).thenReturn(
      if (backends(Type.S3)) "s3a://test-bucket/celeborn" else "")
    Mockito.when(manager.ossDir).thenReturn(
      if (backends(Type.OSS)) "oss://test-bucket/celeborn" else "")
    Mockito.when(manager.hdfsPermission).thenReturn(new FsPermission("755"))
    Mockito.when(manager.localOrDfsStorageAvailable).thenReturn(backends.nonEmpty)
    Mockito.doCallRealMethod().when(manager).createDiskFile(
      any[PartitionLocation](),
      anyString(),
      anyInt(),
      anyString(),
      any[UserIdentifier](),
      any[PartitionType](),
      anyBoolean(),
      any[Type]())

    val location = new PartitionLocation(
      0,
      1,
      "localhost",
      1,
      2,
      3,
      4,
      PartitionLocation.Mode.PRIMARY,
      null,
      new StorageInfo("", locationType, allowedTypes),
      null)
    val context = new PartitionDataWriterContext(
      1024L,
      PartitionSplitMode.HARD,
      false,
      location,
      "app",
      1,
      new UserIdentifier("tenant", "user"),
      PartitionType.REDUCE,
      true,
      false)
    val policy = new StoragePolicy(conf, manager, source)

    def create(): TierWriterBase =
      policy.createFileWriter(context, new AtomicInteger(), new FlushNotifier)

    def assertRemoteWriter(writer: TierWriterBase, expected: Type): Unit = {
      assert(writer.isInstanceOf[DfsTierWriter])
      assert(writer.storageType == expected)
      val info = files.get(context.getShuffleKey).get(location.getFileName)
      assert(info.getStorageType == expected)
      assert(writer.fileInfo eq info)
      assert(context.getWorkingDir == null)
      assert(StorageInfo.isAvailable(expected, allowedTypes))
      assert(location.getStorageInfo.getType == locationType)
      assert(location.getStorageInfo.availableStorageTypes == allowedTypes)
    }
  }

  private def withStorage(f: Fixture, mockDiskWriters: Boolean = true)(
      testBody: Fixture => Unit): Unit = {
    val previousFileSystems = StorageManager.hadoopFs
    val memory = Mockito.mock(classOf[MemoryManager])
    Mockito.when(memory.memoryFileStorageAvailable()).thenReturn(f.memoryAvailable)
    val memorySingleton = Mockito.mockStatic(classOf[MemoryManager])
    try {
      memorySingleton.when[MemoryManager](() => MemoryManager.instance()).thenReturn(memory)
      StorageManager.hadoopFs = f.fileSystems
      // Isolate remote I/O, while retaining real backend selection and file metadata creation.
      val diskWriters =
        if (mockDiskWriters) Some(Mockito.mockConstruction(
          classOf[DfsTierWriter],
          new MockedConstruction.MockInitializer[DfsTierWriter] {
            override def prepare(
                writer: DfsTierWriter,
                context: MockedConstruction.Context): Unit = {
              Mockito.when(writer.fileInfo).thenReturn(
                context.arguments().get(6).asInstanceOf[DiskFileInfo])
              Mockito.when(writer.storageType).thenReturn(
                context.arguments().get(7).asInstanceOf[Type])
            }
          }))
        else None
      try {
        testBody(f)
      } finally {
        diskWriters.foreach(_.close())
      }
    } finally {
      StorageManager.hadoopFs = previousFileSystems
      memorySingleton.close()
    }
  }

  remoteTypes.foreach { storageType =>
    test(s"MEMORY location falls back to $storageType when memory storage is exhausted") {
      withStorage(
        new Fixture(
          allowedTypes = StorageInfo.MEMORY_MASK | storageType.getMask,
          createOrder = s"MEMORY,$storageType",
          backends = Set(storageType)),
        mockDiskWriters = storageType != Type.HDFS) { f =>
        val writer = f.create().asInstanceOf[DfsTierWriter]
        try {
          f.assertRemoteWriter(writer, storageType)
          if (storageType == Type.HDFS) {
            assert(!Mockito.mockingDetails(writer).isMock)
            assert(writer.getFlusher() eq f.hdfsFlusher)
            Mockito.verify(f.fileSystems.get(Type.HDFS)).create(
              writer.fileInfo.asInstanceOf[DiskFileInfo].getDfsPath,
              true)
          }
        } finally {
          writer.closeHdfsStream()
          writer.returnBuffer(false)
        }
      }
    }

    test(s"MEMORY location must not fall back to disallowed $storageType") {
      withStorage(new Fixture(
        allowedTypes = StorageInfo.MEMORY_MASK,
        createOrder = s"MEMORY,$storageType",
        backends = Set(storageType))) { f =>
        intercept[CelebornIOException](f.create())
        assert(f.files.isEmpty)
        Mockito.verifyNoInteractions(f.fileSystems.get(storageType))
      }
    }
  }

  test("default creation policy skips disallowed backends before S3") {
    withStorage(new Fixture(
      createOrder = "MEMORY,SSD,HDD,HDFS,S3,OSS",
      backends = Set(Type.HDFS, Type.S3))) { f =>
      f.assertRemoteWriter(f.create(), Type.S3)
      Mockito.verifyNoInteractions(f.fileSystems.get(Type.HDFS))
    }
  }

  test("MEMORY location starts at the first backend when the policy excludes MEMORY") {
    withStorage(new Fixture(
      allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.HDFS_MASK | StorageInfo.S3_MASK,
      createOrder = "HDFS,S3",
      backends = Set(Type.HDFS, Type.S3),
      memoryAvailable = true)) { f =>
      f.assertRemoteWriter(f.create(), Type.HDFS)
    }
  }

  Seq("MEMORY,S3,HDFS", "MEMORY,HDFS,S3").foreach { order =>
    test(s"MEMORY fallback respects creation order $order") {
      withStorage(new Fixture(
        allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.S3_MASK | StorageInfo.HDFS_MASK,
        createOrder = order,
        backends = Set(Type.S3, Type.HDFS))) { f =>
        f.assertRemoteWriter(f.create(), Type.valueOf(order.split(",")(1)))
      }
    }
  }

  test("MEMORY fallback continues to S3 if HDFS creation fails") {
    withStorage(new Fixture(
      allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.HDFS_MASK | StorageInfo.S3_MASK,
      createOrder = "MEMORY,HDFS,S3",
      backends = Set(Type.HDFS, Type.S3))) { f =>
      Mockito.when(f.fileSystems.get(Type.HDFS).mkdirs(any[Path]()))
        .thenThrow(new IOException("HDFS unavailable"))
      f.assertRemoteWriter(f.create(), Type.S3)
      Mockito.verify(f.fileSystems.get(Type.HDFS)).mkdirs(any[Path]())
    }
  }

  test("MEMORY fallback skips a backend that is not configured on the worker") {
    withStorage(new Fixture(
      allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.HDFS_MASK | StorageInfo.S3_MASK,
      createOrder = "MEMORY,HDFS,S3")) { f =>
      f.assertRemoteWriter(f.create(), Type.S3)
    }
  }

  test("non-memory allocation keeps its target despite another policy candidate") {
    withStorage(new Fixture(
      allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.HDFS_MASK | StorageInfo.S3_MASK,
      locationType = Type.S3,
      createOrder = "MEMORY,HDFS,S3",
      backends = Set(Type.HDFS, Type.S3))) { f =>
      f.assertRemoteWriter(f.create(), Type.S3)
      Mockito.verifyNoInteractions(f.fileSystems.get(Type.HDFS))
    }
  }

  test("eviction still overrides a non-memory allocation hint") {
    withStorage(new Fixture(
      allowedTypes = StorageInfo.MEMORY_MASK | StorageInfo.HDFS_MASK | StorageInfo.S3_MASK,
      locationType = Type.S3,
      createOrder = "MEMORY,HDFS,S3",
      backends = Set(Type.HDFS, Type.S3))) { f =>
      val existingWriter = Mockito.mock(classOf[MemoryTierWriter])
      Mockito.when(existingWriter.storageType).thenReturn(Type.MEMORY)
      val writer = f.policy.getEvictedFileWriter(
        existingWriter,
        f.context,
        new AtomicInteger(),
        new FlushNotifier)
      f.assertRemoteWriter(writer, Type.HDFS)
    }
  }

  test("MEMORY-only creation policy does not use an available S3 backend") {
    withStorage(new Fixture(createOrder = "MEMORY")) { f =>
      intercept[CelebornIOException](f.create())
      assert(f.files.isEmpty)
    }
  }

  test("MEMORY fallback fails when no non-memory storage is available") {
    withStorage(new Fixture(backends = Set.empty)) { f =>
      intercept[CelebornIOException](f.create())
      assert(f.files.isEmpty)
    }
  }
}
