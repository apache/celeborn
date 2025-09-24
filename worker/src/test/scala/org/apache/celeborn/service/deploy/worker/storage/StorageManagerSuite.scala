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
import java.util.concurrent.ConcurrentHashMap

import org.mockito.{Mockito, MockitoSugar}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.stubbing.Stubber

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_DISK_RESERVE_SIZE, WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH}
import org.apache.celeborn.common.meta.DiskInfo
import org.apache.celeborn.common.util.{JavaUtils, Utils}
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

  test("updateDiskInfosWithDiskReserveSize") {
    // reserve size set to 5g
    val conf = new CelebornConf().set(WORKER_DISK_RESERVE_SIZE, Utils.byteStringAsBytes("5g"))
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)

    val diskInfo = new DiskInfo("/mnt/disk1", List.empty, null, conf)
    diskInfo.setUsableSpace(-1L)

    var diskSetSpace = (0L, 0L)
    doReturn(List(diskInfo)).when(spyStorageManager).disksSnapshot()
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

  test("flush hdfs file writer on trim only if size exceeds threshold") {
    val storageManager = mock[StorageManager]
    doReturn(JavaUtils.newConcurrentHashMap[File, ConcurrentHashMap[String, PartitionDataWriter]]())
      .when(storageManager)
      .workingDirWriters
    doReturn(JavaUtils.newConcurrentHashMap[String, PartitionDataWriter]())
      .when(storageManager)
      .hdfsWriters
    doReturn(1048576L)
      .when(storageManager)
      .flushOnTrimHdfsThreshold

    val dir = new File("/tmp")
    val localMap = JavaUtils.newConcurrentHashMap[String, PartitionDataWriter]()
    val localFileWriter = mock[PartitionDataWriter]
    Mockito.doNothing().when(localFileWriter).flushOnMemoryPressure()
    doReturn(null).when(localFileWriter).getException
    doReturn(100).when(localFileWriter).getFlushableBytes()
    localMap.put("1", localFileWriter)
    storageManager.workingDirWriters.put(dir, localMap)

    val hdfsFileWriter1 = mock[PartitionDataWriter]
    Mockito.doNothing().when(hdfsFileWriter1).flushOnMemoryPressure()
    doReturn(null).when(hdfsFileWriter1).getException
    doReturn(100).when(hdfsFileWriter1).getFlushableBytes()
    val hdfsFileWriter2 = mock[PartitionDataWriter]
    Mockito.doNothing().when(hdfsFileWriter2).flushOnMemoryPressure()
    doReturn(null).when(hdfsFileWriter2).getException
    doReturn(1048576).when(hdfsFileWriter2).getFlushableBytes()
    val hdfsFileWriter3 = mock[PartitionDataWriter]
    Mockito.doNothing().when(hdfsFileWriter3).flushOnMemoryPressure()
    doReturn(null).when(hdfsFileWriter3).getException
    doReturn(2048576).when(hdfsFileWriter3).getFlushableBytes()
    storageManager.hdfsWriters.put("1", hdfsFileWriter1)
    storageManager.hdfsWriters.put("2", hdfsFileWriter2)
    storageManager.hdfsWriters.put("3", hdfsFileWriter3)

    val method = storageManager.getClass.getDeclaredMethod("flushFileWriters")
    method.setAccessible(true)

    method.invoke(storageManager)
    verify(localFileWriter, times(1)).flushOnMemoryPressure()
    verify(hdfsFileWriter1, times(0)).flushOnMemoryPressure()
    verify(hdfsFileWriter2, times(1)).flushOnMemoryPressure()
    verify(hdfsFileWriter3, times(1)).flushOnMemoryPressure()
  }
}
