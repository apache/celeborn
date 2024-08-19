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

import org.mockito.{Mockito, MockitoSugar}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.stubbing.Stubber

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_GRACEFUL_SHUTDOWN_ENABLED, WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH}
import org.apache.celeborn.common.meta.DiskInfo
import org.apache.celeborn.common.util.DiskUtils
import org.apache.celeborn.service.deploy.worker.WorkerSource

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class StorageManagerSuite extends CelebornFunSuite with MockitoHelper {

  val conf = new CelebornConf()

  test("[CELEBORN-926] saveAllCommittedFileInfosToDB cause IllegalMonitorStateException") {
    val conf = new CelebornConf().set(WORKER_GRACEFUL_SHUTDOWN_ENABLED, true)
      .set(WORKER_GRACEFUL_SHUTDOWN_RECOVER_PATH, "/tmp/recover")
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    // should not throw IllegalMonitorStateException exception
    storageManager.saveAllCommittedFileInfosToDB()
  }

  test("updateDiskInfosWithDiskReserveSize") {
    val storageManager = new StorageManager(conf, new WorkerSource(conf))
    val spyStorageManager = spy(storageManager)

    val disks = prepareDisks()
    val diskSetSpace = List(80 * 1024 * 1024 * 1024L, 80 * 1024 * 1024 * 1024L)
    doReturn(disks).when(spyStorageManager).disksSnapshot()
    doReturn(diskSetSpace).when(spyStorageManager).getFileSystemReportedSpace(any)
    spyStorageManager.updateDiskInfos()
    for (disk <- disks) {
      val minimumReserveSize =
        DiskUtils.getMinimumUsableSize(
          disk,
          conf.workerDiskReserveSize,
          conf.workerDiskReserveRatio)
      assert(disk.actualUsableSpace == diskSetSpace.head - minimumReserveSize)
    }
  }

  def prepareDisks(): List[DiskInfo] = {
    val diskSetSpaces = Array(
      90L * 1024 * 1024 * 1024,
      95L * 1024 * 1024 * 1024,
      100L * 1024 * 1024 * 1024)

    val diskInfo1 = new DiskInfo("/mnt/disk1", List.empty, null, conf)
    diskInfo1.configuredUsableSpace = (Long.MaxValue)
    diskInfo1.setUsableSpace(diskSetSpaces(0))

    val diskInfo2 = new DiskInfo("/mnt/disk2", List.empty, null, conf)
    diskInfo2.configuredUsableSpace = (Long.MaxValue)
    diskInfo2.setUsableSpace(diskSetSpaces(1))

    val diskInfo3 = new DiskInfo("/mnt/disk3", List.empty, null, conf)
    diskInfo3.configuredUsableSpace = (Long.MaxValue)
    diskInfo3.setUsableSpace(diskSetSpaces(2))

    List(diskInfo1, diskInfo2, diskInfo3)
  }
}
