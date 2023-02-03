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
import java.util
import java.util.{HashSet => JHashSet}

import org.junit.Assert
import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.FileInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments, WorkerSource, WorkingPartition}

class WorkerSuite extends AnyFunSuite {
  val conf = new CelebornConf()
  val workerArgs = new WorkerArguments(Array(), conf)
  test("clean up") {

    val worker = new Worker(conf, workerArgs)
    val expiredShuffleKeys = new JHashSet[String]()
    val shuffleKey1 = "s1"
    val shuffleKey2 = "s2"
    expiredShuffleKeys.add(shuffleKey1)
    expiredShuffleKeys.add(shuffleKey2)
    val pl1 = new PartitionLocation(0, 0, "12", 0, 0, 0, 0, PartitionLocation.Mode.MASTER)
    val pl2 = new PartitionLocation(0, 0, "12", 0, 0, 0, 0, PartitionLocation.Mode.SLAVE)

    val dir1 = new File("/tmp/work1")
    val dir2 = new File("/tmp/work2")
    val dir3 = new File("/tmp/work3")

    val filePath1 = new File(dir1, "/1/1")
    filePath1.mkdirs()
    val file1 = new File(filePath1, "/1")
    file1.createNewFile()
    val filePath2 = new File(dir2, "/2/2")
    filePath2.mkdirs()
    val file2 = new File(filePath2, "/2")
    file2.createNewFile()

    val filePath3 = new File(dir3, "/3/3")
    filePath3.mkdirs()
    val file3 = new File(filePath3, "/3")
    file3.createNewFile()

    val fw1 = new ReducePartitionFileWriter(
      new FileInfo(file1.getAbsolutePath, null, null, PartitionType.REDUCE),
      mock[Flusher],
      new WorkerSource(conf),
      conf,
      mock[DeviceMonitor],
      0,
      null,
      false)
    fw1.registerDestroyHook(new util.ArrayList(util.Arrays.asList(fw1)))
    val fw2 = new ReducePartitionFileWriter(
      new FileInfo(file2.getAbsolutePath, null, null, PartitionType.REDUCE),
      mock[Flusher],
      new WorkerSource(conf),
      conf,
      mock[DeviceMonitor],
      0,
      null,
      false)
    fw2.registerDestroyHook(new util.ArrayList(util.Arrays.asList(fw2)))
    val fw3 = new ReducePartitionFileWriter(
      new FileInfo(file3.getAbsolutePath, null, null, PartitionType.REDUCE),
      mock[Flusher],
      new WorkerSource(conf),
      conf,
      mock[DeviceMonitor],
      0,
      null,
      false)
    fw3.registerDestroyHook(new util.ArrayList(util.Arrays.asList(fw3)))

    val wl1 = new WorkingPartition(pl1, fw1)
    val wl2 = new WorkingPartition(pl2, fw2)
    val wl3 = new WorkingPartition(pl2, fw3)
    worker.partitionLocationInfo.addMasterPartitions(shuffleKey1, util.Arrays.asList(wl1))
    worker.partitionLocationInfo.addMasterPartitions(shuffleKey1, util.Arrays.asList(wl3))
    worker.partitionLocationInfo.addSlavePartitions(shuffleKey1, util.Arrays.asList(wl2))

    val fws1 = new util.ArrayList[FileWriter]()
    fws1.add(fw1)
    val fws2 = new util.ArrayList[FileWriter]()
    fws2.add(fw2)
    worker.storageManager.workingDirWriters.put(dir1, fws1)
    worker.storageManager.workingDirWriters.put(dir2, fws2)
    Assert.assertEquals(1, worker.storageManager.workingDirWriters.get(dir1).size())
    Assert.assertEquals(1, worker.storageManager.workingDirWriters.get(dir2).size())
    worker.cleanup(expiredShuffleKeys)
    Assert.assertEquals(0, worker.storageManager.workingDirWriters.get(dir1).size())
    Assert.assertEquals(0, worker.storageManager.workingDirWriters.get(dir2).size())

    deleteFile(dir1)
    deleteFile(dir2)
    deleteFile(dir3)
  }
  def deleteFile(dir: File): Unit = {
    val files = dir.listFiles();
    if (files != null) {
      files.foreach(file => {
        if (file.isFile()) {
          file.delete();
        } else {
          deleteFile(file);
        }
      })
      dir.delete();
    }
  }
}
