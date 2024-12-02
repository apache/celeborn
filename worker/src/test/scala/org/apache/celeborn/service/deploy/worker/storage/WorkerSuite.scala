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
import java.nio.file.{Files, Paths}
import java.util
import java.util.{HashSet => JHashSet}

import scala.collection.JavaConverters._

import org.junit.Assert
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType}
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, ThreadUtils}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments}

class WorkerSuite extends AnyFunSuite with BeforeAndAfterEach {
  private var worker: Worker = _
  private val conf = new CelebornConf()
  private val workerArgs = new WorkerArguments(Array(), conf)

  override def beforeEach(): Unit = {
    assert(null == worker)
    conf.set(s"${CelebornConf.WORKER_DISK_MONITOR_CHECKLIST.key}", "readwrite")
  }

  override def afterEach(): Unit = {
    if (null != worker) {
      worker.rpcEnv.shutdown()
      worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      worker = null
    }
  }

  test("clean up") {
    val tmpFile =
      Files.createTempDirectory(Paths.get("/tmp"), "celeborn" + System.currentTimeMillis())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, tmpFile.toString)
    worker = new Worker(conf, workerArgs)

    val pl1 = new PartitionLocation(0, 0, "12", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
    val pl2 = new PartitionLocation(1, 0, "12", 0, 0, 0, 0, PartitionLocation.Mode.REPLICA)

    worker.storageManager.createPartitionDataWriter(
      "1",
      1,
      pl1,
      100000,
      PartitionSplitMode.SOFT,
      PartitionType.REDUCE,
      true,
      new UserIdentifier("1", "2"))
    worker.storageManager.createPartitionDataWriter(
      "2",
      2,
      pl2,
      100000,
      PartitionSplitMode.SOFT,
      PartitionType.REDUCE,
      true,
      new UserIdentifier("1", "2"))

    Assert.assertEquals(1, worker.storageManager.workingDirWriters.values().size())
    val expiredShuffleKeys = new JHashSet[String]()
    val shuffleKey1 = "1-1"
    val shuffleKey2 = "2-2"
    expiredShuffleKeys.add(shuffleKey1)
    expiredShuffleKeys.add(shuffleKey2)
    worker.cleanup(
      expiredShuffleKeys,
      ThreadUtils.newDaemonCachedThreadPool(
        "worker-clean-expired-shuffle-keys",
        conf.workerCleanThreads))
    Thread.sleep(3000)
    worker.storageManager.workingDirWriters.values().asScala.map(t => assert(t.size() == 0))
  }

  test("flush filewriters") {
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/tmp")
    worker = new Worker(conf, workerArgs)
    val dir = new File("/tmp")
    val allWriters = new util.HashSet[PartitionDataWriter]()
    val map = JavaUtils.newConcurrentHashMap[String, PartitionDataWriter]()
    worker.storageManager.workingDirWriters.put(dir, map)
    worker.storageManager.workingDirWriters.asScala.foreach { case (_, writers) =>
      writers.synchronized {
        // Filter out FileWriter that already has IOException to avoid printing too many error logs
        allWriters.addAll(writers.values().asScala.filter(_.getException == null).asJavaCollection)
      }
    }
    Assert.assertEquals(0, allWriters.size())

    val fileWriter = mock[PartitionDataWriter]
    when(fileWriter.getException).thenReturn(null)
    map.put("1", fileWriter)
    worker.storageManager.workingDirWriters.asScala.foreach { case (_, writers) =>
      writers.synchronized {
        // Filter out FileWriter that already has IOException to avoid printing too many error logs
        allWriters.addAll(writers.values().asScala.filter(_.getException == null).asJavaCollection)
      }
    }
    Assert.assertEquals(1, allWriters.size())
  }

  test("handle top resource consumption") {
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/tmp")
    worker = new Worker(conf, workerArgs)
    val userIdentifier = new UserIdentifier("default", "celeborn")
    worker.handleTopResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app1" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 2)
    worker.handleTopResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app2" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 2)
    worker.handleTopResourceConsumption(Map.empty[UserIdentifier, ResourceConsumption].asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 0)
  }

  test("only gauge hdfs metrics if HDFS storage enabled") {
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/tmp")
    conf.set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "HDD,HDFS")
    conf.set(CelebornConf.HDFS_DIR.key, "hdfs://localhost:9000/test")

    worker = new Worker(conf, workerArgs)
    val userIdentifier = new UserIdentifier("default", "celeborn")
    worker.handleTopResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app1" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 4)
  }
}
