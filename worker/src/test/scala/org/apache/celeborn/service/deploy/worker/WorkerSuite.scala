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

import java.io.File
import java.nio.file.{Files, Paths}
import java.util
import java.util.{HashSet => JHashSet}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.junit.Assert
import org.mockito.MockitoSugar._
import org.scalatest.{shortstacks, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.CommitFilesResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, ThreadUtils}
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter

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
    conf.set(CelebornConf.METRICS_WORKER_APP_TOP_RESOURCE_CONSUMPTION_COUNT, 5)
    worker = new Worker(conf, workerArgs)
    val userIdentifier = new UserIdentifier("default", "celeborn")
    worker.handleTopAppResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app1" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 2)
    worker.handleTopAppResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app2" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 2)
    worker.handleTopAppResourceConsumption(Map.empty[UserIdentifier, ResourceConsumption].asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 0)
  }

  test("only gauge hdfs metrics if HDFS storage enabled") {
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/tmp")
    conf.set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "HDD,HDFS")
    conf.set(CelebornConf.HDFS_DIR.key, "hdfs://localhost:9000/test")

    worker = new Worker(conf, workerArgs)
    val userIdentifier = new UserIdentifier("default", "celeborn")
    worker.handleTopAppResourceConsumption(Map(userIdentifier -> ResourceConsumption(
      1024,
      1,
      0,
      0,
      Map("app1" -> ResourceConsumption(1024, 1, 0, 0)).asJava)).asJava)
    assert(worker.resourceConsumptionSource.gauges().size == 4)
  }

  test("test checkCommitTimeout in controller") {
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, "/tmp")
    conf.set(CelebornConf.WORKER_SHUFFLE_COMMIT_TIMEOUT.key, "1000")
    worker = new Worker(conf, workerArgs)
    val controller = worker.controller
    controller.init(worker)
    val shuffleCommitInfos = controller.shuffleCommitInfos
    val shuffleCommitTime = controller.shuffleCommitTime
    ThreadUtils.shutdown(worker.controller.commitFinishedChecker)
    shuffleCommitInfos.clear()
    shuffleCommitTime.clear()

    val shuffleKey = "1"
    val context = mock[RpcCallContext]
    shuffleCommitInfos.putIfAbsent(
      shuffleKey,
      JavaUtils.newConcurrentHashMap[Long, CommitInfo]())
    val epochCommitMap = shuffleCommitInfos.get(shuffleKey)
    val primaryIds = List("0", "1", "2", "3")
    val replicaIds = List("4", "5", "6", "7")
    val epoch0: Long = 0
    val epoch1: Long = 1
    val epoch2: Long = 2
    val epoch3: Long = 3
    val startWaitTime = System.currentTimeMillis()

    // update an INPROCESS commitInfo
    val response0 = CommitFilesResponse(
      null,
      List.empty.asJava,
      List.empty.asJava,
      primaryIds.asJava,
      replicaIds.asJava)
    epochCommitMap.putIfAbsent(epoch0, new CommitInfo(response0, CommitInfo.COMMIT_INPROCESS))

    val commitInfo0 = epochCommitMap.get(epoch0)
    commitInfo0.synchronized {
      shuffleCommitTime.putIfAbsent(
        shuffleKey,
        JavaUtils.newConcurrentHashMap[Long, (Long, RpcCallContext)]())
      val epochWaitTimeMap = shuffleCommitTime.get(shuffleKey)
      epochWaitTimeMap.put(epoch0, (startWaitTime, context))
    }

    assert(shuffleCommitTime.get(shuffleKey).get(epoch0)._1 == startWaitTime)
    assert(epochCommitMap.get(epoch0).status == CommitInfo.COMMIT_INPROCESS)

    // update an INPROCESS commitInfo
    val response1 = CommitFilesResponse(
      null,
      List.empty.asJava,
      List.empty.asJava,
      primaryIds.asJava,
      replicaIds.asJava)
    epochCommitMap.putIfAbsent(epoch1, new CommitInfo(response1, CommitInfo.COMMIT_INPROCESS))

    val commitInfo1 = epochCommitMap.get(epoch1)
    commitInfo1.synchronized {
      shuffleCommitTime.putIfAbsent(
        shuffleKey,
        JavaUtils.newConcurrentHashMap[Long, (Long, RpcCallContext)]())
      val epochWaitTimeMap = shuffleCommitTime.get(shuffleKey)
      epochWaitTimeMap.put(epoch1, (startWaitTime, context))
    }

    assert(shuffleCommitTime.get(shuffleKey).get(epoch1)._1 == startWaitTime)
    assert(epochCommitMap.get(epoch1).status == CommitInfo.COMMIT_INPROCESS)

    // update an FINISHED commitInfo
    val response2 = CommitFilesResponse(
      StatusCode.SUCCESS,
      primaryIds.asJava,
      replicaIds.asJava,
      List.empty.asJava,
      List.empty.asJava)
    epochCommitMap.put(epoch2, new CommitInfo(response2, CommitInfo.COMMIT_FINISHED))

    val commitInfo2 = epochCommitMap.get(epoch2)
    commitInfo2.synchronized {
      shuffleCommitTime.putIfAbsent(
        shuffleKey,
        JavaUtils.newConcurrentHashMap[Long, (Long, RpcCallContext)]())
      val epochWaitTimeMap = shuffleCommitTime.get(shuffleKey)
      // epoch2 is already timeout
      epochWaitTimeMap.put(epoch2, (startWaitTime, context))
    }

    assert(shuffleCommitTime.get(shuffleKey).get(epoch2)._1 == startWaitTime)
    assert(epochCommitMap.get(epoch2).status == CommitInfo.COMMIT_FINISHED)

    // add a new shuffleKey2 to shuffleCommitTime but not to shuffleCommitInfos
    val shuffleKey2 = "2"
    shuffleCommitTime.putIfAbsent(
      shuffleKey2,
      JavaUtils.newConcurrentHashMap[Long, (Long, RpcCallContext)]())
    shuffleCommitTime.get(shuffleKey2).put(epoch0, (startWaitTime, context))
    assert(shuffleCommitTime.containsKey(shuffleKey2))
    assert(!shuffleCommitInfos.containsKey(shuffleKey2))

    // add an epoch to shuffleCommitTime but not to shuffleCommitInfos
    shuffleCommitTime.get(shuffleKey).put(epoch3, (startWaitTime, context))
    assert(shuffleCommitTime.get(shuffleKey).get(epoch3)._1 == startWaitTime)
    assert(!shuffleCommitInfos.get(shuffleKey).containsKey(epoch3))

    // update status of epoch1 to FINISHED
    epochCommitMap.get(epoch1).status = CommitInfo.COMMIT_FINISHED
    assert(epochCommitMap.get(epoch1).status == CommitInfo.COMMIT_FINISHED)

    // first timeout check
    controller.checkCommitTimeout(shuffleCommitTime)
    assert(epochCommitMap.get(epoch0).status == CommitInfo.COMMIT_INPROCESS)

    // shuffleCommitTime will be removed when shuffleCommitInfos contains no shuffleKey
    assert(!shuffleCommitTime.containsKey(shuffleKey2))
    assert(!shuffleCommitInfos.containsKey(shuffleKey2))

    // epoch will be removed when shuffleCommitInfos contains no epoch
    assert(!shuffleCommitTime.get(shuffleKey).containsKey(epoch3))

    // FINISHED status of epoch1 will be removed from shuffleCommitTime
    assert(shuffleCommitTime.get(shuffleKey).get(epoch1) == null)

    // timeout after 1000 ms
    Thread.sleep(2000)
    controller.checkCommitTimeout(shuffleCommitTime)

    // remove epoch0 in shuffleCommitTime when timeout
    assert(shuffleCommitTime.get(shuffleKey).get(epoch0) == null)
    assert(epochCommitMap.get(epoch0).status == CommitInfo.COMMIT_FINISHED)
    assert(epochCommitMap.get(epoch0).response.status == StatusCode.COMMIT_FILE_EXCEPTION)

    // timeout but SUCCESS epoch2 can reply
    assert(shuffleCommitTime.get(shuffleKey).get(epoch2) == null)
    assert(epochCommitMap.get(epoch2).response.status == StatusCode.SUCCESS)
  }
}
