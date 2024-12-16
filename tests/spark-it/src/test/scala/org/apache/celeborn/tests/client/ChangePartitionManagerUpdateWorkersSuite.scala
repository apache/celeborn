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

package org.apache.celeborn.tests.client

import java.util
import java.util.Collections

import scala.collection.JavaConverters.{asScalaSetConverter, mapAsScalaMapConverter}

import org.apache.celeborn.client.{ChangePartitionManager, ChangePartitionRequest, LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ChangePartitionManagerUpdateWorkersSuite extends WithShuffleClientSuite
  with MiniClusterFeature {
  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    val testConf = Map(
      s"${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}" -> "3")
    val (master, _) = setupMiniClusterWithRandomPorts(testConf, testConf, workerNum = 1)
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("test changePartition with available workers expansion") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
      .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR.key, "0.0")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager: ChangePartitionManager =
      new ChangePartitionManager(conf, lifecycleManager)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 1)

    lifecycleManager.setupEndpoints(
      res.workerResource.keySet(),
      shuffleId,
      new ShuffleFailedWorkers())

    val reserveSlotsSuccess = lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo(workerInfo)
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
          lifecycleManager.updateLatestPartitionLocations(shuffleId, primaryLocations)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == 1)

    // total workerNum is 1 + 2 = 3 now
    setUpWorkers(workerConfForAdding, 2)
    assert(workerInfos.size == 3)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      changePartitionManager.changePartitionRequests.computeIfAbsent(
        shuffleId,
        changePartitionManager.rpcContextRegisterFunc)
      changePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }
    Thread.sleep(5000)
    assert(lifecycleManager.workerSnapshots(shuffleId).size() > 1)

    lifecycleManager.stop()
  }

  test("test changePartition with available workers shrink") {
    setUpWorkers(workerConfForAdding, 1)
    // total workers 1 + 1 = 2
    assert(workerInfos.size == 2)
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
      .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR.key, "0.5")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager: ChangePartitionManager =
      new ChangePartitionManager(conf, lifecycleManager)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    val workerNum = res.workerResource.keySet().size()
    assert(workerNum == 2)

    lifecycleManager.setupEndpoints(
      res.workerResource.keySet(),
      shuffleId,
      new ShuffleFailedWorkers())

    val reserveSlotsSuccess = lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo(workerInfo)
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
          lifecycleManager.updateLatestPartitionLocations(shuffleId, primaryLocations)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)

    // shut down workers test
    // total workerNum is 2 - 1 = 1 now
    val workerInfoList = workerInfos.toList
    0 until 1 foreach { index =>
      val (worker, _) = workerInfoList(index)
      // Workers in miniClusterFeature wont update status with master through heartbeat.
      // So update status manually.
      masterInfo._1.statusSystem.updateExcludedWorkersMeta(
        Collections.singletonList(worker.workerInfo),
        Collections.emptyList())

      val failedWorker = new ShuffleFailedWorkers()
      failedWorker.put(
        worker.workerInfo,
        (StatusCode.RESERVE_SLOTS_FAILED, System.currentTimeMillis()))
      lifecycleManager.workerStatusTracker.recordWorkerFailure(failedWorker)
    }

    val tmpSnapshotWorkers =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .keySet()
        .asScala
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)
    assert(tmpSnapshotWorkers.size == 1)

    // add another new worker
    setUpWorkers(workerConfForAdding, 1)
    assert(workerInfos.size == 3)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      changePartitionManager.changePartitionRequests.computeIfAbsent(
        shuffleId,
        changePartitionManager.rpcContextRegisterFunc)
      changePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }

    val snapshotCandidates =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .keySet()
        .asScala
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)

    assert(snapshotCandidates.size == 2)
    lifecycleManager.stop()
  }

  test("test changePartition with available workers and factor") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
      .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR.key, "1.0")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager: ChangePartitionManager =
      new ChangePartitionManager(conf, lifecycleManager)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)

    // workerNum is 1
    val workerNum = res.workerResource.keySet().size()
    assert(workerNum == 1)

    lifecycleManager.setupEndpoints(
      res.workerResource.keySet(),
      shuffleId,
      new ShuffleFailedWorkers())

    val reserveSlotsSuccess = lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo(workerInfo)
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)

    // total workerNum is 1 + 2 = 3 now
    setUpWorkers(workerConfForAdding, 2)
    assert(workerInfos.size == 3)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      changePartitionManager.changePartitionRequests.computeIfAbsent(
        shuffleId,
        changePartitionManager.rpcContextRegisterFunc)
      changePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)

    lifecycleManager.stop()
  }

  test("test changePartition without available workers") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
      .set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager: ChangePartitionManager =
      new ChangePartitionManager(conf, lifecycleManager)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)

    // workerNum is 1
    val workerNum = res.workerResource.keySet().size()
    assert(workerNum == 1)

    lifecycleManager.setupEndpoints(
      res.workerResource.keySet(),
      shuffleId,
      new ShuffleFailedWorkers())

    val reserveSlotsSuccess = lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo(workerInfo)
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)

    // total workerNum is 1 + 2 = 3 now
    setUpWorkers(workerConfForAdding, 2)
    // longer than APPLICATION_HEARTBEAT_INTERVAL 10s
    assert(workerInfos.size == 3)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      changePartitionManager.changePartitionRequests.computeIfAbsent(
        shuffleId,
        changePartitionManager.rpcContextRegisterFunc)
      changePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)

    lifecycleManager.stop()
  }

  override def afterEach(): Unit = {
    logInfo("test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
