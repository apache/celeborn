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
import java.util.{Set => JSet}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

import org.mockito.{Mockito, MockitoSugar}
import org.mockito.ArgumentMatchersSugar.any
import org.mockito.stubbing.Stubber

import org.apache.celeborn.client.{ChangePartitionRequest, LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.MiniClusterFeature

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class ChangePartitionManagerUpdateWorkersSuite extends WithShuffleClientSuite
  with MiniClusterFeature with MockitoHelper {

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val testConf = Map(
      s"${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}" -> "3")
    val (master, _) = setupMiniClusterWithRandomPorts(testConf, testConf, workerNum = 1)
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("test changePartition with available workers") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
      .set(CelebornConf.CLIENT_CHANGE_PARTITION_WITH_AVAILABLE_WORKERS.key, "true")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager = lifecycleManager.changePartitionManager
    val mockChangePartitionManager = spy(changePartitionManager)
    doNothing.when(mockChangePartitionManager).replySuccess(
      any[Array[PartitionLocation]],
      any[ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]],
      any[Int])
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

    val slots = res.workerResource
    val candidatesWorkers = new util.HashSet(slots.keySet())
    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo()
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
      lifecycleManager.workerStatusTracker.updateWorkersWithEndpoint(candidatesWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == 1)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == 1)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithoutEndpoint.size() == 0)

    // total workerNum is 1 + 2 = 3 now
    setUpWorkers(workerConfForAdding, 2)
    // longer than APPLICATION_HEARTBEAT_INTERVAL 10s
    Thread.sleep(15000)
    assert(workerInfos.size == 3)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == 1)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithoutEndpoint.size() == 2)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      mockChangePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }
    logInfo(s"reallocated worker num: ${res.workerResource.keySet().size()}; workerInfo: ${res.workerResource.keySet()}")
    assert(
      lifecycleManager.workerStatusTracker.availableWorkersWithoutEndpoint.size() + lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == 3)

    assert(lifecycleManager.workerSnapshots(shuffleId).size() > 1)
    assert(
      lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() > 1)

    lifecycleManager.stop()
  }

  test("test changePartition without available workers") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager = lifecycleManager.changePartitionManager
    val mockChangePartitionManager = spy(changePartitionManager)
    doNothing.when(mockChangePartitionManager).replySuccess(
      any[Array[PartitionLocation]],
      any[ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]],
      any[Int])
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)

    // workerNum is between [1, 3]
    val workerNum = res.workerResource.keySet().size()
    assert(workerNum >= 1)

    lifecycleManager.setupEndpoints(
      res.workerResource.keySet(),
      shuffleId,
      new ShuffleFailedWorkers())

    val reserveSlotsSuccess = lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    val slots = res.workerResource
    val candidatesWorkers = new util.HashSet(slots.keySet())
    if (reserveSlotsSuccess) {
      val allocatedWorkers =
        JavaUtils.newConcurrentHashMap[WorkerInfo, ShufflePartitionLocationInfo]()
      res.workerResource.asScala.foreach {
        case (workerInfo, (primaryLocations, replicaLocations)) =>
          val partitionLocationInfo = new ShufflePartitionLocationInfo()
          partitionLocationInfo.addPrimaryPartitions(primaryLocations)
          partitionLocationInfo.addReplicaPartitions(replicaLocations)
          allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
      lifecycleManager.workerStatusTracker.updateWorkersWithEndpoint(candidatesWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == workerNum)

    // total workerNum is 1 + 2 + 2 = 5 now
    setUpWorkers(workerConfForAdding, 2)
    // longer than APPLICATION_HEARTBEAT_INTERVAL 10s
    Thread.sleep(15000)
    assert(workerInfos.size == 5)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == workerNum)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithoutEndpoint.size() == 0)

    0 until 10 foreach { partitionId: Int =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      mockChangePartitionManager.handleRequestPartitions(
        shuffleId,
        Array(req),
        lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }
    logInfo(s"reallocated worker num: ${res.workerResource.keySet().size()}; workerInfo: ${res.workerResource.keySet()}")
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == workerNum)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithEndpoint.size() == workerNum)
    assert(lifecycleManager.workerStatusTracker.availableWorkersWithoutEndpoint.size() == 0)

    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
