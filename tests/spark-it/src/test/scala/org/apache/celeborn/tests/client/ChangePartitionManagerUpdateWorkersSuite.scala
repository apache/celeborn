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

import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.client.{ChangePartitionManager, ChangePartitionRequest, LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.MiniClusterFeature

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

class ChangePartitionManagerUpdateWorkersSuite extends WithShuffleClientSuite with MiniClusterFeature {

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
    .set(CelebornConf.TEST_CLIENT_RETRY_REVIVE.key, "true")
    .set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "3")
    .set(CelebornConf.CLIENT_SLOT_ASSIGN_MAX_WORKERS.key, "1")
    .set(CelebornConf.MASTER_SLOT_ASSIGN_EXTRA_SLOTS.key, "0")
    .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
    .set(CelebornConf.TEST_CLIENT_UPDATE_AVAILABLE_WORKER.key, "true")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val testConf = Map(
      s"${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}" -> "3",
      s"${CelebornConf.CLIENT_SLOT_ASSIGN_MAX_WORKERS.key}" -> "1",
      s"${CelebornConf.MASTER_SLOT_ASSIGN_EXTRA_SLOTS.key}" -> "0")
    val (master, _) = setupMiniClusterWithRandomPorts(testConf,testConf,workerNum = 3)
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("test revive with available workers") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val changePartitionManager: ChangePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 1)

    logInfo(s"RequestSlots worker num: ${res.workerResource.keySet().size()}; workerInfo: ${res.workerResource.keySet()}")

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
      res.workerResource.asScala.foreach { case (workerInfo, (primaryLocations, replicaLocations)) =>
        val partitionLocationInfo = new ShufflePartitionLocationInfo()
        partitionLocationInfo.addPrimaryPartitions(primaryLocations)
        partitionLocationInfo.addReplicaPartitions(replicaLocations)
        allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    }
    assert(lifecycleManager.workerSnapshots(shuffleId).size() == 1)
    lifecycleManager.workerSnapshots(shuffleId).forEach { case (workerInfo, partitionLocationInfo) =>
      logInfo(s"worker: ${workerInfo}; partitionLocationInfo size: ${partitionLocationInfo.getPrimaryPartitions().size()}")
    }
    ids.forEach { partitionId =>
      val req = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        -1,
        null,
        None)
      changePartitionManager.handleRequestPartitions(shuffleId, Array(req), lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
    }

    assert(lifecycleManager.workerSnapshots(shuffleId).size() > 1)

    lifecycleManager.stop()
  }



  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
