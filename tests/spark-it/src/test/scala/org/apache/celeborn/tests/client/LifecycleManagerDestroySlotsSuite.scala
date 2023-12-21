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

import scala.collection.JavaConverters._

import org.apache.celeborn.client.{LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerDestroySlotsSuite extends WithShuffleClientSuite with MiniClusterFeature {
  private val masterPort = 19097

  celebornConf.set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> masterPort.toString)
    val workerConf = Map(
      "celeborn.master.endpoints" -> s"localhost:$masterPort")
    setUpMiniCluster(masterConf, workerConf)
  }

  test("test destroy workers without mocking failure") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.TEST_CLIENT_MOCK_DESTROY_SLOTS_FAILURE.key, "false")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    lifecycleManager.setupEndpoints(res.workerResource, shuffleId, new ShuffleFailedWorkers())

    lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    val slotsToDestroy = new WorkerResource
    val destroyWorkers = workerInfos.keySet.take(2)
    destroyWorkers.foreach { worker =>
      val entry = res.workerResource.entrySet().asScala.filter(_.getKey == worker.workerInfo).head
      slotsToDestroy.put(entry.getKey, entry.getValue)
    }
    lifecycleManager.destroySlotsWithRetry(shuffleId, slotsToDestroy)

    destroyWorkers.foreach { worker =>
      assert(worker.controller.partitionLocationInfo.emptyShuffle(Utils.makeShuffleKey(
        APP,
        shuffleId)))
    }

    lifecycleManager.stop()
  }

  test("test destroy workers with mocking failure") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.TEST_CLIENT_MOCK_DESTROY_SLOTS_FAILURE.key, "true")
      .set(CelebornConf.CLIENT_RPC_MAX_RETIRES.key, "5")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    lifecycleManager.setupEndpoints(res.workerResource, shuffleId, new ShuffleFailedWorkers())

    lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    val slotsToDestroy = new WorkerResource
    val destroyWorkers = workerInfos.keySet.take(2)
    destroyWorkers.foreach { worker =>
      val entry = res.workerResource.entrySet().asScala.filter(_.getKey == worker.workerInfo).head
      slotsToDestroy.put(entry.getKey, entry.getValue)
    }

    lifecycleManager.destroySlotsWithRetry(shuffleId, slotsToDestroy)

    destroyWorkers.foreach { worker =>
      assert(worker.controller.partitionLocationInfo.emptyShuffle(Utils.makeShuffleKey(
        APP,
        shuffleId)))
    }

    lifecycleManager.stop()
  }

  test("test destroy workers with one worker down") {
    val shuffleId = nextShuffleId
    val conf = celebornConf.clone
    conf.set(CelebornConf.TEST_CLIENT_MOCK_DESTROY_SLOTS_FAILURE.key, "false")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val ids = new util.ArrayList[Integer](10)
    0 until 10 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    lifecycleManager.setupEndpoints(res.workerResource, shuffleId, new ShuffleFailedWorkers())

    lifecycleManager.reserveSlotsWithRetry(
      shuffleId,
      new util.HashSet(res.workerResource.keySet()),
      res.workerResource,
      updateEpoch = false)

    val slotsToDestroy = new WorkerResource
    val destroyWorkers = workerInfos.keySet.take(2)
    destroyWorkers.foreach { worker =>
      val entry = res.workerResource.entrySet().asScala.filter(_.getKey == worker.workerInfo).head
      slotsToDestroy.put(entry.getKey, entry.getValue)
    }

    destroyWorkers.head.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    destroyWorkers.head.rpcEnv.shutdown()

    lifecycleManager.destroySlotsWithRetry(shuffleId, slotsToDestroy)

    assert(!destroyWorkers.head.controller.partitionLocationInfo.emptyShuffle(Utils.makeShuffleKey(
      APP,
      shuffleId)))
    assert(destroyWorkers.last.controller.partitionLocationInfo.emptyShuffle(Utils.makeShuffleKey(
      APP,
      shuffleId)))

    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
