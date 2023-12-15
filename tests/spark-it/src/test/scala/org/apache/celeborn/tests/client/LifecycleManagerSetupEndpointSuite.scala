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
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.CelebornExitKind
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerSetupEndpointSuite extends WithShuffleClientSuite with MiniClusterFeature {
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

  test("test setup endpoints with all workers good") {
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)
    val ids = new util.ArrayList[Integer](100)
    0 until 100 foreach { ids.add(_) }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(0, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    val connectFailedWorkers = new ShuffleFailedWorkers()
    lifecycleManager.setupEndpoints(res.workerResource, 0, connectFailedWorkers)
    assert(connectFailedWorkers.isEmpty)

    lifecycleManager.stop()
  }

  test("test setup endpoints with one worker down") {
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)
    val ids = new util.ArrayList[Integer](100)
    0 until 100 foreach {
      ids.add(_)
    }
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(0, ids)
    assert(res.status == StatusCode.SUCCESS)
    assert(res.workerResource.keySet().size() == 3)

    val firstWorker = workerInfos.keySet.head
    firstWorker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    firstWorker.rpcEnv.shutdown()

    val connectFailedWorkers = new ShuffleFailedWorkers()
    lifecycleManager.setupEndpoints(res.workerResource, 0, connectFailedWorkers)
    assert(connectFailedWorkers.size() == 1)
    assert(connectFailedWorkers.keySet().asScala.head == firstWorker.workerInfo)

    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
