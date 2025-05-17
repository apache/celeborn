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

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ShuffleClientSuite extends WithShuffleClientSuite with MiniClusterFeature {
  private val masterPort = 19097

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (master, _) = setupMiniClusterWithRandomPorts()
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
    celebornConf.set(
      CelebornConf.MASTER_PORT.key,
      master.conf.get(CelebornConf.MASTER_PORT.key))
  }

  test("test register when master not available") {
    val newCelebornConf: CelebornConf = new CelebornConf()
    newCelebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      s"localhost:${celebornConf.get(CelebornConf.MASTER_PORT) + 1}")
    newCelebornConf.set(CelebornConf.MASTER_CLIENT_MAX_RETRIES.key, "0")

    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, newCelebornConf)
    val shuffleClient: ShuffleClientImpl = {
      val client = new ShuffleClientImpl(APP, newCelebornConf, userIdentifier)
      client.setupLifecycleManagerRef(lifecycleManager.self)
      client
    }

    assertThrows[IOException] {
      shuffleClient.registerMapPartitionTask(1, 1, 0, 0, 1)
    }

    lifecycleManager.stop()
  }

  test("is shuffle stage end") {
    prepareService()
    val shuffleId = 0
    val counts = 10
    val ids =
      new util.ArrayList[Integer]((0 until counts).toList.map(x => Integer.valueOf(x)).asJava)
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
    assert(res.status == StatusCode.SUCCESS)
    lifecycleManager.registeredShuffle.add(shuffleId)
    assert(!shuffleClient.isShuffleStageEnd(shuffleId))
    lifecycleManager.commitManager.setStageEnd(shuffleId)
    assert(shuffleClient.isShuffleStageEnd(shuffleId))
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
