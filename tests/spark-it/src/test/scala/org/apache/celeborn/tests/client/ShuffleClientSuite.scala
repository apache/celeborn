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

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{MessageType, PartitionLocation, PbOpenStream}
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ShuffleClientSuite extends WithShuffleClientSuite with MiniClusterFeature {
  private val masterPort = 19097

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (master, worker) = setupMiniClusterWithRandomPorts()
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
    celebornConf.set(
      CelebornConf.MASTER_PORT.key,
      master.conf.get(CelebornConf.MASTER_PORT.key))
    celebornConf.set(
      CelebornConf.WORKER_FETCH_PORT.key,
      worker.toList.head.workerInfo.fetchPort.toString)
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

  test("[Celeborn-1445] Celeborn client open stream with unregister shuffle key") {
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)
    val shuffleClient: ShuffleClientImpl = {
      val client = new ShuffleClientImpl(APP, celebornConf, userIdentifier)
      client.setupLifecycleManagerRef(lifecycleManager.self)
      client
    }

    val client = shuffleClient.getDataClientFactory.createClient(
      "localhost",
      celebornConf.get(CelebornConf.WORKER_FETCH_PORT))
    val msg = new TransportMessage(
      MessageType.OPEN_STREAM,
      PbOpenStream.newBuilder()
        .setShuffleKey("999999")
        .setFileName("unimportant")
        .setStartIndex(0)
        .setEndIndex(1)
        .build()
        .toByteArray);
    val exception: IOException = intercept[IOException] {
      client.sendRpcSync(msg.toByteBuffer, 1000 * 60 * 10)
    }
    assert(exception.getCause.getMessage.contains("PartitionUnRetryAbleException"))
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
