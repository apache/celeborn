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

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.client.{LifecycleManager, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerUnregisterShuffleSuite extends WithShuffleClientSuite
  with MiniClusterFeature {

  celebornConf
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val (master, _) = setupMiniClusterWithRandomPorts()
    celebornConf.set(
      CelebornConf.MASTER_ENDPOINTS.key,
      master.conf.get(CelebornConf.MASTER_ENDPOINTS.key))
  }

  test("test unregister shuffle in batch") {
    val conf = celebornConf.clone
    conf.set(CelebornConf.CLIENT_BATCH_REMOVE_EXPIRED_SHUFFLE.key, "true")
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val counts = 10
    val ids =
      new util.ArrayList[Integer]((0 until counts).toList.map(x => Integer.valueOf(x)).asJava)
    val shuffleIds = (1 to counts).toList

    shuffleIds.foreach { shuffleId: Int =>
      val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
      assert(res.status == StatusCode.SUCCESS)
      lifecycleManager.registeredShuffle.add(shuffleId)
      assert(lifecycleManager.registeredShuffle.contains(shuffleId))
      val shuffleKey = Utils.makeShuffleKey(APP, shuffleId)
      assert(masterInfo._1.statusSystem.registeredShuffle.contains(shuffleKey))
      lifecycleManager.commitManager.setStageEnd(shuffleId)
    }

    shuffleIds.foreach { shuffleId: Int =>
      lifecycleManager.unregisterShuffle(shuffleId)
    }
    // after unregister shuffle
    eventually(timeout(120.seconds), interval(2.seconds)) {
      shuffleIds.foreach { shuffleId: Int =>
        val shuffleKey = Utils.makeShuffleKey(APP, shuffleId)
        assert(!lifecycleManager.registeredShuffle.contains(shuffleId))
        assert(!masterInfo._1.statusSystem.registeredShuffle.contains(shuffleKey))
      }
    }

    lifecycleManager.stop()
  }

  test("test unregister shuffle") {
    val conf = celebornConf.clone
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, conf)
    val counts = 10
    val ids =
      new util.ArrayList[Integer]((0 until counts).toList.map(x => Integer.valueOf(x)).asJava)
    val shuffleIds = (1 to counts).toList

    shuffleIds.foreach { shuffleId: Int =>
      val res = lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, ids)
      assert(res.status == StatusCode.SUCCESS)
      lifecycleManager.registeredShuffle.add(shuffleId)
      assert(lifecycleManager.registeredShuffle.contains(shuffleId))
      val shuffleKey = Utils.makeShuffleKey(APP, shuffleId)
      assert(masterInfo._1.statusSystem.registeredShuffle.contains(shuffleKey))
      lifecycleManager.commitManager.setStageEnd(shuffleId)
    }
    val previousTime = System.currentTimeMillis()
    shuffleIds.foreach { shuffleId: Int =>
      lifecycleManager.unregisterShuffle(shuffleId)
    }
    // after unregister shuffle
    eventually(timeout(120.seconds), interval(2.seconds)) {
      shuffleIds.foreach { shuffleId: Int =>
        val shuffleKey = Utils.makeShuffleKey(APP, shuffleId)
        assert(!lifecycleManager.registeredShuffle.contains(shuffleId))
        assert(!masterInfo._1.statusSystem.registeredShuffle.contains(shuffleKey))
      }
    }
    val currentTime = System.currentTimeMillis()
    assert(currentTime - previousTime > conf.shuffleExpiredCheckIntervalMs)
    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
