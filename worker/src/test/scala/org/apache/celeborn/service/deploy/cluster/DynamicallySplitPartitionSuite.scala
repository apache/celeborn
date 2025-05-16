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

package org.apache.celeborn.service.deploy.cluster

import java.nio.charset.StandardCharsets
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.service.deploy.MiniClusterFeature

class DynamicallySplitPartitionSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {
  var masterEndpoint = ""

  override def beforeAll(): Unit = {
    val conf = Map("celeborn.worker.flusher.buffer.size" -> "0")

    logInfo("test initialized , setup Celeborn mini cluster")
    val (master, _) = setupMiniClusterWithRandomPorts(conf, conf, 5)
    masterEndpoint = master.conf.get(CelebornConf.MASTER_ENDPOINTS.key)
  }

  class PushTask(shuffleClient: ShuffleClientImpl, shuffleId: Int, mapId: Int, mapNum: Int)
    extends Runnable {
    override def run(): Unit = {
      val startTime = System.nanoTime()
      var i = 0
      // 512 KB/s, 4 tasks, 60s
      // expect 10 M soft_split, require a soft split every 5 seconds.
      while (i < 60 * 8) {
        val DATA = RandomStringUtils.random(64 * 1024).getBytes(StandardCharsets.UTF_8)
        shuffleClient.pushData(shuffleId, mapId, 0, 0, DATA, 0, DATA.length, mapNum, 1)
        i += 1
        Thread.sleep(125)
      }
      shuffleClient.mapperEnd(shuffleId, mapId, 0, mapNum)
      val endTime = System.nanoTime()
      logInfo(
        s"PushTask $mapId finished, cost ${TimeUnit.NANOSECONDS.toSeconds(endTime - startTime)} s")
    }
  }

  test("dynamically split partition") {
    val appId = s"dynamically-split-partition-test-${System.currentTimeMillis()}"
    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, masterEndpoint)
      .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key, "10M")
      .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_MODE.key, "HARD")
      .set(CelebornConf.CLIENT_ASYNC_SPLIT_PARTITION_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_ACTIVE_FULL_LOCATION_TIME_WINDOW.key, "10s")
      .set(CelebornConf.CLIENT_ACTIVE_FULL_LOCATION_INTERVAL_PER_BUCKET.key, "1s")
      .set(CelebornConf.CLIENT_EXPECTED_WORKER_SPEED_MB_PER_SECOND.key, "1")
    val lifecycleManager = new LifecycleManager(appId, clientConf)
    val shuffleClient = new ShuffleClientImpl(appId, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    val executor = Executors.newFixedThreadPool(4)
    val task0 = new PushTask(shuffleClient, 0, 0, 4)
    val task1 = new PushTask(shuffleClient, 0, 1, 4)
    val task2 = new PushTask(shuffleClient, 0, 2, 4)
    val task3 = new PushTask(shuffleClient, 0, 3, 4)
    val startTime = System.nanoTime()
    executor.submit(task0)
    executor.submit(task1)
    executor.submit(task2)
    executor.submit(task3)
    executor.shutdown()
    executor.awaitTermination(120, TimeUnit.SECONDS)
    val endTime = System.nanoTime()
    assert(TimeUnit.NANOSECONDS.toSeconds(endTime - startTime) < 100)
  }
}
