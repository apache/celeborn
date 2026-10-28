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

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.message.ControlMessages.MapperMetrics
import org.apache.celeborn.service.deploy.MiniClusterFeature

/**
 * A map attempt that reports MapperEnd but is then failed by Spark (e.g. its executor was
 * preempted after it read truncated input) must not be the attempt reducers read.
 */
class MapperEndConfirmationSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  private val APP = "app-mapper-end-confirmation"
  private val numMappers = 2
  private var masterPort = 0

  override def beforeAll(): Unit = {
    val (m, _) = setupMiniClusterWithRandomPorts()
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    shutdownMiniCluster()
  }

  private def clientConf(confirmationEnabled: Boolean): CelebornConf = {
    new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_MAPPER_END_CONFIRMATION_ENABLED.key, confirmationEnabled.toString)
      .set(CelebornConf.CLIENT_MAPPER_END_CONFIRMATION_INTERVAL.key, "50ms")
  }

  private def push(
      client: ShuffleClientImpl,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      record: String): Unit = {
    val bytes = record.getBytes(StandardCharsets.UTF_8)
    client.pushData(shuffleId, mapId, attemptId, 0, bytes, 0, bytes.length, numMappers, 1)
  }

  private def readAll(client: ShuffleClientImpl, shuffleId: Int): String = {
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }
    val in = client.readPartition(shuffleId, 0, 0, 0L, 0, Integer.MAX_VALUE, metricsCallback)
    val out = new ByteArrayOutputStream()
    var b = in.read()
    while (b != -1) {
      out.write(b)
      b = in.read()
    }
    in.close()
    new String(out.toByteArray, StandardCharsets.UTF_8)
  }

  private def waitUntil(condition: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + 20000
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    assert(condition)
  }

  /**
   * Map 0 attempt 0 pushes truncated data and reports MapperEnd, then "fails" in Spark; map 0
   * attempt 1 pushes the full data. Map 1 succeeds on its first attempt. `registered` plays
   * Spark's MapOutputTracker. Returns what the reducer reads.
   */
  private def runScenario(shuffleId: Int, confirmationEnabled: Boolean): (String, Array[Int]) = {
    val conf = clientConf(confirmationEnabled)
    val lifecycleManager = new LifecycleManager(APP, conf)
    val registered = new ConcurrentHashMap[Integer, Array[Long]]()
    lifecycleManager.registerMapTaskIdResolver(new JFunction[Integer, Array[Long]] {
      override def apply(appShuffleId: Integer): Array[Long] = registered.get(appShuffleId)
    })
    val client = new ShuffleClientImpl(APP, conf, UserIdentifier("mock", "mock"))
    client.setupLifecycleManagerRef(lifecycleManager.self)
    try {
      push(client, shuffleId, mapId = 0, attemptId = 0, "TRUNCATED;")
      client.mapperEnd(shuffleId, 0, 0, numMappers, 1, MapperMetrics(), 100L)

      push(client, shuffleId, mapId = 1, attemptId = 0, "MAP1;")
      client.mapperEnd(shuffleId, 1, 0, numMappers, 1, MapperMetrics(), 101L)
      registered.put(shuffleId, Array(-1L, 101L))

      if (confirmationEnabled) {
        waitUntil(lifecycleManager.commitManager.isMapperEnded(shuffleId, 1))
        // attempt 0 of map 0 was never accepted by Spark, so the stage must stay open
        assert(!lifecycleManager.commitManager.isMapperEnded(shuffleId, 0))
        assert(!lifecycleManager.commitManager.isStageEndOrInProcess(shuffleId))
      }

      // Spark retries map 0
      push(client, shuffleId, mapId = 0, attemptId = 1, "FULL-MAP0;")
      client.mapperEnd(shuffleId, 0, 1, numMappers, 1, MapperMetrics(), 200L)
      registered.put(shuffleId, Array(200L, 101L))

      waitUntil(lifecycleManager.commitManager.isStageEnd(shuffleId))
      val attempts = lifecycleManager.commitManager.getMapperAttempts(shuffleId).clone()
      (readAll(client, shuffleId), attempts)
    } finally {
      client.shutdown()
      lifecycleManager.stop()
      lifecycleManager.rpcEnv.shutdown()
    }
  }

  test("reducer reads the attempt Spark accepted when confirmation is enabled") {
    val (data, attempts) = runScenario(shuffleId = 1, confirmationEnabled = true)
    assert(attempts.toSeq == Seq(1, 0))
    assert(data.contains("FULL-MAP0;"))
    assert(data.contains("MAP1;"))
    assert(!data.contains("TRUNCATED;"))
  }

  test("without confirmation the first MapperEnd wins (the bug being fixed)") {
    val (data, attempts) = runScenario(shuffleId = 2, confirmationEnabled = false)
    assert(attempts.toSeq == Seq(0, 0))
    assert(data.contains("TRUNCATED;"))
    assert(!data.contains("FULL-MAP0;"))
  }
}
