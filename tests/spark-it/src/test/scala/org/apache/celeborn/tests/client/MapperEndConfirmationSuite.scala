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
import java.util.function.BiFunction

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.ShuffleMode
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

  private def registerResolver(
      lifecycleManager: LifecycleManager,
      registered: ConcurrentHashMap[Integer, Array[Long]]): Unit = {
    lifecycleManager.registerMapTaskIdResolver(new BiFunction[Integer, Array[Int], Array[Long]] {
      override def apply(appShuffleId: Integer, mapIndexes: Array[Int]): Array[Long] = {
        Option(registered.get(appShuffleId)).map(ids => mapIndexes.map(ids(_))).orNull
      }
    })
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
    try {
      var b = in.read()
      while (b != -1) {
        out.write(b)
        b = in.read()
      }
    } finally {
      in.close()
    }
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
    registerResolver(lifecycleManager, registered)
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

  test("reader shuffle ID lookup confirms registered attempts and retries a pending result") {
    val appShuffleId = 3
    val conf = clientConf(confirmationEnabled = true)
      .set(CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_MAPPER_END_CONFIRMATION_INTERVAL.key, "1h")
    val lifecycleManager = new LifecycleManager(APP, conf)
    val registered = new ConcurrentHashMap[Integer, Array[Long]]()
    registerResolver(lifecycleManager, registered)
    val client = new ShuffleClientImpl(APP, conf, UserIdentifier("mock", "mock"))
    client.setupLifecycleManagerRef(lifecycleManager.self)
    try {
      val (shuffleId, writeSuccess) = client.getShuffleId(appShuffleId, "writer", true, false)
      assert(writeSuccess)
      (0 until numMappers).foreach { mapId =>
        push(client, shuffleId, mapId, 0, s"MAP$mapId;")
        client.mapperEnd(shuffleId, mapId, 0, numMappers, 1, MapperMetrics(), 100L + mapId)
      }
      assert(!client.getShuffleId(appShuffleId, "reader", false, false)._2)
      registered.put(appShuffleId, Array(100L, 101L))
      assert(!lifecycleManager.commitManager.areAllMapperAttemptsFinished(shuffleId))
      assert(client.getShuffleId(appShuffleId, "reader", false, false) == ((shuffleId, true)))
      val data = readAll(client, shuffleId)
      assert(data.contains("MAP0;") && data.contains("MAP1;"))
    } finally {
      client.shutdown()
      lifecycleManager.stop()
      lifecycleManager.rpcEnv.shutdown()
    }
  }

  for (mode <- Seq(ShuffleMode.HASH, ShuffleMode.SORT)) {
    test(s"$mode writer confirms Spark map statuses with the old fetch protocol") {
      val conf = new SparkConf(false)
        .setAppName(s"mapper-end-confirmation-$mode")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
        .set("spark.shuffle.useOldFetchProtocol", "true")
        .set("spark.stage.maxConsecutiveAttempts", "1")
        .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", s"localhost:$masterPort")
        .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
        .set(s"spark.${CelebornConf.SPARK_SHUFFLE_FALLBACK_POLICY.key}", "NEVER")
        .set(s"spark.${CelebornConf.SPARK_SHUFFLE_WRITER_MODE.key}", mode.toString)
        .set(s"spark.${CelebornConf.CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED.key}", "false")
        .set(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "true")
        .set(s"spark.${CelebornConf.CLIENT_MAPPER_END_CONFIRMATION_ENABLED.key}", "true")
        .set(s"spark.${CelebornConf.CLIENT_MAPPER_END_CONFIRMATION_INTERVAL.key}", "1h")
      ShuffleClient.reset()
      val sc = new SparkContext(conf)
      try {
        // Make shuffle map task attempt IDs differ from their partition indexes.
        assert(sc.parallelize(Seq(1, 2), 2).count() == 2L)
        val input = 0 until 100
        val totals = sc.parallelize(input, 4).map(n => (n % 4, n)).reduceByKey(_ + _, 2)
        assert(totals.collect().toMap == input.groupBy(_ % 4).map { case (k, ns) => k -> ns.sum })
      } finally {
        sc.stop()
        ShuffleClient.reset()
      }
    }
  }
}
