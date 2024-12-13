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

package org.apache.celeborn.tests.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

class RetryReviveTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
  }

  override def beforeEach(): Unit = {}

  override def afterEach(): Unit = {
    shutdownMiniCluster()
    System.gc()
  }

  test("celeborn spark integration test - retry revive as configured times") {
    setupMiniClusterWithRandomPorts()
    ShuffleClient.reset()
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.TEST_CLIENT_RETRY_REVIVE.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}", "3")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    val result = ss.sparkContext.parallelize(1 to 1000, 2)
      .map { i => (i, Range(1, 1000).mkString(",")) }.groupByKey(4).collect()
    assert(result.size == 1000)
    ss.stop()
  }

  test(
    "celeborn spark integration test - e2e test retry revive with new allocated workers from RPC") {
    val testConf = Map(
      s"${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}" -> "3",
      s"${CelebornConf.MASTER_SLOT_ASSIGN_EXTRA_SLOTS.key}" -> "0")
    setupMiniClusterWithRandomPorts(testConf)
    ShuffleClient.reset()
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.TEST_CLIENT_RETRY_REVIVE.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}", "3")
      .set(s"spark.${CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_FACTOR.key}", "0")
      .set(s"spark.${CelebornConf.MASTER_SLOT_ASSIGN_EXTRA_SLOTS.key}", "0")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    val result = ss.sparkContext.parallelize(1 to 1000, 2)
      .map { i => (i, Range(1, 1000).mkString(",")) }.groupByKey(4).collect()
    assert(result.size == 1000)
    ss.stop()
  }

  test("celeborn spark integration test - test failed worker") {
    setupMiniClusterWithRandomPorts()
    logInfo("[test] setupCluster time")
    ShuffleClient.reset()
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "false")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    killer()
    val result = ss.sparkContext.parallelize(1 to 10000, 5)
      .map { i => (i, Range(1, 10000).mkString(",")) }.groupByKey(20).collect()
    logInfo("[test] finish work time")
    assert(result.length == 10000)
    val value = Range(1, 10000).mkString(",")
    for (elem <- result) {
      assert(elem._2.mkString(",").equals(value))
    }
    ss.stop()
  }

  test("celeborn spark integration test - test failed worker1") {
    setupMiniClusterWithRandomPorts()
    Thread.sleep(5000)
    logInfo("[test] setupCluster time")
    ShuffleClient.reset()
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "false")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    killer()
    val result = ss.sparkContext.parallelize(1 to 800, 5)
      .flatMap( _ => (1 to 1500).iterator.map(num => num)).repartition(20).count()
    logInfo("[test] finish work time")
    assert(result == 1200000)
    ss.stop()
  }

}
