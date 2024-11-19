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

class GroupMapTaskTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val testConf = Map(
      s"${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}" -> "3",
      s"${CelebornConf.GROUP_MAP_TASK_ENABLED.key}" -> "true",
      s"${CelebornConf.GROUP_MAP_TASK_GROUP_SIZE.key}" -> "2")
    setupMiniClusterWithRandomPorts(testConf, workerNum = 9)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test("celeborn spark integration test - groupMapTask e2e test with retry revive") {
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.TEST_CLIENT_RETRY_REVIVE.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}", "3")
      .set(s"spark.${CelebornConf.GROUP_MAP_TASK_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.GROUP_MAP_TASK_GROUP_SIZE.key}", "2")
      .set(s"spark.${CelebornConf.GROUP_WORKER_ENABLED.key}", "true")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    val value = Range(1, 1000).mkString(",")
    val result = ss.sparkContext.parallelize(1 to 1000, 6)
      .map { i => (i, value) }.groupByKey(4).collect()
    assert(result.length == 1000)
    for (elem <- result) {
      assert(elem._2.mkString(",").equals(value))
    }
    ss.stop()
  }

  test("celeborn spark integration test - groupMapTask e2e test huge data") {
    val sparkConf = new SparkConf()
      .set(s"spark.${CelebornConf.TEST_CLIENT_RETRY_REVIVE.key}", "false")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key}", "3")
      .set(s"spark.${CelebornConf.GROUP_MAP_TASK_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.GROUP_MAP_TASK_GROUP_SIZE.key}", "2")
      .setAppName("celeborn-demo").setMaster("local[2]")
    val ss = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    val value = Range(1, 10000).mkString(",")
    val tuples = ss.sparkContext.parallelize(1 to 10000, 8)
      .map { i => (i, value) }.groupByKey(16).collect()

    // verify result
    assert(tuples.length == 10000)
    for (elem <- tuples) {
      assert(elem._2.mkString(",").equals(value))
    }

    ss.stop()
  }
}
