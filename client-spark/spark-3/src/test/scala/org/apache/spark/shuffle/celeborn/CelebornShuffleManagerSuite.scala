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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.internal.SQLConf
import org.junit
import org.junit.Assert
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

@RunWith(classOf[JUnit4])
class SparkShuffleManagerSuite extends Logging {
  @junit.Test
  def testFallBack(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.useOldFetchProtocol", "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .setAppName("test")
    val sc = new SparkContext(conf)
    // scalastyle:off println
    sc.parallelize(1 to 1000, 2).map { i => (i, Range(1, 100).mkString(",")) }
      .groupByKey(16).count()
    // scalastyle:on println
    sc.stop()
  }

  @junit.Test
  def testClusterNotAvailable(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.useOldFetchProtocol", "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .setAppName("test")
    val sc = new SparkContext(conf)
    // scalastyle:off println
    sc.parallelize(1 to 1000, 2).map { i => (i, Range(1, 100).mkString(",")) }
      .groupByKey(16).count()
    // scalastyle:on println
    sc.stop()
  }

  @junit.Test
  def testChangeWriteModeByPartitionCount(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set("spark.shuffle.useOldFetchProtocol", "true")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_DYNAMIC_WRITE_MODE_ENABLED.key}", "true")
      .set(
        s"spark.${CelebornConf.CLIENT_PUSH_DYNAMIC_WRITE_MODE_PARTITION_NUM_THRESHOLD.key}",
        "15")
      .setAppName("test")
    val sc = new SparkContext(conf)
    // scalastyle:off println
    sc.parallelize(1 to 1000, 10).repartition(20).repartition(10).count()
    // scalastyle:on println
    sc.stop()
  }

  @junit.Test
  def testWrongSparkConfMaxAttemptLimit(): Unit = {
    val conf = new SparkConf().setIfMissing("spark.master", "local")
      .setIfMissing(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", "localhost:9097")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "false")
      .set("spark.shuffle.service.enabled", "false")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

    // default conf, will success
    new SparkShuffleManager(conf, true)

    conf
      .set("spark.stage.maxConsecutiveAttempts", "32768")
      .set("spark.task.maxFailures", "10")
    try {
      new SparkShuffleManager(conf, true)
      Assert.fail()
    } catch {
      case e: IllegalArgumentException =>
        Assert.assertTrue(
          e.getMessage.contains("The spark.stage.maxConsecutiveAttempts should be less than 32768"))
      case _: Throwable =>
        Assert.fail()
    }
  }
}
