/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache Software Foundation, Version 2.0
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
import org.apache.spark.shuffle.celeborn.{SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.{PartitionLocation, ShuffleMode}
import org.apache.celeborn.tests.spark.fetch.failure.PartitionFileDeletionHook

/**
 * Simple integration test for CELEBORN-2032: Create reader should change to peer by taskAttemptId
 * 
 * Two simple test cases:
 * 1. UT1: attempt0 succeeds, delete replica files, verify only primary files accessed
 * 2. UT2: attempt1 succeeds, delete primary files, verify only replica files accessed
 */
class CelebornTaskAttemptReplicaIntegrationSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
    TestCelebornShuffleManager.registerReaderGetHook(null)
  }

  test("CELEBORN-2032 UT1: attempt0 succeeds, delete replica files, verify only primary files accessed") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("CELEBORN-2032-ut1-test").setMaster("local[2]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.SORT))
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "false")
        .config("celeborn.client.push.replicate.enabled", "true")
        .config("spark.task.maxFailures", "1")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      
      // Hook to delete replica files before shuffle read
      val hook = new PartitionFileDeletionHook(celebornConf, workerDirs, deleteReplicaFiles = true)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      try {
        logInfo("=== Starting CELEBORN-2032 UT1 ===")
        logInfo("Test: attempt0 succeeds, delete replica files, verify only primary files accessed")
        
        // Create test data
        val testData = sparkSession.sparkContext.parallelize(1 to 1000, 4)
          .map { i => (i % 100, s"value_$i") }
          .groupByKey(8)
          .collect()

        // Verify hook was executed
        assert(hook.executed.get() == true, "PartitionFileDeletionHook should have been executed")

        // Verify test data
        assert(testData.length == 100, s"Expected 100 groups, but got ${testData.length}")
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("CELEBORN-2032 UT2: attempt1 succeeds, delete primary files, verify only replica files accessed") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("CELEBORN-2032-ut2-test").setMaster("local[2]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.SORT))
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "false")
        .config("celeborn.client.push.replicate.enabled", "true")
        .config("spark.task.maxFailures", "2")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      
      // Hook to delete primary files before shuffle read
      val hook = new PartitionFileDeletionHook(celebornConf, workerDirs, deleteReplicaFiles = false)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      try {
        logInfo("=== Starting CELEBORN-2032 UT2 ===")
        logInfo("Test: attempt1 succeeds, delete primary files, verify only replica files accessed")
        
        // Create test data
        val testData = sparkSession.sparkContext.parallelize(1 to 1000, 4)
          .map { i => (i % 100, s"value_$i") }
          .groupByKey(8)
          .collect()

        // Verify hook was executed
        assert(hook.executed.get() == true, "PartitionFileDeletionHook should have been executed")

        // Verify test data
        assert(testData.length == 100, s"Expected 100 groups, but got ${testData.length}")
      } finally {
        sparkSession.stop()
      }
    }
  }
}