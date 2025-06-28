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

import org.apache.spark._
import org.apache.spark.shuffle.celeborn.{SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.{CompressionCodec, ShuffleMode}

class CelebornIntegrityCheckSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  private val logger = LoggerFactory.getLogger(classOf[CelebornIntegrityCheckSuite])

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test("celeborn spark integration test - corrupted data, no integrity check - app succeeds but data is different") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .config(
          s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}",
          CompressionCodec.NONE.toString)
        .getOrCreate()

      // Introduce Data Corruption in single bit in 1 partition location file
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHookForCorruptedData(celebornConf, workerDirs)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val value = Range(1, 10000).mkString(",")
      val tuples = sparkSession.sparkContext.parallelize(1 to 1000, 2)
        .map { i => (i, value) }.groupByKey(16).collect()

      assert(tuples.length == 1000)

      try {
        for (elem <- tuples) {
          assert(elem._2.mkString(",").equals(value))
        }
      } catch {
        case e: Throwable =>
          e.getMessage.contains("elem._2.mkString(\",\").equals(value) was false")
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - corrupted data, integrity checks enabled, no stage rerun - app fails") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config(
          s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}",
          CompressionCodec.NONE.toString)
        .config(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "false")
        .config("spark.celeborn.client.shuffle.integrityCheck.enabled", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      // Introduce Data Corruption in single bit in 1 partition location file
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHookForCorruptedData(celebornConf, workerDirs)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      try {
        val value = Range(1, 10000).mkString(",")
        val tuples = sparkSession.sparkContext.parallelize(1 to 1000, 2)
          .map { i => (i, value) }.groupByKey(16).collect()
        fail("App should abort prior to this step and throw an exception")
      } catch {
        // verify that the app fails
        case e: Throwable => {
          logger.error("Expected exception, logging the full exception", e)
          assert(e.getMessage.contains("Job aborted"))
          assert(e.getMessage.contains("CommitMetadata mismatch"))
        }
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - corrupted data, integrity checks enabled, stage rerun enabled - app succeeds") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config(
          s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}",
          CompressionCodec.NONE.toString)
        .config(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "true")
        .config("spark.celeborn.client.shuffle.integrityCheck.enabled", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      // Introduce Data Corruption in single bit in 1 partition location file
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHookForCorruptedData(celebornConf, workerDirs)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      try {
        val value = Range(1, 10000).mkString(",")
        val tuples = sparkSession.sparkContext.parallelize(1 to 1000, 2)
          .map { i => (i, value) }.groupByKey(16).collect()

        // verify result
        assert(tuples.length == 1000)
        for (elem <- tuples) {
          assert(elem._2.mkString(",").equals(value))
        }
      } finally {
        sparkSession.stop()
      }
    }
  }
}
