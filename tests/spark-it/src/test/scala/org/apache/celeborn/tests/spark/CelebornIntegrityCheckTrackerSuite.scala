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

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker
import org.apache.celeborn.common.CelebornConf

/**
 * Integration tests for the tracker-based integrity enforcement layer:
 * [[org.apache.spark.shuffle.celeborn.ValidatingSparkShuffleManager]] +
 * [[org.apache.spark.shuffle.celeborn.CelebornIntegrityCheckExecutorPlugin]] +
 * [[org.apache.celeborn.client.read.CelebornIntegrityCheckTracker]].
 *
 * These tests complement [[CelebornIntegrityCheckSuite]], which tests the underlying
 * server-side `readReducerPartitionEnd` RPC. Here we test that the client-side tracker
 * correctly enforces that every fully-consumed partition stream registered a validation.
 */
class CelebornIntegrityCheckTrackerSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  private def enableValidatingManager(conf: SparkConf): SparkConf = {
    conf
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key}", "true")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.ValidatingSparkShuffleManager")
      .set(
        "spark.plugins",
        "org.apache.spark.shuffle.celeborn.CelebornIntegrityCheckExecutorPlugin")
  }

  test("join with ValidatingSparkShuffleManager succeeds when data is intact") {
    val sparkConf = new SparkConf().setAppName("celeborn-tracker-test").setMaster("local[2]")
    enableValidatingManager(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (Spark3OrNewer) {
      try {
        import sparkSession.implicits._
        val random = new Random(42)
        val df1 = sparkSession.sparkContext
          .parallelize(1 to 10000, 4)
          .map(i => (random.nextInt(32), i.toString))
          .toDF("key", "val1")
        val df2 = sparkSession.sparkContext
          .parallelize(1 to 100, 4)
          .map(i => (random.nextInt(32), i.toString))
          .toDF("key", "val2")

        val count = df1.join(df2, "key").count()
        assert(count > 0)
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("empty partitions are handled gracefully with ValidatingSparkShuffleManager") {
    val sparkConf = new SparkConf().setAppName("celeborn-tracker-test").setMaster("local[2]")
    enableValidatingManager(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (Spark3OrNewer) {
      try {
        import sparkSession.implicits._
        val result = sparkSession.sparkContext
          .parallelize(1 to 2, 2)
          .toDF("r")
          .repartition(10)
          .mapPartitions(iter => Iterator(iter.map(_.getInt(0)).toSeq))
          .collect()
          .sortBy(_.headOption.getOrElse(Int.MaxValue))

        result shouldBe List(1) :: List(2) :: List.fill(8)(List())
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("limit query does not trigger false integrity check failure") {
    // When Spark closes a stream early (limit), validateIntegrity() is NOT called,
    // so no registerValidation() fires. The ValidatingIterator only calls
    // ensureIntegrityCheck when hasNext() returns false — which never happens for
    // an early-closed stream — so no assertion fires.
    val sparkConf = new SparkConf().setAppName("celeborn-tracker-test").setMaster("local[2]")
    enableValidatingManager(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (Spark3OrNewer) {
      try {
        import sparkSession.implicits._
        val result = sparkSession.sparkContext
          .parallelize(1 to 10000, 1)
          .toDF("r")
          .repartition(2)
          .limit(1)
          .collect()

        result.length shouldBe 1
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("tracker detects a partition stream that skipped registerValidation") {
    // Simulate a bug where CelebornInputStream.validateIntegrity() runs but does not
    // call registerValidation() for one partition. The ValidatingIterator should detect
    // the missing registration when ensureIntegrityCheck fires on iterator exhaustion.
    val sparkConf = new SparkConf().setAppName("celeborn-tracker-test").setMaster("local[2]")
    enableValidatingManager(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (Spark3OrNewer) {
      import sparkSession.implicits._
      val registerValidationCalled = new AtomicBoolean(false)
      CelebornIntegrityCheckTracker.setContextCreator(() =>
        new CelebornIntegrityCheckTracker.CelebornReaderContext {
          override def registerValidation(
              shuffleId: Int,
              startMapper: Int,
              endMapper: Int,
              partition: Int): Unit = {
            // Drop the registration for partition 0 of the first shuffle to simulate the bug.
            if (shuffleId == 0 && startMapper == 0 && endMapper == Integer.MAX_VALUE
              && partition == 0) {
              registerValidationCalled.set(true)
            } else {
              super.registerValidation(shuffleId, startMapper, endMapper, partition)
            }
          }
        })
      try {
        intercept[Exception] {
          sparkSession.sparkContext
            .parallelize(1 to 10000, 1)
            .toDF("r")
            .repartition(10)
            .sort(col("r"))
            .collect()
        }.getMessage should include("Validation is not registered")

        registerValidationCalled.get() shouldBe true
      } finally {
        CelebornIntegrityCheckTracker.setContextCreator(() =>
          new CelebornIntegrityCheckTracker.CelebornReaderContext())
        sparkSession.stop()
      }
    }
  }
}
