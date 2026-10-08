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

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

class CelebornQueryEndShuffleCleanupSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    // Always stop the session so that a failed test never leaks its SparkSession
    // (getOrCreate would otherwise reuse it in the next test).
    stopActiveSparkSessions()
  }

  private def lifecycleManager(): LifecycleManager = {
    SparkEnv.get.shuffleManager.asInstanceOf[SparkShuffleManager].getLifecycleManager
  }

  private def awaitShufflesUnregistered(lifecycleManager: LifecycleManager): Unit = {
    val deadline = System.currentTimeMillis() + 60000
    while (!lifecycleManager.registeredShuffle.isEmpty &&
      System.currentTimeMillis() < deadline) {
      Thread.sleep(500)
    }
    assert(
      lifecycleManager.registeredShuffle.isEmpty,
      "Shuffles should be unregistered after the SQL query completes.")
  }

  private def queryEndShuffleCleanupConf(aqeEnabled: Boolean): SparkConf = {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, aqeEnabled.toString)
    sparkConf.set(
      s"spark.${CelebornConf.CLIENT_SPARK_SQL_QUERY_END_SHUFFLE_CLEANUP_ENABLED.key}",
      "true")
    // Speed up the delayed unregister inside LifecycleManager.
    sparkConf.set(s"spark.${CelebornConf.SHUFFLE_EXPIRED_CHECK_INTERVAL.key}", "1s")
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sparkConf
  }

  private val groupByQuery = "SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10"

  private def expectedGroupByResult: Map[Long, Long] = (0L until 10L).map(_ -> 100L).toMap

  Seq(true, false).foreach { aqeEnabled =>
    test(s"CELEBORN-2465: shuffles are unregistered on SQL query completion, " +
      s"aqeEnabled: $aqeEnabled") {
      val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(aqeEnabled))
        .getOrCreate()

      spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
      spark.sql("SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10").collect()
      awaitShufflesUnregistered(lifecycleManager())

      // A subsequent query must not be affected by the cleanup of the previous query.
      val result = spark.sql(
        """
          |SELECT k, SUM(cnt) FROM (
          |  SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10
          |) t GROUP BY k
          |""".stripMargin).collect()
      assert(result.length == 10)
      awaitShufflesUnregistered(lifecycleManager())
    }
  }

  test("CELEBORN-2465: CTAS shuffles are unregistered on SQL query completion, " +
    "aqeEnabled: true") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    try {
      // For CTAS / INSERT the executedPlan root is a DataWritingCommandExec wrapping an
      // AdaptiveSparkPlanExec, so exchanges are only reachable by unwrapping it.
      import org.apache.spark.sql.functions._
      spark.range(0, 1000, 1, 4)
        .withColumn("k", expr("id % 10"))
        .groupBy("k")
        .count()
        .write
        .mode("overwrite")
        .saveAsTable("celeborn_cleanup_ctas_test")
      awaitShufflesUnregistered(lifecycleManager())
    } finally {
      spark.sql("DROP TABLE IF EXISTS celeborn_cleanup_ctas_test")
    }
  }

  test("CELEBORN-2465: all shuffles of a multi-join query are unregistered, aqeEnabled: true") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.range(0, 1000, 1, 4).createOrReplaceTempView("tb")
    // Four shuffle joins in a row, so the final plan holds multiple shuffle query stages, and
    // exchange reuse makes some of them ReusedExchangeExec.
    val result = spark.sql(
      """
        |SELECT COUNT(*) FROM (
        |  SELECT a1.id AS k FROM ta a1
        |  JOIN tb b1 ON a1.id = b1.id
        |  JOIN ta a2 ON a1.id = a2.id
        |  JOIN tb b2 ON a1.id = b2.id
        |  JOIN ta a3 ON a1.id = a3.id
        |)
        |""".stripMargin).collect()
    assert(result.head.getLong(0) == 1000)
    awaitShufflesUnregistered(lifecycleManager())
  }

  test("CELEBORN-2465: shuffles are not unregistered on SQL query completion by default") {
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-test").setMaster("local[2]"),
      ShuffleMode.HASH)
    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.sql("SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10").collect()

    assert(
      !lifecycleManager().registeredShuffle.isEmpty,
      "Shuffles should still be registered when query end shuffle cleanup is disabled.")
  }

  test("CELEBORN-2465: failed query only cleans up actually registered shuffles") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(false)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.udf.register(
      "fail_udf",
      (v: Long) => {
        throw new RuntimeException("intentional failure for test")
        v
      })

    // The query fails in the reduce stage after the shuffle has been written.
    assertThrows[SparkException] {
      spark.sql(
        """
          |SELECT k, fail_udf(cnt) FROM (
          |  SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10
          |) t
          |""".stripMargin).collect()
    }

    // The listener must not break error propagation, and must clean up the materialized
    // shuffle of the failed query without touching non-registered shuffle ids.
    awaitShufflesUnregistered(lifecycleManager())
  }

  test("CELEBORN-2465: failed AQE query only cleans up materialized shuffles") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    spark.udf.register(
      "fail_udf",
      (v: Long) => {
        throw new RuntimeException("intentional failure for test")
        v
      })

    // The query fails in the reduce stage after the shuffle stage has been materialized.
    assertThrows[SparkException] {
      spark.sql(
        """
          |SELECT k, fail_udf(cnt) FROM (
          |  SELECT id % 10 AS k, COUNT(1) AS cnt FROM ta GROUP BY id % 10
          |) t
          |""".stripMargin).collect()
    }

    // The materialized shuffle stage of the failed AQE query must be cleaned up without
    // touching exchanges that were never materialized.
    awaitShufflesUnregistered(lifecycleManager())
  }

  test("CELEBORN-2465: re-executing the same DataFrame sequentially returns correct results") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    val df = spark.sql(groupByQuery)

    // Each re-execution reuses the same ShuffleDependency whose shuffle was unregistered
    // after the previous execution; stage rerun must regenerate the shuffle data.
    (1 to 3).foreach { _ =>
      assert(
        df.collect().map(row => row.getLong(0) -> row.getLong(1)).toMap == expectedGroupByResult)
      awaitShufflesUnregistered(lifecycleManager())
    }
  }

  test("CELEBORN-2465: re-executing the same DataFrame concurrently returns correct results") {
    val spark = SparkSession.builder().config(queryEndShuffleCleanupConf(true)).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    val df = spark.sql(groupByQuery)
    assert(df.collect().length == 10)
    awaitShufflesUnregistered(lifecycleManager())

    // Two threads collect the same DataFrame concurrently; one execution's query-end
    // cleanup may race the other execution's shuffle read, and stage rerun must keep
    // both results correct.
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    val failure = new AtomicReference[Throwable]()
    try {
      val results = (1 to 2).map { _ =>
        Future {
          df.collect().map(row => row.getLong(0) -> row.getLong(1)).toMap
        }
      }
      results.foreach { f =>
        try {
          assert(Await.result(f, Duration("5min")) == expectedGroupByResult)
        } catch {
          case t: Throwable => failure.compareAndSet(null, t)
        }
      }
    } finally {
      executor.shutdownNow()
    }
    if (failure.get() != null) {
      throw failure.get()
    }
    awaitShufflesUnregistered(lifecycleManager())
  }

  test("CELEBORN-2465: query end shuffle cleanup requires stage rerun to be enabled") {
    val sparkConf = queryEndShuffleCleanupConf(false)
    sparkConf.set(s"spark.${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}", "false")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.range(0, 1000, 1, 4).createOrReplaceTempView("ta")
    val t = intercept[Throwable] {
      spark.sql(groupByQuery).collect()
    }
    val messages = Iterator.iterate(Option(t))(_.flatMap(cause => Option(cause.getCause)))
      .takeWhile(_.isDefined)
      .flatten
      .map(_.getMessage)
      .mkString("\n")
    assert(
      messages.contains(CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key),
      s"Expected fail-fast on ${CelebornConf.CLIENT_STAGE_RERUN_ENABLED.key}, got: $messages")
  }
}
