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

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.scheduler.{TaskSchedulerImpl, TaskSetManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.{PartitionLocation, ShuffleMode}
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.tests.spark.SparkTestBase
import org.apache.celeborn.tests.spark.fetch.failure.ShuffleReaderGetHooks

class SparkUtilsSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test("check if fetch failure task another attempt is running or successful") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHooks(celebornConf, workerDirs)
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      SparkUtils.lastReportedShuffleFetchFailureTaskId = null

      try {
        val sc = sparkSession.sparkContext
        val jobThread = new Thread {
          override def run(): Unit = {
            try {
              val value = Range(1, 10000).mkString(",")
              sc.parallelize(1 to 10000, 2)
                .map { i => (i, value) }
                .groupByKey(10)
                .mapPartitions { iter =>
                  Thread.sleep(3000)
                  iter
                }.collect()
            } catch {
              case _: InterruptedException =>
            }
          }
        }
        jobThread.start()

        val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
        eventually(timeout(30.seconds), interval(0.milliseconds)) {
          assert(hook.executed.get() == true)
          val reportedTaskId = SparkUtils.lastReportedShuffleFetchFailureTaskId
          assert(reportedTaskId != null)
          val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, reportedTaskId)
          assert(taskSetManager != null)
          assert(SparkUtils.getTaskAttempts(taskSetManager, reportedTaskId)._2.size() == 1)
          assert(SparkUtils.shouldReportShuffleFetchFailure(reportedTaskId))
        }

        sparkSession.sparkContext.cancelAllJobs()

        jobThread.interrupt()

        eventually(timeout(3.seconds), interval(100.milliseconds)) {
          assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 0)
        }
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("getTaskSetManager/getTaskAttempts test") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val jobThread = new Thread {
        override def run(): Unit = {
          try {
            sc.parallelize(1 to 100, 2)
              .repartition(1)
              .mapPartitions { iter =>
                Thread.sleep(3000)
                iter
              }.collect()
          } catch {
            case _: InterruptedException =>
          }
        }
      }
      jobThread.start()

      val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
      eventually(timeout(3.seconds), interval(100.milliseconds)) {
        val taskId = 0
        val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, taskId)
        assert(taskSetManager != null)
        assert(SparkUtils.getTaskAttempts(taskSetManager, taskId)._2.size() == 1)
        assert(SparkUtils.shouldReportShuffleFetchFailure(taskId))
        assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 1)
      }

      sparkSession.sparkContext.cancelAllJobs()

      jobThread.interrupt()

      eventually(timeout(3.seconds), interval(100.milliseconds)) {
        assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.size() == 0)
      }
    } finally {
      sparkSession.stop()
    }
  }

  test("shouldReportShuffleFetchFailure returns false when TaskSetManager is zombie") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val jobThread = new Thread {
        override def run(): Unit = {
          try {
            sc.parallelize(1 to 100, 2)
              .repartition(1)
              .mapPartitions { iter =>
                Thread.sleep(10000)
                iter
              }.collect()
          } catch {
            case _: InterruptedException =>
          }
        }
      }
      jobThread.start()

      val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
      eventually(timeout(3.seconds), interval(100.milliseconds)) {
        val taskId: Long = 0
        val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, taskId)
        assert(taskSetManager != null)

        val isZombieField = classOf[TaskSetManager].getDeclaredField("isZombie")
        isZombieField.setAccessible(true)
        isZombieField.setBoolean(taskSetManager, true)

        SparkUtils.reportedStageShuffleFetchFailureTaskIds.clear()
        SparkUtils.lastReportedShuffleFetchFailureTaskId = null

        assert(!SparkUtils.shouldReportShuffleFetchFailure(taskId))
        assert(SparkUtils.reportedStageShuffleFetchFailureTaskIds.isEmpty)
        assert(SparkUtils.lastReportedShuffleFetchFailureTaskId == null)
      }

      sparkSession.sparkContext.cancelAllJobs()
      jobThread.interrupt()
    } finally {
      sparkSession.stop()
    }
  }

  test("getTaskFailureCount") {
    assert(SparkUtils.getTaskFailureCount(null, 0) == -1)

    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      try {
        val sc = sparkSession.sparkContext
        val jobThread = new Thread {
          override def run(): Unit = {
            try {
              sc.parallelize(1 to 100, 2)
                .repartition(1)
                .mapPartitions { iter =>
                  Thread.sleep(3000)
                  iter
                }.collect()
            } catch {
              case _: InterruptedException =>
            }
          }
        }
        jobThread.start()

        val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
        eventually(timeout(3.seconds), interval(100.milliseconds)) {
          val taskId = 0
          val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, taskId)
          assert(taskSetManager != null)
          assert(SparkUtils.getTaskFailureCount(taskSetManager, 0) == 0)
          assert(SparkUtils.getTaskFailureCount(taskSetManager, -1) == -1)
          assert(SparkUtils.getTaskFailureCount(taskSetManager, Int.MaxValue) == -1)
        }

        sparkSession.sparkContext.cancelAllJobs()
        jobThread.interrupt()
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("getTaskFailureCount after real task failures") {
    if (Spark3OrNewer) {
      // local[1,4]: 1 core (sequential execution), max 4 task failures before stage abort
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[1,4]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      try {
        val sc = sparkSession.sparkContext
        SparkUtilsSuite.survivingTaskAttemptId.set(-1)

        val jobThread = new Thread {
          override def run(): Unit = {
            try {
              sc.parallelize(1 to 10, 1).mapPartitions { iter =>
                val ctx = TaskContext.get()
                if (ctx.attemptNumber() < 2) {
                  throw new RuntimeException("Simulated task failure")
                }
                SparkUtilsSuite.survivingTaskAttemptId.set(ctx.taskAttemptId())
                Thread.sleep(10000)
                iter
              }.collect()
            } catch {
              case _: Exception =>
            }
          }
        }
        jobThread.start()

        val taskScheduler = sc.taskScheduler.asInstanceOf[TaskSchedulerImpl]
        eventually(timeout(10.seconds), interval(100.milliseconds)) {
          val runningTaskId = SparkUtilsSuite.survivingTaskAttemptId.get()
          assert(runningTaskId >= 0)
          val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, runningTaskId)
          assert(taskSetManager != null)
          assert(SparkUtils.getTaskFailureCount(taskSetManager, 0) == 2)
        }

        sparkSession.sparkContext.cancelAllJobs()
        jobThread.interrupt()
      } finally {
        sparkSession.stop()
      }
    }
  }

  test("serialize/deserialize GetReducerFileGroupResponse with broadcast") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    try {
      val shuffleId = Integer.MAX_VALUE
      val getReducerFileGroupResponse = GetReducerFileGroupResponse(
        StatusCode.SUCCESS,
        Map(Integer.valueOf(shuffleId) -> Set(new PartitionLocation(
          0,
          1,
          "localhost",
          1,
          2,
          3,
          4,
          PartitionLocation.Mode.REPLICA)).asJava).asJava,
        Array(1),
        Set(Integer.valueOf(shuffleId)).asJava)

      val serializedBytes =
        SparkUtils.serializeGetReducerFileGroupResponse(shuffleId, getReducerFileGroupResponse)
      assert(serializedBytes != null && serializedBytes.length > 0)
      val broadcast = SparkUtils.getReducerFileGroupResponseBroadcasts.values().asScala.head._1
      assert(broadcast.isValid)

      val deserializedGetReducerFileGroupResponse =
        SparkUtils.deserializeGetReducerFileGroupResponse(shuffleId, serializedBytes)
      assert(deserializedGetReducerFileGroupResponse.status == getReducerFileGroupResponse.status)
      assert(
        deserializedGetReducerFileGroupResponse.fileGroup == getReducerFileGroupResponse.fileGroup)
      assert(java.util.Arrays.equals(
        deserializedGetReducerFileGroupResponse.attempts,
        getReducerFileGroupResponse.attempts))
      assert(deserializedGetReducerFileGroupResponse.partitionIds == getReducerFileGroupResponse.partitionIds)
      assert(
        deserializedGetReducerFileGroupResponse.pushFailedBatches == getReducerFileGroupResponse.pushFailedBatches)

      assert(!SparkUtils.getReducerFileGroupResponseBroadcasts.isEmpty)
      SparkUtils.invalidateSerializedGetReducerFileGroupResponse(shuffleId)
      assert(SparkUtils.getReducerFileGroupResponseBroadcasts.isEmpty)
      assert(!broadcast.isValid)
    } finally {
      sparkSession.stop()
      SparkUtils.getReducerFileGroupResponseBroadcasts.clear()
      SparkUtils.getReducerFileGroupResponseBroadcastNum.set(0)
    }
  }
}

object SparkUtilsSuite {
  val survivingTaskAttemptId = new java.util.concurrent.atomic.AtomicLong(-1)
}
