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

import org.apache.spark.SparkConf
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
import org.apache.celeborn.reflect.{DynFields, DynMethods}
import org.apache.celeborn.tests.spark.SparkTestBase

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
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderFetchFailureGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

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
          val reportedTaskId =
            SparkUtils.reportedStageShuffleFetchFailureTaskIds.values().asScala.flatMap(
              _.asScala).head
          val taskSetManager = SparkUtils.getTaskSetManager(taskScheduler, reportedTaskId)
          assert(taskSetManager != null)
          assert(SparkUtils.getTaskAttempts(taskSetManager, reportedTaskId)._2.size() == 1)
          assert(!SparkUtils.taskAnotherAttemptRunningOrSuccessful(reportedTaskId))
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
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
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
        assert(!SparkUtils.taskAnotherAttemptRunningOrSuccessful(taskId))
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

<<<<<<< HEAD
  test("serialize/deserialize GetReducerFileGroupResponse with broadcast") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
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

  test("is stage inDeterminate") {
=======
  test("is stage inDeterminate and killTaskSetManagerRunningTasks") {
>>>>>>> b3ceb100d (test name)
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
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

        val getStage_METHOD = DynMethods.builder("getStage")
          .hiddenImpl(classOf[SparkUtils], classOf[Int])
          .orNoop()
          .build()
        val isIndeterminate_METHOD = DynFields.builder()
          .hiddenImpl(classOf[SparkUtils], "isIndeterminate_METHOD")
          .defaultAlwaysNull()
          .buildStatic()
          .get()
          .asInstanceOf[DynMethods.UnboundMethod]
        val isStageIndeterminate_METHOD = DynMethods.builder("isStageIndeterminate")
          .impl(classOf[SparkUtils], classOf[Int])
          .orNoop()
          .build()
        val getTaskSetManagerByStageId_METHOD = DynMethods.builder("getTaskSetManager")
          .impl(classOf[SparkUtils], classOf[Int], classOf[Int])
          .orNoop()
          .build()
        val killTaskSetManagerRunningTasks_METHOD =
          DynMethods.builder("killTaskSetManagerRunningTasks")
            .hiddenImpl(classOf[SparkUtils], classOf[TaskSetManager], classOf[String])
            .orNoop()
            .build()

        assert(!getStage_METHOD.isNoop)
        assert(!isIndeterminate_METHOD.isNoop)
        assert(!isStageIndeterminate_METHOD.isNoop)
        assert(!getTaskSetManagerByStageId_METHOD.isNoop)
        assert(!killTaskSetManagerRunningTasks_METHOD.isNoop)

        eventually(timeout(3.seconds), interval(100.milliseconds)) {
          val stage = getStage_METHOD.asStatic().invoke(Integer.valueOf(0)).asInstanceOf[
            org.apache.spark.scheduler.ShuffleMapStage]
          assert(stage != null)
          assert(!isIndeterminate_METHOD.invoke(stage).asInstanceOf[Boolean])
          val taskSetManager: TaskSetManager = getTaskSetManagerByStageId_METHOD.asStatic().invoke(
            Integer.valueOf(0),
            Integer.valueOf(0))
          assert(taskSetManager != null)
          assert(killTaskSetManagerRunningTasks_METHOD.asStatic.invoke(
            taskSetManager,
            "test").asInstanceOf[Int] > 0)
        }

        sparkSession.sparkContext.cancelAllJobs()

        jobThread.interrupt()
      } finally {
        sparkSession.stop()
      }
    }
  }
}
