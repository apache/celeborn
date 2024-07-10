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

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.DurationInt

import org.apache.spark.{BarrierTaskContext, ShuffleDependency, SparkConf, SparkContextHelper, TaskContext, TaskKilledException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskStart}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ExecutorShuffleIdTracker, ShuffleManagerHook, SparkShuffleManager, SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.Worker

class CelebornFetchFailureSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach
  with Eventually {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  var workerDirs: Seq[String] = Seq.empty

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    this.synchronized {
      workerDirs = workerDirs :+ storageDir
    }
    super.createWorker(map, storageDir)
  }

  class ShuffleReaderGetHook(conf: CelebornConf) extends ShuffleManagerHook {
    var executed: AtomicBoolean = new AtomicBoolean(false)
    val lock = new Object

    override def exec(
        handle: ShuffleHandle,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext): Unit = {
      if (executed.get() == true) return

      lock.synchronized {
        handle match {
          case h: CelebornShuffleHandle[_, _, _] => {
            val appUniqueId = h.appUniqueId
            val shuffleClient = ShuffleClient.get(
              h.appUniqueId,
              h.lifecycleManagerHost,
              h.lifecycleManagerPort,
              conf,
              h.userIdentifier,
              h.extension)
            val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
            val allFiles = workerDirs.map(dir => {
              new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
            })
            val datafile = allFiles.filter(_.exists())
              .flatMap(_.listFiles().iterator).headOption
            datafile match {
              case Some(file) => file.delete()
              case None => throw new RuntimeException("unexpected, there must be some data file" +
                  s" under ${workerDirs.mkString(",")}")
            }
          }
          case _ => throw new RuntimeException("unexpected, only support RssShuffleHandle here")
        }
        executed.set(true)
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val value = Range(1, 10000).mkString(",")
      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, value) }.groupByKey(16).collect()

      // verify result
      assert(hook.executed.get() == true)
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        assert(elem._2.mkString(",").equals(value))
      }

      val shuffleMgr = SparkContextHelper.env
        .shuffleManager
        .asInstanceOf[TestCelebornShuffleManager]
      val lifecycleManager = shuffleMgr.getLifecycleManager

      shuffleMgr.unregisterShuffle(0)
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(1))

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - unregister shuffle with throwsFetchFailure disabled") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "false")
        .getOrCreate()

      val value = Range(1, 10000).mkString(",")
      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, value) }.groupByKey(16).collect()

      // verify result
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        assert(elem._2.mkString(",").equals(value))
      }

      val shuffleMgr = SparkContextHelper.env
        .shuffleManager
        .asInstanceOf[SparkShuffleManager]
      val lifecycleManager = shuffleMgr.getLifecycleManager

      shuffleMgr.unregisterShuffle(0)
      assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with multiple shuffle data") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      import sparkSession.implicits._

      val df1 = Seq((1, "a"), (2, "b")).toDF("id", "data").groupBy("id").count()
      val df2 = Seq((2, "c"), (2, "d")).toDF("id", "data").groupBy("id").count()
      val tuples = df1.hint("merge").join(df2, "id").select("*").collect()

      // verify result
      assert(hook.executed.get() == true)
      val expect = "[2,1,2]"
      assert(tuples.head.toString().equals(expect))
      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with RDD reuse") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val sc = sparkSession.sparkContext
      val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
      val rdd2 = sc.parallelize(0 until 10000, 2).map(v => (v, v)).groupByKey()
      val rdd3 = rdd1.map(v => (v._2, v._1))

      hook.executed.set(true)

      rdd1.count()
      rdd2.count()

      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()
      hook.executed.set(false)
      rdd3.count()

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - Fetch Failure with read write shuffles in one stage") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHook(celebornConf)
      TestCelebornShuffleManager.registerReaderGetHook(hook)

      val sc = sparkSession.sparkContext
      val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
      val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()

      hook.executed.set(true)
      rdd1.count()

      hook.executed.set(false)
      rdd2.count()

      sparkSession.stop()
    }
  }

  test("celeborn spark integration test - empty shuffle data") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.shuffle.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
        .getOrCreate()

      sparkSession.sql("create table if not exists t_1 (a bigint) using parquet")
      sparkSession.sql("create table if not exists t_2 (a bigint) using parquet")
      sparkSession.sql("create table if not exists t_3 (a bigint) using parquet")
      val df1 = sparkSession.table("t_1")
      val df2 = sparkSession.table("t_2")
      val df3 = sparkSession.table("t_3")
      df1.union(df2).union(df3).count()

      sparkSession.stop()
    }
  }

  test(
    "celeborn spark integration test - resubmit a barrier stage and do not reuse the shuffle id") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.shuffle.enabled", "true")
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    val tracker = new ExecutorShuffleIdTracker {
      override def unregisterAppShuffleId(shuffleClient: ShuffleClient, appShuffleId: Int): Unit = {
        // do not clean up shuffle ids for this test
      }
    }
    TestCelebornShuffleManager.staticRegisterExecutorShuffleIdTracker(tracker)

    val sc = sparkSession.sparkContext
    val listener = new SparkListener {
      override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        val stageId = taskStart.stageId
        val stageAttemptId = taskStart.stageAttemptId
        val partitionId = taskStart.taskInfo.index
        if (stageId == 1 && stageAttemptId == 0 && partitionId == 0) {
          new Thread {
            override def run: Unit = {
              eventually(timeout(10.seconds)) {
                sc.killTaskAttempt(taskStart.taskInfo.taskId, interruptThread = true)
              }
            }
          }.start()
        }
      }
    }
    sc.addSparkListener(listener)
    // stage 1 is a barrier stage
    val rdd1 =
      sc.parallelize(0 until 10000, 2).map(v => (v, v)).groupByKey().barrier().mapPartitions {
        it =>
          val context = BarrierTaskContext.get()
          try {
            Thread.sleep(5000)
            context.barrier()
          } catch {
            case _: TaskKilledException =>
              throw new RuntimeException("failed")
            case e: Throwable =>
              throw e
          }
          it
      }
    val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()
    rdd2.count()
    var hasShuffleDependency = false
    rdd2.dependencies.foreach {
      case dependency: ShuffleDependency[_, _, _] =>
        val appShuffleId = dependency.shuffleId
        // shuffle id is not expected to be reused, so size is 2
        hasShuffleDependency = true
        assert(tracker.getShuffleIdSet(appShuffleId).size() == 2)
      case _ =>
      // do nothing
    }
    assert(hasShuffleDependency, "no shuffle dependency")
    sparkSession.stop()
  }
}
