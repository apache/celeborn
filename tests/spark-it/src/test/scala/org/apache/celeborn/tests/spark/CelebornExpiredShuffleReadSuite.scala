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
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import org.apache.spark.{FetchFailed, SparkConf, TaskContext, TaskEndReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.worker.Worker

/**
 * Replays a reader that resolved a celeborn shuffle id before the copy was
 * released by a stage rerun, then asked for its file group only after the
 * failed-shuffle cleaner had removed the copy from the workers.
 */
class CelebornExpiredShuffleReadSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    setupMiniClusterWithRandomPorts(workerNum = 1)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    workerDirs = workerDirs :+ storageDir
    super.createWorker(map ++ Map("celeborn.master.heartbeat.worker.timeout" -> "10s"), storageDir)
  }

  test("a reader holding a released shuffle id fails instead of reading it as empty") {
    if (Spark3OrNewer) {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      val sparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
        .config("spark.celeborn.client.spark.stageRerun.enabled", "true")
        .config("spark.celeborn.client.spark.fetch.cleanFailedShuffle", "true")
        .config("spark.celeborn.client.shuffle.expired.checkInterval", "5s")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
        .getOrCreate()
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)

      val staleTaskFinished = new CountDownLatch(1)
      val staleTaskEnd = new AtomicReference[TaskEndReason]()
      val staleTaskRecordsRead = new AtomicReference[java.lang.Long](-1L)
      val hook = new StaleReaderHook(celebornConf, workerDirs, staleTaskFinished)
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      sparkSession.sparkContext.addSparkListener(new SparkListener {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          if (taskEnd.stageAttemptId == 0 && taskEnd.stageId == hook.reduceStageId.get() &&
            taskEnd.taskInfo.index == StaleReaderHook.StalePartition) {
            staleTaskEnd.set(taskEnd.reason)
            if (taskEnd.taskMetrics != null) {
              staleTaskRecordsRead.set(taskEnd.taskMetrics.shuffleReadMetrics.recordsRead)
            }
            staleTaskFinished.countDown()
          }
        }
      })

      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, i) }.groupByKey(4).collect()

      assert(hook.observedRelease.get(), "stale reader never saw the old copy leave the workers")
      assert(staleTaskFinished.await(60, TimeUnit.SECONDS))
      staleTaskEnd.get() match {
        case FetchFailed(_, _, _, _, _, message) =>
          assert(message.contains(StatusCode.SHUFFLE_EXPIRED.toString), message)
        case other =>
          fail(s"stale reader ended with $other after reading " +
            s"${staleTaskRecordsRead.get()} records from a released shuffle")
      }
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        elem._2.foreach(i => assert(i.equals(elem._1)))
      }
      sparkSession.stop()
    }
  }
}

object StaleReaderHook {
  val StalePartition = 0
}

/**
 * For the first attempt of one reduce partition: resolve the celeborn shuffle id
 * the way the real reader does, delete the shuffle files so every other reader
 * of this copy hits a fetch failure and triggers the rerun, then block until the
 * failed-shuffle cleaner has removed the copy from the worker. The reader then
 * proceeds with the id it resolved before the release and no cached file group,
 * exactly like a task that was stuck on the file-group broadcast in production.
 * The rerun's reader of the
 * same partition is held back until the stale one has finished, so a wrong empty
 * success would be the result Spark records.
 */
class StaleReaderHook(
    conf: CelebornConf,
    workerDirs: Seq[String],
    staleTaskFinished: CountDownLatch)
  extends ShuffleManagerHook {

  val observedRelease = new AtomicBoolean(false)
  val reduceStageId = new AtomicInteger(-1)
  private val armed = new AtomicBoolean(false)

  private def shuffleDirs(appUniqueId: String, celebornShuffleId: Int): Seq[File] =
    workerDirs.map { dir =>
      new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
    }

  override def exec(
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): Unit = {
    if (startPartition != StaleReaderHook.StalePartition) {
      return
    }
    if (context.stageAttemptNumber() != 0) {
      staleTaskFinished.await(60, TimeUnit.SECONDS)
      return
    }
    if (!armed.compareAndSet(false, true)) {
      return
    }
    val h = handle.asInstanceOf[CelebornShuffleHandle[_, _, _]]
    reduceStageId.set(context.stageId())
    val shuffleClient = ShuffleClient.get(
      h.appUniqueId,
      h.lifecycleManagerHost,
      h.lifecycleManagerPort,
      conf,
      h.userIdentifier,
      h.extension)
    val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
    val dirs = shuffleDirs(h.appUniqueId, celebornShuffleId)
    val dataFiles = dirs.filter(_.exists()).flatMap(_.listFiles())
    if (dataFiles.isEmpty) {
      throw new RuntimeException("unexpected, there must be some data file")
    }
    dataFiles.foreach(_.delete())
    val deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(120)
    while (dirs.exists(_.exists()) && System.currentTimeMillis() < deadline) {
      Thread.sleep(500)
    }
    observedRelease.set(!dirs.exists(_.exists()))
    // the executors in production held no usable file group for the released copy
    // (their broadcast fetch had failed), so the reader reloaded it over RPC
    shuffleClient.cleanupShuffle(celebornShuffleId)
  }
}
