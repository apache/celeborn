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

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.celeborn.{SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.tests.spark.fetch.failure.ShuffleReaderGetHooks

class CelebornFetchFailureDiskCleanSuite extends AnyFunSuite
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

  test("celeborn spark integration test - the failed shuffle file is cleaned up correctly") {
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
        .config("spark.celeborn.client.spark.fetch.cleanFailedShuffle", "true")
        .getOrCreate()

      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new ShuffleReaderGetHooks(
        celebornConf,
        workerDirs,
        shuffleIdToBeDeleted = Seq(0))
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      val checkingThread =
        triggerStorageCheckThread(Seq(0), Seq(1), sparkSession)
      val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
        .map { i => (i, i) }.groupByKey(4).collect()
      checkStorageValidation(checkingThread)
      // verify result
      assert(hook.executed.get())
      assert(tuples.length == 10000)
      for (elem <- tuples) {
        elem._2.foreach(i => assert(i.equals(elem._1)))
      }
      sparkSession.stop()
    }
  }

  class CheckingThread(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession)
    extends Thread {
    var exception: Exception = _

    protected def checkDirStatus(): Boolean = {
      val deletedSuccessfully = shuffleIdShouldNotExist.forall(shuffleId => {
        workerDirs.forall(dir =>
          !new File(s"$dir/celeborn-worker/shuffle_data/" +
            s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists())
      })
      val deletedSuccessfullyString = shuffleIdShouldNotExist.map(shuffleId => {
        shuffleId.toString + ":" +
          workerDirs.map(dir =>
            !new File(s"$dir/celeborn-worker/shuffle_data/" +
              s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists()).toList
      }).mkString(",")
      val createdSuccessfully = shuffleIdMustExist.forall(shuffleId => {
        workerDirs.exists(dir =>
          new File(s"$dir/celeborn-worker/shuffle_data/" +
            s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists())
      })
      val createdSuccessfullyString = shuffleIdMustExist.map(shuffleId => {
        shuffleId.toString + ":" +
          workerDirs.map(dir =>
            new File(s"$dir/celeborn-worker/shuffle_data/" +
              s"${sparkSession.sparkContext.applicationId}/$shuffleId").exists()).toList
      }).mkString(",")
      println(s"shuffle-to-be-deleted status: $deletedSuccessfullyString \n" +
        s"shuffle-to-be-created status: $createdSuccessfullyString")
      deletedSuccessfully && createdSuccessfully
    }

    override def run(): Unit = {
      var allDataInShape = checkDirStatus()
      while (!allDataInShape) {
        Thread.sleep(1000)
        allDataInShape = checkDirStatus()
      }
    }
  }

  protected def triggerStorageCheckThread(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      sparkSession: SparkSession): CheckingThread = {
    val checkingThread =
      new CheckingThread(shuffleIdShouldNotExist, shuffleIdMustExist, sparkSession)
    checkingThread.setDaemon(true)
    checkingThread.start()
    checkingThread
  }

  protected def checkStorageValidation(thread: Thread, timeout: Long = 1200 * 1000): Unit = {
    val checkingThread = thread.asInstanceOf[CheckingThread]
    checkingThread.join(timeout)
    if (checkingThread.isAlive || checkingThread.exception != null) {
      throw new IllegalStateException("the storage checking status failed," +
        s"${checkingThread.isAlive} ${if (checkingThread.exception != null) checkingThread.exception.getMessage
        else "NULL"}")
    }
  }
}
