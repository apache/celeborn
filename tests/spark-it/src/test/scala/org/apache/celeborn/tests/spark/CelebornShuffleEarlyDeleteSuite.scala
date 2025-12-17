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

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.celeborn.{SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.tests.spark.fetch.failure.FailedCommitAndExpireDataReaderHook

class CelebornShuffleEarlyDeleteSuite extends SparkTestBase {

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

  private def createSparkSession(additionalConf: Map[String, String] = Map()): SparkSession = {
    var builder = SparkSession
      .builder()
      .master("local[*, 4]")
      .appName("celeborn early delete")
      .config(updateSparkConf(new SparkConf(), ShuffleMode.SORT))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.shuffle.enabled", "true")
      .config("spark.celeborn.client.shuffle.expired.checkInterval", "1s")
      .config("spark.kryoserializer.buffer.max", "2047m")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config(s"spark.${CelebornConf.CLIENT_SHUFFLE_EARLY_DELETION.key}", "true")
      .config(s"spark.${CelebornConf.CLIENT_SHUFFLE_EARLY_DELETION_INTERVAL_MS.key}", "1000")
    additionalConf.foreach { case (k, v) =>
      builder = builder.config(k, v)
    }
    builder.getOrCreate()
  }

  test("spark integration test - delete shuffle data from unneeded stages") {
    if (Spark3OrNewer) {
      val spark = createSparkSession()
      try {
        val rdd1 = spark.sparkContext.parallelize(0 until 20, 3).repartition(2)
          .repartition(4)
        val t = new Thread() {
          override def run(): Unit = {
            // shuffle 1
            rdd1.mapPartitions(iter => {
              Thread.sleep(20000)
              iter
            }).count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = Seq(0, 2), // guard on 2 to prevent any stage retry
          shuffleIdMustExist = Seq(1),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - delete shuffle data only when all child stages finished") {
    if (Spark3OrNewer) {
      val spark = createSparkSession()
      try {
        val rdd1 = spark.sparkContext.parallelize(0 until 20, 3).repartition(2)
        val rdd2 = rdd1.repartition(4)
        val rdd3 = rdd1.repartition(4)
        val t = new Thread() {
          override def run(): Unit = {
            rdd2.union(rdd3).mapPartitions(iter => {
              Thread.sleep(20000)
              iter
            }).count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = Seq(0, 3), // guard on 3 to prevent any stage retry
          shuffleIdMustExist = Seq(1, 2),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - delete shuffle data only when all child stages finished" +
    " (multi-level lineage)") {
    if (Spark3OrNewer) {
      val spark = createSparkSession()
      try {
        val rdd1 = spark.sparkContext.parallelize(0 until 20, 3).repartition(2)
        val rdd2 = rdd1.repartition(4).repartition(2)
        val rdd3 = rdd1.repartition(4).repartition(2)
        val t = new Thread() {
          override def run(): Unit = {
            rdd2.union(rdd3).mapPartitions(iter => {
              Thread.sleep(20000)
              iter
            }).count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = Seq(0, 1, 2, 5), // guard on 5 to prevent any stage retry
          shuffleIdMustExist = Seq(3, 4),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - when the stage has a skipped parent stage, we should still be" +
    " able to delete data") {
    if (Spark3OrNewer) {
      val spark = createSparkSession()
      try {
        val rdd1 = spark.sparkContext.parallelize(0 until 20, 3).repartition(2)
        rdd1.count()
        val t = new Thread() {
          override def run(): Unit = {
            rdd1.mapPartitions(iter => {
              Thread.sleep(20000)
              iter
            }).repartition(3).count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = Seq(0, 2),
          shuffleIdMustExist = Seq(1),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
      } finally {
        spark.stop()
      }
    }
  }

  private def deleteTooEarlyTest(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      spark: SparkSession): Unit = {
    if (Spark3OrNewer) {
      var r = 0L
      try {
        // shuffle 0
        val rdd1 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        rdd1.count()
        val t = new Thread() {
          override def run(): Unit = {
            // shuffle 1
            val rdd2 = rdd1.mapPartitions(iter => {
              Thread.sleep(10000)
              iter
            }).repartition(3)
            rdd2.count()
            println("rdd2.count() finished")
            // leaving enough time for shuffle 0 to be expired
            Thread.sleep(10000)
            // shuffle 2
            val rdd3 = rdd1.repartition(5).mapPartitions(iter => {
              Thread.sleep(10000)
              iter
            })
            r = rdd3.count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = shuffleIdShouldNotExist,
          shuffleIdMustExist = shuffleIdMustExist,
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
        assert(r === 20)
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - do not fail job when shuffle is deleted \"too early\"") {
    val spark = createSparkSession()
    deleteTooEarlyTest(Seq(0, 3, 5), Seq(1, 2, 4), spark)
  }

  //  test("spark integration test - do not fail job when shuffle is deleted \"too early\"" +
  //    " (with failed shuffle deletion)") {
  //    val spark = createSparkSession(
  //      Map(s"spark.${CelebornConf.CLIENT_FETCH_CLEAN_FAILED_SHUFFLE.key}" -> "true"))
  //    deleteTooEarlyTest(Seq(0, 2, 3, 5), Seq(1, 4), spark)
  //  }

  test("spark integration test - do not fail job when shuffle files" +
    " are deleted \"too early\" (ancestor dependency)") {
    val spark = createSparkSession()
    if (Spark3OrNewer) {
      var r = 0L
      try {
        // shuffle 0
        val rdd1 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        rdd1.count()
        val t = new Thread() {
          override def run(): Unit = {
            // shuffle 1
            val rdd2 = rdd1.repartition(3)
            rdd2.count()
            println("rdd2.count finished()")
            // leaving enough time for shuffle 0 to be expired
            Thread.sleep(10000)
            // shuffle 2
            rdd2.repartition(4).count()
            // leaving enough time for shuffle 1 to be expired
            Thread.sleep(10000)
            val rdd4 = rdd1.union(rdd2).mapPartitions(iter => {
              Thread.sleep(10000)
              iter
            })
            r = rdd4.count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = Seq(0, 1, 5),
          shuffleIdMustExist = Seq(3, 4),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
        assert(r === 40)
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - do not fail job when multiple shuffles (be unioned)" +
    " are deleted \"too early\"") {
    if (Spark3OrNewer) {
      val spark = createSparkSession()
      var r = 0L
      try {
        // shuffle 0&1
        val rdd1 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        val rdd2 = spark.sparkContext.parallelize((0 until 30), 3).repartition(2)
        rdd1.count()
        rdd2.count()
        val t = new Thread() {
          override def run(): Unit = {
            // shuffle 2&3
            val rdd3 = rdd1.repartition(3)
            val rdd4 = rdd2.repartition(3)
            rdd3.count()
            rdd4.count()
            // leaving enough time for shuffle 0&1 to be expired
            Thread.sleep(10000)
            // shuffle 4&5
            rdd3.repartition(4).count()
            rdd4.repartition(4).count()
            // leaving enough time for shuffle 2&3 to be expired
            Thread.sleep(10000)
            val rdd5 = rdd3.union(rdd4).mapPartitions(iter => {
              Thread.sleep(10000)
              iter
            })
            r = rdd5.count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          // 4,5 are based on vanilla spark gc which are not necessarily stable in a test
          // 6,7 is based on failed shuffle cleanup, which is not covered here
          shuffleIdShouldNotExist = Seq(0, 1, 2, 3, 8, 9, 12),
          shuffleIdMustExist = Seq(10, 11),
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
        assert(r === 50)
      } finally {
        spark.stop()
      }
    }
  }

  //  test("spark integration test - do not fail job when multiple shuffles (be zipped/joined)" +
  //    " are deleted \"too early\"") {
  //    if (runningWithSpark3OrNewer()) {
  //      val spark = createSparkSession(
  //        Map(s"spark.${CelebornConf.CLIENT_FETCH_CLEAN_FAILED_SHUFFLE.key}" -> "true"))
  //      var r = 0L
  //      try {
  //        // shuffle 0&1
  //        val rdd1 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
  //        val rdd2 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
  //        rdd1.count()
  //        rdd2.count()
  //        val t = new Thread() {
  //          override def run(): Unit = {
  //            // shuffle 2&3
  //            val rdd3 = rdd1.repartition(3)
  //            val rdd4 = rdd2.repartition(3)
  //            rdd3.count()
  //            rdd4.count()
  //            // leaving enough time for shuffle 0&1 to be expired
  //            Thread.sleep(10000)
  //            // shuffle 4&5
  //            rdd3.repartition(4).count()
  //            rdd4.repartition(4).count()
  //            // leaving enough time for shuffle 2&3 to be expired
  //            Thread.sleep(10000)
  //            println("starting job for rdd 5")
  //            val rdd5 = rdd3.zip(rdd4).mapPartitions(iter => {
  //              Thread.sleep(10000)
  //              iter
  //            })
  //            r = rdd5.count()
  //          }
  //        }
  //        t.start()
  //        val thread = StorageCheckUtils.triggerStorageCheckThread(
  //          workerDirs,
  //          // 4,5 are based on vanilla spark gc which are not necessarily stable in a test
  //          // 6,9 is based on failed shuffle cleanup, which is not covered here
  //          shuffleIdShouldNotExist = Seq(0, 1, 2, 3, 7, 10, 12),
  //          shuffleIdMustExist = Seq(8, 11),
  //          sparkSession = spark,
  //          forStableStatusChecking = false)
  //        StorageCheckUtils.checkStorageValidation(thread)
  //        t.join()
  //        assert(r === 20)
  //      } finally {
  //        spark.stop(
  //      }
  //    }
  //  }

  private def multiShuffleFailureTest(
      shuffleIdShouldNotExist: Seq[Int],
      shuffleIdMustExist: Seq[Int],
      spark: SparkSession): Unit = {
    if (Spark3OrNewer) {
      val celebornConf = SparkUtils.fromSparkConf(spark.sparkContext.getConf)
      val hook = new FailedCommitAndExpireDataReaderHook(
        celebornConf,
        triggerShuffleId = 6,
        shuffleIdsToExpire = (0 to 5).toList)
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      var r = 0L
      try {
        // shuffle 0&1&2
        val rdd1 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        val rdd2 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        val rdd3 = spark.sparkContext.parallelize((0 until 20), 3).repartition(2)
        val t = new Thread() {
          override def run(): Unit = {
            // shuffle 3&4&5
            val rdd4 = rdd1.repartition(3)
            val rdd5 = rdd2.repartition(3)
            val rdd6 = rdd3.repartition(3)
            println("starting job for rdd 7")
            val rdd7 = rdd4.zip(rdd5).zip(rdd6).repartition(2)
            r = rdd7.count()
          }
        }
        t.start()
        val thread = StorageCheckUtils.triggerStorageCheckThread(
          workerDirs,
          shuffleIdShouldNotExist = shuffleIdShouldNotExist,
          shuffleIdMustExist = shuffleIdMustExist,
          sparkSession = spark)
        StorageCheckUtils.checkStorageValidation(thread)
        t.join()
        assert(r === 20)
      } finally {
        spark.stop()
      }
    }
  }

  test("spark integration test - do not fail job when multiple shuffles (be zipped/joined)" +
    " are to be retried for fetching") {
    val spark = createSparkSession(Map("spark.stage.maxConsecutiveAttempts" -> "3"))
    multiShuffleFailureTest(Seq(0, 1, 2, 3, 4, 5), Seq(17), spark)
  }

  //  test("spark integration test - do not fail job when multiple shuffles (be zipped/joined)" +
  //    " are to be retried for fetching (with failed shuffle deletion)") {
  //    val spark = createSparkSession(Map(
  //      "spark.stage.maxConsecutiveAttempts" -> "3",
  //      s"spark.${CelebornConf.CLIENT_FETCH_CLEAN_FAILED_SHUFFLE.key}" -> "true"))
  //    multiShuffleFailureTest(Seq(0, 1, 2, 3, 4, 5, 8, 9, 10), Seq(17), spark)
  //  }
}
