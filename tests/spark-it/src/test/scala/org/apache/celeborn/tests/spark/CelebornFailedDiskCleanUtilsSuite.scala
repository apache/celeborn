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

import org.apache.spark.SparkEnv
import org.apache.spark.shuffle.celeborn.SparkShuffleManager
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.spark.FailedShuffleCleaner
import org.apache.celeborn.tests.spark.fetch_failure.FetchFailureDiskCleanBase

class CelebornFailedDiskCleanUtilsSuite extends SparkTestBase {
  test("test correctness of RunningStageManager") {
    System.clearProperty(FailedShuffleCleaner.RUNNING_STAGE_CHECKER_CLASS)
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
      val t = new Thread {
        override def run(): Unit = {
          try {
            sparkSession.sparkContext.parallelize(List(1, 2, 3)).mapPartitions { iter =>
              Thread.sleep(60 * 1000)
              iter
            }.collect()
          } catch {
            case _: InterruptedException =>
      t.start()

      eventually(timeout(20.seconds), interval(100.milliseconds)) {
        assert(FailedShuffleCleaner.runningStageManager.isRunningStage(0))
      }

      sparkSession.sparkContext.cancelAllJobs()
      t.interrupt()

      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        assert(!FailedShuffleCleaner.runningStageManager.isRunningStage(0))
      }
    } finally {
      sparkSession.stop()
    }
  }
}
