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
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.spark.FailedShuffleCleaner

class CelebornFailedDiskCleanUtils extends SparkTestBase {
  test("test correctness of RunningStageManager") {
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
    var t: Thread = null
    eventually(timeout(20.seconds), interval(100.milliseconds)) {
      t = new Thread {
        override def run(): Unit = {
          try {
            sparkSession.sparkContext.parallelize(List(1, 2, 3)).foreach(_ =>
              Thread.sleep(60 * 1000))
          } catch {
            case _: Throwable =>
            // swallow everything
          }
        }
      }
      t.start()
      assert(FailedShuffleCleaner.runningStageManager.isRunningStage(0))
    }
    sparkSession.stop()
  }
}
