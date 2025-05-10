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

package org.apache.celeborn.tests.spark.fetch_failure

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.tests.spark.SparkTestBase

private[tests] trait FetchFailureTestBase extends SparkTestBase {

  def createSparkSession(
      overrideShuffleMgr: Boolean = true,
      enableFailedShuffleCleaner: Boolean = false,
      enableCustomizedRunningStageMgr: Boolean = false): SparkSession = {
    val sparkConf = new SparkConf().setAppName({
      if (!enableFailedShuffleCleaner) {
        "fetch-failure"
      } else {
        "fetch-failure-failed-shuffle-clean"
      }
    }).setMaster("local[2,3]")
    var baseBuilder = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.driver.memory", "4g")
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.shuffle.enabled", "true")
      .config("spark.celeborn.client.shuffle.expired.checkInterval", "1s")
      .config("spark.kryoserializer.buffer.max", "2047m")
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")

    baseBuilder =
      if (overrideShuffleMgr) {
        baseBuilder.config(
          "spark.shuffle.manager",
          "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      } else {
        baseBuilder
      }
    baseBuilder =
      if (enableFailedShuffleCleaner) {
        baseBuilder.config("spark.celeborn.client.spark.fetch.cleanFailedShuffle", "true")
      } else {
        baseBuilder
      }

    baseBuilder =
      if (enableCustomizedRunningStageMgr) {
        baseBuilder.config(
          "spark.celeborn.client.spark.fetch.cleanFailedShuffle" +
            ".runningStageManagerImpl",
          "org.apache.celeborn.tests.spark.fetch_failure.TestRunningStageManager")
      } else {
        baseBuilder
      }
    baseBuilder.getOrCreate()
  }
}
