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

import org.apache.spark.shuffle.celeborn.{SparkUtils, TestCelebornShuffleManager}

import org.apache.celeborn.tests.spark.fetch_failure.{FetchFailureDiskCleanBase, FileDeletionShuffleReaderGetHook}

class CelebornFetchFailureDiskCleanExpensiveSuite extends FetchFailureDiskCleanBase {

  // 6. for multiple level M - 1 lineage , all failed disk spaces are cleaned
  test("celeborn spark integration test - (M-1 dep with multi-level lineage) the failed shuffle files are all cleaned up" +
    " correctly") {
    if (Spark3OrNewer) {
      val sparkSession = createSparkSession(enableFailedShuffleCleaner = true)
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new FileDeletionShuffleReaderGetHook(
        celebornConf,
        workerDirs,
        shuffleIdToBeDeleted = Seq(0, 1, 2, 3),
        triggerStageId = Some(4))
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      val checkingThread = triggerStorageCheckThread(
        Seq(0, 1, 2, 3),
        Seq(4, 5, 6, 7),
        sparkSession,
        forStableStatusChecking = false)
      import sparkSession.implicits._
      val df1 = Seq((1, "a"), (2, "b")).toDF("id", "data").groupBy("id").count()
        .withColumnRenamed("count", "countId").groupBy("countId").count()
        .withColumnRenamed("count", "df1_count")
      val df2 = Seq((2, "c"), (3, "d")).toDF("id", "data").groupBy("id").count()
        .withColumnRenamed("count", "countId").groupBy("countId").count()
        .withColumnRenamed("count", "df2_count")
      val df = df1.hint("merge").join(df2, "countId").select("*").persist()
      val tuples = df.collect()
      println("first job done")
      df.collect()
      println("second job done")
      checkStorageValidation(checkingThread, timeout = 600 * 1000)
      // verify result
      assert(hook.executed.get())
      val expect = "[1,2,2]"
      assert(tuples.head.toString().equals(expect))
      sparkSession.stop()
    }
  }

}
