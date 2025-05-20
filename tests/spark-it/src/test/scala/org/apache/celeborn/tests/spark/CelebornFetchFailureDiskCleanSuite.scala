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

import org.apache.celeborn.tests.spark.fetch.failure.{FetchFailureDiskCleanBase, FileDeletionShuffleReaderGetHook}

class CelebornFetchFailureDiskCleanSuite extends FetchFailureDiskCleanBase {

  test("celeborn spark integration test - the failed shuffle file is cleaned up correctly") {
    if (Spark3OrNewer) {
      val sparkSession = createSparkSession(enableFailedShuffleCleaner = true)
      val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
      val hook = new FileDeletionShuffleReaderGetHook(
        celebornConf,
        workerDirs,
        shuffleIdToBeDeleted = Seq(0))
      TestCelebornShuffleManager.registerReaderGetHook(hook)
      val checkingThread =
        triggerStorageCheckThread(Seq(0), Seq(1), sparkSession, forStableStatusChecking = false)
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
}
