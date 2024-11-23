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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

class SparkUtilsSuite extends AnyFunSuite {
  test("another task running or successful") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    try {
      val sc = sparkSession.sparkContext
      val jobThread = new Thread {
        override def run(): Unit = {
          try {
            val rdd = sc.parallelize(1 to 100, 2)
            rdd.mapPartitions { iter =>
              Thread.sleep(5000)
              iter
            }.collect()
          } catch {
            case _: InterruptedException =>
          }
        }
      }
      jobThread.start()

      eventually(timeout(5 seconds), interval(100 milliseconds)) {
        val taskId = 0
        val taskSetManager = SparkUtils.getTaskSetManager(taskId)
        assert(taskSetManager != null)
        assert(SparkUtils.getTaskAttempts(taskSetManager, taskId).size() == 1)
        assert(!SparkUtils.taskAnotherAttemptRunningOrSuccessful(taskId))
      }

      jobThread.interrupt()
    } finally {
      sparkSession.stop()
    }
  }
}
