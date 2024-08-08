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

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{ShuffleManagerHook, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.worker.Worker

class CelebornWorkerRestartSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    setupMiniClusterWithRandomPorts(workerNum = 1)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  var workerList: Seq[Worker] = Seq.empty

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    val worker = super.createWorker(map, storageDir)
    workerList = workerList :+ worker
    worker
  }

  class ShuffleWriteCleanWorkerHook(targetAppShuffleId: Int, shuffleKey: String)
    extends ShuffleManagerHook {
    var executed: AtomicBoolean = new AtomicBoolean(false)
    val lock = new Object

    override def exec(handle: ShuffleHandle, mapId: Long, context: TaskContext): Unit = {
      if (executed.get() == true) return

      if (handle.shuffleId == targetAppShuffleId && mapId == 0) {
        lock.synchronized {
          // Wait for other tasks to succeed
          Thread.sleep(20 * 1000)
          val jHashSet: java.util.HashSet[String] = new java.util.HashSet[String]()
          jHashSet.add(shuffleKey)
          workerList.foreach(_.cleanup(
            jHashSet,
            ThreadUtils.newDaemonCachedThreadPool(
              "worker-clean-expired-shuffle-keys",
              1)))
          executed.set(true)
        }
      }
    }
  }

  test("celeborn spark integration test - correctness issue when worker restart") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
      .set("spark.proxy.appid", "appId-test01")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.shuffle.enabled", "true")
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config("spark.celeborn.client.unregister.shuffle.when.fetchFailure", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val hook = new ShuffleWriteCleanWorkerHook(0, "appId-test01-2")
    TestCelebornShuffleManager.registerWriterGetHook(hook)

    val rdd1 = sc.parallelize(0 until 10000, 2).map(v => (v, v)).groupByKey()
    val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()
    val rdd3 = rdd2.map(v => (v._1, v._2.map(t => t * t))).groupByKey()
    hook.executed.set(false)
    assert(rdd3.count() == 10000)
    sparkSession.stop()
  }

}
