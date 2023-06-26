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
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.PushDataHandler

class PushDataTimeoutTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val workerConf = Map(
      CelebornConf.TEST_CLIENT_PUSH_MASTER_DATA_TIMEOUT.key -> "true",
      CelebornConf.TEST_WORKER_PUSH_SLAVE_DATA_TIMEOUT.key -> "true")
    setUpMiniCluster(masterConfs = null, workerConfs = workerConf, workerNum = 4)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
    PushDataHandler.pushMasterDataTimeoutTested.set(false)
    PushDataHandler.pushSlaveDataTimeoutTested.set(false)
    PushDataHandler.pushMasterMergeDataTimeoutTested.set(false)
    PushDataHandler.pushSlaveMergeDataTimeoutTested.set(false)
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  Seq(false, true).foreach { enabled =>
    test(s"celeborn spark integration test - pushdata timeout w/ replicate = $enabled") {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
        .set(s"spark.${CelebornConf.CLIENT_PUSH_DATA_TIMEOUT.key}", "5s")
        .set(s"spark.celeborn.data.push.timeoutCheck.interval", "2s")
        .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", enabled.toString)
        .set(s"spark.${CelebornConf.CLIENT_BLACKLIST_SLAVE_ENABLED.key}", "false")
        // make sure PushDataHandler.handlePushData be triggered
        .set(s"spark.${CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key}", "5")

      val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      val sqlResult = runsql(sparkSession)

      Thread.sleep(3000L)
      sparkSession.stop()

      val rssSparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .getOrCreate()
      val rssSqlResult = runsql(rssSparkSession)

      assert(sqlResult.equals(rssSqlResult))

      rssSparkSession.stop()
      ShuffleClient.reset()

      assert(PushDataHandler.pushMasterDataTimeoutTested.get())
      if (enabled) {
        assert(PushDataHandler.pushSlaveDataTimeoutTested.get())
      }
    }
  }

  Seq(false, true).foreach { enabled =>
    test(s"celeborn spark integration test - pushMergeData timeout w/ replicate = $enabled") {
      val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
        .set(s"spark.${CelebornConf.CLIENT_PUSH_DATA_TIMEOUT.key}", "5s")
        .set(s"spark.celeborn.data.push.timeoutCheck.interval", "2s")
        .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", enabled.toString)
        .set(s"spark.${CelebornConf.CLIENT_BLACKLIST_SLAVE_ENABLED.key}", "false")

      val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      val sqlResult = runsql(sparkSession)

      Thread.sleep(3000L)
      sparkSession.stop()

      val rssSparkSession = SparkSession.builder()
        .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
        .getOrCreate()
      val rssSqlResult = runsql(rssSparkSession)

      assert(sqlResult.equals(rssSqlResult))

      rssSparkSession.stop()
      ShuffleClient.reset()
      assert(PushDataHandler.pushMasterMergeDataTimeoutTested.get())
      if (enabled) {
        assert(PushDataHandler.pushSlaveMergeDataTimeoutTested.get())
      }
    }
  }

  test("celeborn spark integration test - pushdata timeout slave will add to blacklist") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2]")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_DATA_TIMEOUT.key}", "5s")
      .set(s"spark.${CelebornConf.CLIENT_BLACKLIST_SLAVE_ENABLED.key}", "true")
    val rssSparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .getOrCreate()
    try {
      combine(rssSparkSession)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        e.getMessage.concat("Revive Failed in retry push merged data for location")
    }

    rssSparkSession.stop()
    ShuffleClient.reset()
    assert(PushDataHandler.pushMasterMergeDataTimeoutTested.get())
    assert(!PushDataHandler.pushSlaveMergeDataTimeoutTested.get())
  }
}
