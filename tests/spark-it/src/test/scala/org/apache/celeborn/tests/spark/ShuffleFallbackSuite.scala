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

class ShuffleFallbackSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized")
  }

  override def afterAll(): Unit = {
    logInfo("all test complete")
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  private def enableCeleborn(conf: SparkConf) = {
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
  }
  /*
  test(s"celeborn spark integration test - fallback") {
    setupMiniClusterWithRandomPorts(workerNum = 5)
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")
      .set(s"spark.${CelebornConf.SPARK_SHUFFLE_FORCE_FALLBACK_ENABLED.key}", "true")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = sparkSession.sparkContext.parallelize(1 to 120000, 8)
      .repartition(100)
    df.collect()
    sparkSession.stop()
    shutdownMiniCluster()
  }

  test(s"celeborn spark integration test - fallback with workers unavailable") {
    setupMiniClusterWithRandomPorts(workerNum = 0)
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df = sparkSession.sparkContext.parallelize(1 to 120000, 8)
      .repartition(100)
    df.collect()
    sparkSession.stop()
    shutdownMiniCluster()
  }

   */
}
