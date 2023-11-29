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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.spark.shuffle.celeborn.SparkColumnarShuffleInterceptor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait SparkColumnarShuffleTestBase extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    SparkColumnarShuffleInterceptor.install()
    checkColumnarShuffleAvaliable
    logInfo("test initialized, setup Celeborn mini cluster")
    setUpMiniCluster(workerNum = 5)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete, stop Celeborn mini cluster")
    SparkSession.getActiveSession.foreach(s => s.stop())
    shutdownMiniCluster()
  }

  def checkColumnarShuffleAvaliable: Unit = {
    assert(
      Class.forName("org.apache.spark.ShuffleDependency")
        .getDeclaredFields
        .exists(_.getName == "schema"),
      "Failed to get schema field"
    )
  }

  def sparkConf: SparkConf =
    new SparkConf()
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set("spark.shuffle.service.enabled", "false")
      .set(s"spark.${CelebornConf.COLUMNAR_SHUFFLE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.SPARK_SHUFFLE_WRITER_MODE.key}", "hash")
      .setMaster("local[2]")

  lazy val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
}


