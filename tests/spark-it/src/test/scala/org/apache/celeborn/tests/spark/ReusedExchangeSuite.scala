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
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

class ReusedExchangeSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  test("[CELEBORN-980] Asynchronously delete original files to fix ReusedExchange bug") {
    testReusedExchange(false)
  }

  test("[CELEBORN-1177] OpenStream should register stream via ChunkStreamManager to close stream for ReusedExchange") {
    testReusedExchange(true)
  }

  def testReusedExchange(readLocalShuffle: Boolean): Unit = {
    val sparkConf = new SparkConf().setAppName("celeborn-test").setMaster("local[2]")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.READ_LOCAL_SHUFFLE_FILE.key}", readLocalShuffle.toString)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "100")
    //    spark.sql("set spark.sql.adaptive.localShuffleReader.enabled=false")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
    spark.range(0, 1000, 1, 10)
      .selectExpr("id as k1", "id as v1")
      .createOrReplaceTempView("ta")
    spark.range(0, 1000, 1, 10)
      .selectExpr("id % 1 as k21", "id % 1 as k22", "id as v2")
      .createOrReplaceTempView("tb")
    spark.range(140)
      .select(
        col("id").cast("long").as("k3"),
        concat(col("id").cast("string"), lit("a")).as("v3"))
      .createOrReplaceTempView("tc")

    spark.sql(
      """
        |SELECT *
        |FROM ta
        |LEFT JOIN tb ON ta.k1 = tb.k21
        |LEFT JOIN tc ON tb.k22 = tc.k3
        |""".stripMargin)
      .createOrReplaceTempView("v1")

    spark.sql(
      """
        |SELECT * FROM v1 WHERE v3 IS NOT NULL
        |UNION
        |SELECT * FROM v1
        |""".stripMargin)
      .collect()
    spark.stop
  }
}
