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

  private def enableCeleborn(conf: SparkConf) = {
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "10MB")
  }

  test("ReusedExchange end to end test") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo").setMaster("local[2]")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "100")
    //    spark.sql("set spark.sql.adaptive.localShuffleReader.enabled=false")
    val spark = SparkSession.builder()
      .config(enableCeleborn(sparkConf))
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
        |select *
        | from ta
        |left join tb on ta.k1 = tb.k21
        |left join tc on tb.k22 = tc.k3
        |""".stripMargin)
      .createOrReplaceTempView("v1")

    spark.sql(
      """
        |select distinct * from v1 where v3 is not null
        |union
        |select distinct * from v1
        |""".stripMargin)
      .collect()
  }
}
