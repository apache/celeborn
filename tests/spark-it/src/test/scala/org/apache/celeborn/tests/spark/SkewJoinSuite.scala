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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.CompressionCodec

class SkewJoinSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  private def enableCeleborn(conf: SparkConf) = {
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "10MB")
  }

  CompressionCodec.values.foreach { codec =>
    Seq(false, true).foreach { enabled =>
      test(
        s"celeborn spark integration test - skew join - with $codec - with client skew $enabled") {
        val sparkConf = new SparkConf().setAppName("celeborn-demo")
          .setMaster("local[2]")
          .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
          .set("spark.sql.adaptive.skewJoin.enabled", "true")
          .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
          .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "12MB")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
          .set(SQLConf.PARQUET_COMPRESSION.key, "gzip")
          .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", codec.name)
          .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")
          .set(
            s"spark.${CelebornConf.CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ_ENABLED.key}",
            s"$enabled")

        enableCeleborn(sparkConf)

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        if (sparkSession.version.startsWith("3")) {
          import sparkSession.implicits._
          val df = sparkSession.sparkContext.parallelize(1 to 120000, 8)
            .map(i => {
              val random = new Random()
              val oriKey = random.nextInt(64)
              val key = if (oriKey < 32) 1 else oriKey
              val fas = random.nextInt(1200000)
              val fa = Range(fas, fas + 100).mkString(",")
              val fbs = random.nextInt(1200000)
              val fb = Range(fbs, fbs + 100).mkString(",")
              val fcs = random.nextInt(1200000)
              val fc = Range(fcs, fcs + 100).mkString(",")
              val fds = random.nextInt(1200000)
              val fd = Range(fds, fds + 100).mkString(",")

              (key, fa, fb, fc, fd)
            })
            .toDF("fa", "f1", "f2", "f3", "f4")
          df.createOrReplaceTempView("view1")
          val df2 = sparkSession.sparkContext.parallelize(1 to 8, 8)
            .map(i => {
              val random = new Random()
              val oriKey = random.nextInt(64)
              val key = if (oriKey < 32) 1 else oriKey
              val fas = random.nextInt(1200000)
              val fa = Range(fas, fas + 100).mkString(",")
              val fbs = random.nextInt(1200000)
              val fb = Range(fbs, fbs + 100).mkString(",")
              val fcs = random.nextInt(1200000)
              val fc = Range(fcs, fcs + 100).mkString(",")
              val fds = random.nextInt(1200000)
              val fd = Range(fds, fds + 100).mkString(",")
              (key, fa, fb, fc, fd)
            })
            .toDF("fb", "f6", "f7", "f8", "f9")
          df2.createOrReplaceTempView("view2")
          sparkSession.sql("drop table if exists fres")
          sparkSession.sql("create table fres using parquet as select * from view1 a inner join view2 b on a.fa=b.fb where a.fa=1 ")
          sparkSession.sql("drop table fres")
          sparkSession.stop()
        }
      }
    }
  }

}
