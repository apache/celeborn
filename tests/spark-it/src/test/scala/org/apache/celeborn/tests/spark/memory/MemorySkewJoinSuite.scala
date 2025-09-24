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

package org.apache.celeborn.tests.spark.memory

import java.io.File

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.CompressionCodec

class MemorySkewJoinSuite extends AnyFunSuite
  with MemorySparkTestBase
  with BeforeAndAfterEach {

  var lastResult: Row = _

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

  val sparkConf = new SparkConf().setAppName("celeborn-demo")
    .setMaster("local[2]")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
    .set("spark.sql.adaptive.skewJoin.enabled", "true")
    .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
    .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "12MB")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    .set(SQLConf.PARQUET_COMPRESSION.key, "gzip")
    .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")

  val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
  if (sparkSession.version.startsWith("3")) {
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.parallelize(1 to 120000, 8)
      .map(_ => {
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
      .toDF("key", "fa", "fb", "fc", "fd")
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
      .toDF("key", "fa", "fb", "fc", "fd")
    df2.createOrReplaceTempView("view2")
    JavaUtils.deleteRecursively(new File("./df1"))
    JavaUtils.deleteRecursively(new File("./df2"))
    df.write.parquet("./df1")
    df2.write.parquet("./df2")
    sparkSession.close()
  }

  CompressionCodec.values.foreach { codec =>
    Array(true, false).foreach { enablePrefetch =>
      test(s"celeborn spark integration test - skew join - $codec $enablePrefetch") {
        val sparkConf = new SparkConf().setAppName("celeborn-demo")
          .setMaster("local[2]")
          .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
          .set("spark.sql.adaptive.skewJoin.enabled", "true")
          .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
          .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "12MB")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
          .set(s"spark.celeborn.storage.availableTypes", "HDD,MEMORY")
          .set(SQLConf.PARQUET_COMPRESSION.key, "gzip")
          .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", codec.name)
          .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")
          .set(s"spark.${CelebornConf.CLIENT_CHUNK_PREFETCH_ENABLED.key}", enablePrefetch.toString)

        enableCeleborn(sparkConf)

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        if (sparkSession.version.startsWith("3")) {
          sparkSession.read.parquet("./df1").repartition(8).createOrReplaceTempView("df1")
          sparkSession.read.parquet("./df2").repartition(8).createOrReplaceTempView("df2")
          val result = sparkSession.sql("select count(*), max(df1.fa), max(df1.fb), max(df1.fc)," +
            "max(df1.fd), max(df2.fa), max(df2.fb), max(df2.fc), max(df2.fd)" +
            "from df1 join df2 on df1.key=df2.key and df1.key=1").collect()
          if (lastResult == null) {
            lastResult = result(0)
          } else {
            assert((lastResult.getLong(0) == result(0).getLong(0)) &&
              (lastResult.getString(1) == result(0).getString(1)) &&
              (lastResult.getString(2) == result(0).getString(2)) &&
              (lastResult.getString(3) == result(0).getString(3)) &&
              (lastResult.getString(4) == result(0).getString(4)) &&
              (lastResult.getString(5) == result(0).getString(5)) &&
              (lastResult.getString(6) == result(0).getString(6)) &&
              (lastResult.getString(7) == result(0).getString(7)) &&
              (lastResult.getString(8) == result(0).getString(8)))
          }
          sparkSession.stop()
        }
      }
    }
  }
}
