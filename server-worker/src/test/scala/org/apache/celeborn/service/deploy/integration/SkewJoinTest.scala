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

package org.apache.celeborn.service.deploy.integration

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.{AfterClass, BeforeClass, Test}

import org.apache.celeborn.client.compress.Compressor.CompressionCodec
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.SparkTestBase

class SkewJoinTest extends SparkTestBase {

  private def enableRss(conf: SparkConf) = {
    val localhost = Utils.localHostName()
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.RssShuffleManager")
      .set("spark.rss.master.address", s"${localhost}:9097")
      .set("spark.rss.shuffle.split.threshold", "10MB")
  }

  @Test
  def test(): Unit = {
    CompressionCodec.values().foreach { codec =>
      val sparkConf = new SparkConf().setAppName("rss-demo")
        .setMaster("local[4]")
        .set("spark.sql.adaptive.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.enabled", "true")
        .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
        .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "12MB")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("spark.sql.parquet.compression.codec", "gzip")
        .set("rss.client.compression.codec", codec.name());

      enableRss(sparkConf)

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
      }
    }
  }
}

object SkewJoinTest extends SparkTestBase {
  @BeforeClass
  def beforeAll(): Unit = {
    logInfo("test initialized , setup rss mini cluster")
    tuple = setupRssMiniCluster()
  }

  @AfterClass
  def afterAll(): Unit = {
    logInfo("all test complete , stop rss mini cluster")
    clearMiniCluster(tuple)
  }
}
