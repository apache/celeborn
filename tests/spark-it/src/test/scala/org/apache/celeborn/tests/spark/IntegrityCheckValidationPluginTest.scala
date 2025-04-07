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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.include
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.CompressionCodec

class IntegrityCheckValidationPluginTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  private def enableCeleborn(conf: SparkConf) = {
    conf
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "10MB")
      .set(s"spark.${CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key}", "true")
      .set(s"spark.plugins", "org.apache.spark.shuffle.celeborn.CelebornIntegrityCheckPlugin")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.ValidatingSparkShuffleManager")
  }

  test(s"celeborn spark integration test - join (no data loss)") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkSession.version.startsWith("3")) {
      import sparkSession.implicits._
      val df1 = sparkSession.sparkContext.parallelize(1 to 120000, 8)
        .map(i => {
          val random = new Random(1024)
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
      val df2 = sparkSession.sparkContext.parallelize(1 to 8, 8)
        .map(i => {
          val random = new Random(1024)
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

      df1.join(df2, $"fa" === $"fb").count shouldBe 960000

      sparkSession.stop()
    }
  }

  test(s"celeborn spark integration test - empty partitions (no loss)") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkSession.version.startsWith("3")) {
      import sparkSession.implicits._

      val resultWithEmptyPartitions = sparkSession.sparkContext
        .parallelize(1 to 2, 2).toDF("r")
        .repartition(10)
        .mapPartitions(iter => Iterator(iter.map(_.getInt(0)).toSeq))
        .collect()
        .sortBy(_.headOption.getOrElse(Int.MaxValue))

      resultWithEmptyPartitions shouldBe List(1) :: List(2) :: List.fill(8)(List())

      sparkSession.stop()
    }
  }

  ignore(s"celeborn spark integration test - RDD collect") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_SORT_RANDOMIZE_PARTITION_ENABLED.key}", "false")
      .set(s"spark.${CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key}", "true")
      .set(s"spark.plugins", "org.apache.spark.shuffle.celeborn.CelebornIntegrityCheckPlugin")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.ValidatingSparkShuffleManager")
      .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", CompressionCodec.LZ4.name)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkSession.version.startsWith("3")) {

      val sampleSeq = (1 to 78)
        .map(Random.alphanumeric)
        .toList
        .map(v => (v.toUpper, Random.nextInt(12) + 1))
      val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 4)
      val resultWithOutCeleborn = inputRdd
        .combineByKey(
          (k: Int) => (k, 1),
          (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
        .collectAsMap()

      resultWithOutCeleborn.size shouldBe 2
    }

    sparkSession.stop()
  }

  test(s"celeborn spark integration test - limit (no loss)") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkSession.version.startsWith("3")) {
      import sparkSession.implicits._
      val limitedResult = sparkSession.sparkContext
        .parallelize(1 to 10000, 1).toDF("r")
        .repartition(2)
        .limit(1)
        .collect()

      limitedResult.length shouldBe 1
    }
    sparkSession.stop()
  }

  test(s"celeborn spark integration test - lost validation") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    if (sparkSession.version.startsWith("3")) {
      import sparkSession.implicits._
      try {

        val registerValidationCalled = new AtomicBoolean(false)
        CelebornIntegrityCheckTracker.setContextCreator(() => {
          new CelebornIntegrityCheckTracker.CelebornReaderContext {
            override def registerValidation(
                shuffleId: Int,
                startMapper: Int,
                endMapper: Int,
                partition: Int): Unit = {
              if (shuffleId == 0 && startMapper == 0 && endMapper == Integer.MAX_VALUE && partition == 0) {
                registerValidationCalled.set(true)
                // skipping the validation registration
              } else {
                super.registerValidation(shuffleId, startMapper, endMapper, partition)
              }
            }
          }
        })

        try {
          sparkSession.sparkContext
            .parallelize(1 to 10000, 1)
            .toDF("r")
            .repartition(10)
            .sort(col("r"))
            .collect()
          fail("Should have thrown an exception")
        } catch {
          case e: Exception =>
            e.getMessage should include("Validation is not registered CelebornIntegrityCheckTracker{shuffleId=0, startMapper=0, endMapper=2147483647, partition=0}")
        }
        registerValidationCalled.get() shouldBe true
        sparkSession.stop()
      } finally {
        CelebornIntegrityCheckTracker.setContextCreator(() =>
          new CelebornIntegrityCheckTracker.CelebornReaderContext())
        sparkSession.stop()
      }
    }
  }

}
