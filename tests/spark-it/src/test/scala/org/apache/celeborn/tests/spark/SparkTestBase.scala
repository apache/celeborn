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

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker

trait SparkTestBase extends Logging with MiniClusterFeature {
  private val sampleSeq = (1 to 78)
    .map(Random.alphanumeric)
    .toList
    .map(v => (v.toUpper, Random.nextInt(12) + 1))

  @volatile var tuple: (
      Master,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Thread,
      Thread,
      Thread,
      Thread) = _

  def clearMiniCluster(
      tuple: (
          Master,
          RpcEnv,
          Worker,
          RpcEnv,
          Worker,
          RpcEnv,
          Worker,
          RpcEnv,
          Thread,
          Thread,
          Thread,
          Thread)): Unit = {
    tuple._3.close()
    tuple._4.shutdown()
    tuple._5.close()
    tuple._6.shutdown()
    tuple._7.close()
    tuple._8.shutdown()
    tuple._1.close()
    tuple._2.shutdown()
    Thread.sleep(5000L)
    tuple._10.interrupt()
    tuple._11.interrupt()
    tuple._12.interrupt()
  }

  def setupRssMiniClusterSpark(
      masterConfs: Map[String, String] = null,
      workerConfs: Map[String, String] = null): (
      Master,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Worker,
      RpcEnv,
      Thread,
      Thread,
      Thread,
      Thread) = {
    Thread.sleep(3000L)

    val (master, masterRpcEnv) = createMaster(masterConfs)
    val (worker1, workerRpcEnv1) = createWorker(workerConfs)
    val (worker2, workerRpcEnv2) = createWorker(workerConfs)
    val (worker3, workerRpcEnv3) = createWorker(workerConfs)
    val masterThread = runnerWrap(masterRpcEnv.awaitTermination())
    val workerThread1 = runnerWrap(worker1.initialize())
    val workerThread2 = runnerWrap(worker2.initialize())
    val workerThread3 = runnerWrap(worker3.initialize())

    masterThread.start()
    Thread.sleep(5000L)

    workerThread1.start()
    workerThread2.start()
    workerThread3.start()
    Thread.sleep(5000L)

    assert(worker1.isRegistered())
    assert(worker2.isRegistered())
    assert(worker3.isRegistered())

    (
      master,
      masterRpcEnv,
      worker1,
      workerRpcEnv1,
      worker2,
      workerRpcEnv2,
      worker3,
      workerRpcEnv3,
      masterThread,
      workerThread1,
      workerThread2,
      workerThread3)
  }

  def updateSparkConf(sparkConf: SparkConf, sort: Boolean): SparkConf = {
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.RssShuffleManager")
    sparkConf.set("spark.shuffle.useOldFetchProtocol", "true")
    sparkConf.set("spark.sql.adaptive.enabled", "false")
    sparkConf.set("spark.shuffle.service.enabled", "false")
    sparkConf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    sparkConf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    sparkConf.set("spark.celeborn.master.endpoints", tuple._1.rpcEnv.address.toString)
    if (sort) {
      sparkConf.set("spark.celeborn.shuffle.writer.mode", "sort")
    }
    sparkConf
  }

  def combine(sparkSession: SparkSession): collection.Map[Char, (Int, Int)] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 4)
    val resultWithOutRss = inputRdd
      .combineByKey(
        (k: Int) => (k, 1),
        (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .collectAsMap()
    resultWithOutRss
  }

  def repartition(sparkSession: SparkSession): collection.Map[Char, Int] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.repartition(8).reduceByKey((acc, v) => acc + v).collectAsMap()
    result
  }

  def groupBy(sparkSession: SparkSession): collection.Map[Char, String] = {
    val inputRdd = sparkSession.sparkContext.parallelize(sampleSeq, 2)
    val result = inputRdd.groupByKey().sortByKey().collectAsMap()
    result.map(k => (k._1, k._2.toList.sorted.mkString(","))).toMap
  }

  def runsql(sparkSession: SparkSession): Map[String, Long] = {
    import sparkSession.implicits._
    val df = Seq(("fa", "fb"), ("fa1", "fb1"), ("fa2", "fb2"), ("fa3", "fb3")).toDF("fa", "fb")
    df.createOrReplaceTempView("tmp")
    val result = sparkSession.sql("select fa,count(fb) from tmp group by fa order by fa")
    val outMap = result.collect().map(row => row.getString(0) -> row.getLong(1)).toMap
    outMap
  }
}
