package org.apache.celeborn.tests.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient

class RetryReviveTest extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup celeborn mini cluster")
    val workerConf = Map(
      "celeborn.test.retryCommitFiles" -> s"true")
    setUpMiniCluster(masterConfs = null, workerConfs = workerConf)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test("celeborn spark integration test - retry revive as configued times") {
    val sparkConf = new SparkConf()
//      .set("spark.celeborn.test.retryCommitFiles", "true")
      .setAppName("rss-demo").setMaster("local[4]")
    val ss = SparkSession.builder().config(updateSparkConf(sparkConf, false)).getOrCreate()
    ss.sparkContext.parallelize(1 to 1000, 2)
      .map { i => (i, Range(1, 1000).mkString(",")) }.groupByKey(16).collect()
    ss.stop()
  }
}
