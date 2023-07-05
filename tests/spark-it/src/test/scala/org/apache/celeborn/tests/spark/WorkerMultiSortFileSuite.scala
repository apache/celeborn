package org.apache.celeborn.tests.spark

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

class WorkerMultiSortFileSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("SkewJoinSuite initialized , setup Celeborn mini cluster")
    val workConf = Map("celeborn.worker.sortPartition.sortFileSizePerThread" -> "3mb");
    setUpMiniCluster(workerNum = 5)
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
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "10MB")
  }

  test(s"celeborn spark integration test multi sort file") {
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "12MB")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.parquet.compression.codec", "gzip")
      .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", "ZSTD")
      .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")

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
