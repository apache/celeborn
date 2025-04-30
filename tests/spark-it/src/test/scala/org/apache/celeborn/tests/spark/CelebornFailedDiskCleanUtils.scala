package org.apache.celeborn.tests.spark

import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.spark.FailedShuffleCleaner

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CelebornFailedDiskCleanUtils extends SparkTestBase {
  test("test correctness of RunningStageManager") {
    val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
    val sparkSession = SparkSession.builder()
      .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
      .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
      .getOrCreate()
    val t = new Thread {
      override def run(): Unit = {
        try {
          sparkSession.sparkContext.parallelize(List(1, 2, 3)).foreach(_ =>
            Thread.sleep(60 * 1000))
        } catch {
          case _: Throwable =>
          // swallow everything
        }
      }
    }
    t.start()
    Thread.sleep(20 * 1000)
    assert(FailedShuffleCleaner.runningStageManager.isRunningStage(0))
    sparkSession.stop()
  }
}
