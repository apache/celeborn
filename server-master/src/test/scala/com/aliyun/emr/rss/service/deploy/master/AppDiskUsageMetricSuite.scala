package com.aliyun.emr.rss.service.deploy.master

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.service.deploy.master.metrics.{AppDiskUsageMetric, AppDiskUsageSnapShot}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.util.Random

class AppDiskUsageMetricSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with Logging {
  val WORKER1 = new WorkerInfo("host1", 111, 112, 113, 114)
  val WORKER2 = new WorkerInfo("host2", 211, 212, 213, 214)
  val WORKER3 = new WorkerInfo("host3", 311, 312, 313, 314)

  test("test snapshot ordering") {
    val snapShot = new AppDiskUsageSnapShot(50)
    val rand = new Random()
    for (i <- 1 to 60) {
      snapShot.updateAppDiskUsage(s"app-${i}", rand.nextInt(100000000) + 1)
    }
    println(snapShot.toString)
  }

  test("test snapshot ordering with duplicate entries") {
    val snapShot = new AppDiskUsageSnapShot(50)
    val rand = new Random()
    for (i <- 1 to 60) {
      snapShot.updateAppDiskUsage(s"app-${i}", rand.nextInt(100000000) + 1)
    }
    for (i <- 1 to 15) {
      snapShot.updateAppDiskUsage(s"app-${i}", rand.nextInt(100000000) + 1000000000)
    }

    println(snapShot.toString)
  }

  test("test app usage snapshot") {
    Thread.sleep(5000)

    val conf = new RssConf()
    conf.set("rss.metrics.app.topDiskUsage.windowSize", "5")
    conf.set("rss.metrids.app.topDiskUsage.interval", "2s")
    val usageMetric = new AppDiskUsageMetric(conf)

    val map1 = new util.HashMap[WorkerInfo, util.Map[String, java.lang.Long]]()
    val worker1Map = new util.HashMap[String, java.lang.Long]()
    val worker2Map = new util.HashMap[String, java.lang.Long]()
    val worker3Map = new util.HashMap[String, java.lang.Long]()
    worker1Map.put("app1-1", 2874371)
    worker1Map.put("app1-2", 4524)
    worker1Map.put("app1-3", 43452)
    worker2Map.put("app2-1", 2134526)
    worker2Map.put("app2-1", 4526)
    worker2Map.put("app1-1", 23465463)
    worker3Map.put("app1-1", 132456)
    worker3Map.put("app3-1", 43254)
    worker3Map.put("app4-1", 6535635)
    map1.put(WORKER1, worker1Map)
    map1.put(WORKER2, worker2Map)
    map1.put(WORKER3, worker3Map)
    usageMetric.update(map1)
    println(usageMetric.summary())
    Thread.sleep(2000)

    map1.clear()
    worker1Map.clear()
    worker2Map.clear()
    worker3Map.clear()
    worker1Map.put("app1-1", 23523450)
    worker1Map.put("app1-2", 3231453)
    worker1Map.put("app1-3", 2345645)
    worker2Map.put("app2-1", 12324143)
    worker2Map.put("app2-1", 23454)
    worker2Map.put("app5-1", 234235613)
    worker3Map.put("app1-1", 1234454)
    worker3Map.put("app3-1", 43532)
    worker3Map.put("app4-1", 134345213)
    map1.put(WORKER1, worker1Map)
    map1.put(WORKER2, worker2Map)
    map1.put(WORKER3, worker3Map)
    usageMetric.update(map1)
    println(usageMetric.summary())
    Thread.sleep(2000)

    map1.clear()
    worker1Map.clear()
    worker2Map.clear()
    worker3Map.clear()
    worker1Map.put("app1-1", 82352345)
    worker1Map.put("app1-2", 7253423)
    worker1Map.put("app1-3", 42345645)
    worker2Map.put("app2-1", 12324143)
    worker2Map.put("app2-1", 563456)
    worker2Map.put("app5-1", 2341343267L)
    worker3Map.put("app1-1", 971234454)
    worker3Map.put("app3-1", 32443532L)
    worker3Map.put("app4-1", 8734345213L)
    map1.put(WORKER1, worker1Map)
    map1.put(WORKER2, worker2Map)
    map1.put(WORKER3, worker3Map)
    usageMetric.update(map1)
    println(usageMetric.summary())
    Thread.sleep(2500)
    println(usageMetric.summary())
  }
}
