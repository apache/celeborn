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

package org.apache.celeborn.common.metrics.source

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class CelebornSourceSuite extends CelebornFunSuite {

  private class TestSource extends AbstractSource(new CelebornConf(), Role.MASTER) {
    override def sourceName: String = "testSource"

    def updateGauge(
        name: String,
        labels: Map[String, String],
        appId: String,
        value: Long): Unit =
      addOrUpdateGaugeForApp(name, labels, appId, value)

    def updateCounter(
        name: String,
        labels: Map[String, String],
        appId: String,
        delta: Long): Unit =
      addOrUpdateCounterForApp(name, labels, appId, delta)

    def removeApp(appId: String): Unit = removeAppFromMetrics(appId)

    def trackedGaugeCount: Int = namedGaugesWithDetails.size()

    def trackedCounterCount: Int = namedCountersWithDetails.size()
  }

  private def hasLabels(
      actual: Map[String, String],
      expected: Map[String, String]): Boolean =
    expected.forall { case (key, value) => actual.get(key).contains(value) }

  private def gaugeValue(
      source: AbstractSource,
      labels: Map[String, String],
      name: String): Option[Long] =
    source.gauges()
      .find(g => g.name == name && hasLabels(g.labels, labels))
      .map(_.gauge.getValue.asInstanceOf[Number].longValue())

  private def counterValue(
      source: AbstractSource,
      labels: Map[String, String],
      name: String): Option[Long] =
    source.counters()
      .find(c => c.name == name && hasLabels(c.labels, labels))
      .map(_.counter.getCount)

  test("test histogram") {
    val conf = new CelebornConf()

    val mockSource = new AbstractSource(conf, Role.WORKER) {
      override def sourceName: String = "mockSource"
    }
    val histogram = "abc"
    mockSource.addHistogram(histogram)
    for (i <- 1 to 100) {
      mockSource.updateHistogram(histogram, 10)
    }
    val res = mockSource.getMetrics

    assert(res.contains("metrics_abc_Count"))
  }

  test("test getMetrics with customized label") {
    val conf = new CelebornConf()
    createAbstractSourceAndCheck(conf, "", Role.MASTER)
    createAbstractSourceAndCheck(conf, "", Role.WORKER)
  }

  def createAbstractSourceAndCheck(
      conf: CelebornConf,
      extraLabels: String,
      role: String = "mock"): Unit = {
    val mockSource = new AbstractSource(conf, role) {
      override def sourceName: String = "mockSource"
    }
    val user1 = Map("user" -> "user1")
    val user2 = Map("user" -> "user2")
    val user3 = Map("user" -> "user3")
    mockSource.addGauge("Gauge1") { () => 1000 }
    mockSource.addGauge("Gauge2", user1) { () => 2000 }
    mockSource.addCounter("Counter1")
    mockSource.addCounter("Counter2", user2)
    // test operation with and without label
    mockSource.incCounter("Counter1", 3000)
    mockSource.incCounter("Counter2", 4000, user2)
    mockSource.addTimer("Timer1")
    mockSource.addTimer("Timer2", user3)
    // ditto
    mockSource.startTimer("Timer1", "key1")
    mockSource.startTimer("Timer2", "key2", user3)
    Thread.sleep(10)
    mockSource.stopTimer("Timer1", "key1")
    mockSource.stopTimer("Timer2", "key2", user3)

    val res = mockSource.getMetrics
    var extraLabelsStr = extraLabels
    if (extraLabels.nonEmpty) {
      extraLabelsStr = extraLabels + ","
    }
    val instanceLabelStr =
      mockSource.instanceLabel.map(kv => s"""${kv._1}="${kv._2}",""").mkString(",")
    val exp1 = s"""metrics_Gauge1_Value{${extraLabelsStr}${instanceLabelStr}role="$role"} 1000"""
    val exp2 =
      s"""metrics_Gauge2_Value{${extraLabelsStr}${instanceLabelStr}role="$role",user="user1"} 2000"""
    val exp3 = s"""metrics_Counter1_Count{${extraLabelsStr}${instanceLabelStr}role="$role"} 3000"""
    val exp4 =
      s"""metrics_Counter2_Count{${extraLabelsStr}${instanceLabelStr}role="$role",user="user2"} 4000"""
    val exp5 = s"""metrics_Timer1_Count{${extraLabelsStr}${instanceLabelStr}role="$role"} 1"""
    val exp6 =
      s"""metrics_Timer2_Count{${extraLabelsStr}${instanceLabelStr}role="$role",user="user3"} 1"""

    assert(res.contains(exp1))
    assert(res.contains(exp2))
    assert(res.contains(exp3))
    assert(res.contains(exp4))
    assert(res.contains(exp5))
    assert(res.contains(exp6))
  }

  test("test getMetrics with customized label by conf") {
    val conf = new CelebornConf()
    // label's is normal
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "l1=v1,l2=v2,l3=v3")
    val extraLabels = """l1="v1",l2="v2",l3="v3""""
    createAbstractSourceAndCheck(conf, extraLabels)

    // labels' kv not correct
    assertThrows[IllegalArgumentException] {
      conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "l1=v1,l2=")
      val extraLabels2 = """l1="v1",l2="v2",l3="v3""""
      createAbstractSourceAndCheck(conf, extraLabels2)
    }

    // there are spaces in labels
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, " l1 = v1, l2  =v2  ,l3 =v3  ")
    val extraLabels3 = """l1="v1",l2="v2",l3="v3""""
    createAbstractSourceAndCheck(conf, extraLabels3)

  }

  test("test customized labels override extra labels") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "user=extra,role=extra,instance=extra")
    val source = new AbstractSource(conf, Role.MASTER) {
      override def sourceName: String = "mockSource"
    }
    val labels = Map("user" -> "metric")
    source.addCounter("Counter", labels)
    source.incCounter("Counter", 1, labels)
    conf.set(CelebornConf.METRICS_EXTRA_LABELS.key, "user=changed")
    source.incCounter("Counter", 1, labels)

    val metrics = source.getMetrics
    val instance = source.instanceLabel("instance")
    assert(metrics.contains(
      s"""metrics_Counter_Count{instance="$instance",role="Master",user="metric"} 2"""))
  }

  test("dynamic app gauge reuses existing gauge registration and updates value") {
    val source = new TestSource()
    val labels = Map("user" -> "metric")

    source.updateGauge("DynamicGauge", labels, "app-1", 1L)
    source.updateGauge("DynamicGauge", labels, "app-1", 42L)

    assert(source.trackedGaugeCount == 1)
    assert(source.gauges().count(g => g.name == "DynamicGauge" && hasLabels(g.labels, labels)) == 1)
    assert(gaugeValue(source, labels, "DynamicGauge").contains(42L))
    assert(source.getMetrics.contains("""metrics_DynamicGauge_Value"""))
    assert(source.getMetrics.contains("""user="metric""""))
  }

  test("dynamic app counter reuses existing counter registration and accumulates deltas") {
    val source = new TestSource()
    val labels = Map("user" -> "metric")

    source.updateCounter("DynamicCounter", labels, "app-1", 10L)
    source.updateCounter("DynamicCounter", labels, "app-1", 5L)
    source.updateCounter("DynamicCounter", labels, "app-1", 0L)
    source.updateCounter("DynamicCounter", labels, "app-1", -1L)

    assert(source.trackedCounterCount == 1)
    assert(source.counters().count(c =>
      c.name == "DynamicCounter" && hasLabels(c.labels, labels)) == 1)
    assert(counterValue(source, labels, "DynamicCounter").contains(15L))
  }

  test("dynamic app metrics are removed only after all contributing apps are removed") {
    val source = new TestSource()
    val labels = Map("user" -> "metric")

    source.updateGauge("DynamicGauge", labels, "app-1", 1L)
    source.updateGauge("DynamicGauge", labels, "app-2", 2L)
    source.updateCounter("DynamicCounter", labels, "app-1", 10L)
    source.updateCounter("DynamicCounter", labels, "app-2", 5L)

    source.removeApp("app-1")

    assert(source.trackedGaugeCount == 1)
    assert(source.trackedCounterCount == 1)
    assert(gaugeValue(source, labels, "DynamicGauge").contains(2L))
    assert(counterValue(source, labels, "DynamicCounter").contains(15L))

    source.removeApp("app-2")

    assert(source.trackedGaugeCount == 0)
    assert(source.trackedCounterCount == 0)
    assert(gaugeValue(source, labels, "DynamicGauge").isEmpty)
    assert(counterValue(source, labels, "DynamicCounter").isEmpty)
    assert(!source.gaugeExists("DynamicGauge", labels))
    assert(!source.counterExists("DynamicCounter", labels))
  }
}
