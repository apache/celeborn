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

package org.apache.celeborn.common.metrics.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}

class PrometheusServletSuite extends CelebornFunSuite {

  private def newServlet(sources: AbstractSource*): PrometheusServlet = {
    new PrometheusServlet(
      new Properties(),
      new MetricRegistry(),
      sources,
      "/metrics/prometheus")
  }

  private def newSource(conf: CelebornConf): AbstractSource = {
    new AbstractSource(conf, Role.WORKER) {
      override def sourceName: String = "mockSource"
    }
  }

  test("getMetricsSnapshot deduplicates # HELP and # TYPE lines per metric family") {
    val conf = new CelebornConf()
    val source = newSource(conf)

    val user1 = Map("user" -> "user1")
    val user2 = Map("user" -> "user2")
    source.addCounter("Counter", user1)
    source.addCounter("Counter", user2)
    source.incCounter("Counter", 1, user1)
    source.incCounter("Counter", 2, user2)

    source.addGauge("Gauge1") { () => 1000 }

    val snapshot = newServlet(source).getMetricsSnapshot
    val lines = snapshot.linesIterator.toList

    val counterName = "metrics_Counter_Count"
    val gaugeName = "metrics_Gauge1_Value"

    assert(lines.count(_ == s"# HELP $counterName") == 1)
    assert(lines.count(_ == s"# TYPE $counterName counter") == 1)
    assert(lines.count(_.startsWith(s"$counterName{")) == 2)
    assert(snapshot.contains("""user="user1""""))
    assert(snapshot.contains("""user="user2""""))
    assert(lines.count(_ == s"# HELP $gaugeName") == 1)
    assert(lines.count(_ == s"# TYPE $gaugeName gauge") == 1)
    val gaugeSamples = lines.filter(_.startsWith(gaugeName))
    assert(gaugeSamples.size == 1)
    assert(gaugeSamples.head.split(" ")(1) == "1000")
  }

  test("getMetricsSnapshot groups samples of a metric family that are interleaved") {
    val conf = new CelebornConf()
    val user1 = Map("user" -> "user1")
    val user2 = Map("user" -> "user2")
    // Sources emit gauges before counters, so the raw snapshot is
    // Counter{user1}, Gauge1, Counter{user2}.
    val source1 = newSource(conf)
    source1.addCounter("Counter", user1)
    val source2 = newSource(conf)
    source2.addGauge("Gauge1") { () => 1000 }
    source2.addCounter("Counter", user2)

    val servlet = newServlet(source1, source2)
    val counterName = "metrics_Counter_Count"
    val gaugeName = "metrics_Gauge1_Value"
    val rawLines = servlet.sources.map(_.getMetrics).mkString.linesIterator.toList
    assert(rawLines.indexWhere(_.startsWith(gaugeName)) <
      rawLines.lastIndexWhere(_.startsWith(s"$counterName{")))

    val lines = servlet.getMetricsSnapshot.linesIterator.toList
    val counterStart = lines.indexOf(s"# HELP $counterName")
    assert(lines(counterStart + 1) == s"# TYPE $counterName counter")
    val counterSamples = lines.slice(counterStart + 2, counterStart + 4)
    assert(counterSamples.forall(_.startsWith(s"$counterName{")))
    assert(counterSamples.exists(_.contains("user=\"user1\"")))
    assert(counterSamples.exists(_.contains("user=\"user2\"")))
    assert(lines.count(_.startsWith(s"$counterName{")) == 2)
    val gaugeStart = lines.indexOf(s"# HELP $gaugeName")
    assert(lines(gaugeStart + 1) == s"# TYPE $gaugeName gauge")
    assert(lines(gaugeStart + 2).startsWith(s"$gaugeName{"))
  }
}
