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

  private def newServlet(source: AbstractSource): PrometheusServlet = {
    new PrometheusServlet(
      new Properties(),
      new MetricRegistry(),
      Seq(source),
      "/metrics/prometheus")
  }

  test("getMetricsSnapshot deduplicates # HELP and # TYPE lines per metric family") {
    val conf = new CelebornConf()
    val source = new AbstractSource(conf, Role.WORKER) {
      override def sourceName: String = "mockSource"
    }

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
    assert(lines.exists(_ == s"# TYPE $counterName counter"))
    assert(lines.count(_.startsWith(s"$counterName{")) == 2)
    assert(snapshot.contains("""user="user1""""))
    assert(snapshot.contains("""user="user2""""))
    assert(lines.count(_ == s"# HELP $gaugeName") == 1)
    assert(lines.count(_ == s"# TYPE $gaugeName gauge") == 1)
    assert(lines.exists(_.startsWith(s"$gaugeName")) && snapshot.contains("1000"))
  }
}
