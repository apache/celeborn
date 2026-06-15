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

package org.apache.celeborn.service.deploy.master

import java.util.{HashMap => JHashMap}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class ApplicationMetricsSourceSuite extends CelebornFunSuite {

  private def metricsOf(app: String, value: Long): JHashMap[String, java.lang.Long] = {
    val map = new JHashMap[String, java.lang.Long]()
    map.put("ClientRegisterShuffleCount", java.lang.Long.valueOf(value))
    map
  }

  test("client metrics are emitted as gauges labeled by applicationId") {
    val source = new ApplicationMetricsSource(new CelebornConf())

    source.updateApplicationMetrics("app-1", metricsOf("app-1", 3))
    source.updateApplicationMetrics("app-2", metricsOf("app-2", 7))

    val metrics = source.getMetrics
    assert(metrics.contains("metrics_ClientRegisterShuffleCount_Value"))
    assert(metrics.contains("""applicationId="app-1""""))
    assert(metrics.contains("""applicationId="app-2""""))
    assert(metrics.contains("""role="Master""""))
  }

  test("latest reported value is reflected and removed on application lost") {
    val source = new ApplicationMetricsSource(new CelebornConf())

    source.updateApplicationMetrics("app-1", metricsOf("app-1", 1))
    source.updateApplicationMetrics("app-1", metricsOf("app-1", 42))
    assert(source.gauges().exists(g =>
      g.labels.get("applicationId").contains("app-1") &&
        g.gauge.getValue.asInstanceOf[Number].longValue() == 42L))

    source.removeApplicationMetrics("app-1")
    assert(!source.gauges().exists(_.labels.get("applicationId").contains("app-1")))
  }

  test("empty metrics map registers nothing") {
    val source = new ApplicationMetricsSource(new CelebornConf())
    source.updateApplicationMetrics("app-1", new JHashMap[String, java.lang.Long]())
    assert(source.gauges().isEmpty)
  }
}
