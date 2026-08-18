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

package org.apache.celeborn.client

import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricType

class CelebornClientSourceSuite extends CelebornFunSuite {
  test("gauges registered on the source are reflected in metrics and snapshot") {
    val source = new CelebornClientSource(new CelebornConf())
    val excluded = new AtomicInteger(0)

    source.addGauge(CelebornClientSource.EXCLUDED_WORKER_COUNT) { () => excluded.get() }

    assert(source.getMetricsSnapshot()(CelebornClientSource.EXCLUDED_WORKER_COUNT).value == 0)

    excluded.set(5)
    val metrics = source.getMetrics
    assert(metrics.contains("metrics_ClientExcludedWorkerCount_Value"))
    val snapshot = source.getMetricsSnapshot()
    assert(snapshot(CelebornClientSource.EXCLUDED_WORKER_COUNT).value == 5)
    assert(snapshot(CelebornClientSource.EXCLUDED_WORKER_COUNT).metricType == MetricType.Gauge)
  }
}
