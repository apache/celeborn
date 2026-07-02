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

  test("counters are declared, increment, and emit with role=Client") {
    val source = new CelebornClientSource(new CelebornConf())

    source.incCounter(CelebornClientSource.REGISTER_SHUFFLE_COUNT)
    source.incCounter(CelebornClientSource.REGISTER_SHUFFLE_COUNT)
    source.incCounter(CelebornClientSource.REGISTER_SHUFFLE_FAIL_COUNT)
    source.incCounter(CelebornClientSource.UNREGISTER_SHUFFLE_COUNT, 3)
    source.incCounter(CelebornClientSource.REVIVE_REQUEST_COUNT, 5)
    source.incCounter(CelebornClientSource.REVIVE_FAIL_COUNT, 2)
    source.incCounter(CelebornClientSource.SLOT_RESERVATION_FAIL_COUNT)
    source.incCounter(CelebornClientSource.SHUFFLE_FETCH_FAILURE_COUNT)
    source.incCounter(CelebornClientSource.SHUFFLE_DATA_LOST_COUNT)

    val metrics = source.getMetrics
    assert(metrics.contains("""metrics_ClientRegisterShuffleCount_Count"""))
    assert(metrics.contains("""role="Client""""))

    val snapshot = source.getMetricsSnapshot()
    assert(snapshot(CelebornClientSource.REGISTER_SHUFFLE_COUNT).value == 2)
    assert(snapshot(CelebornClientSource.REGISTER_SHUFFLE_COUNT).metricType == MetricType.Counter)
    assert(snapshot(CelebornClientSource.REGISTER_SHUFFLE_FAIL_COUNT).value == 1)
    assert(snapshot(CelebornClientSource.UNREGISTER_SHUFFLE_COUNT).value == 3)
    assert(snapshot(CelebornClientSource.REVIVE_REQUEST_COUNT).value == 5)
    assert(snapshot(CelebornClientSource.REVIVE_FAIL_COUNT).value == 2)
    assert(snapshot(CelebornClientSource.SLOT_RESERVATION_FAIL_COUNT).value == 1)
    assert(snapshot(CelebornClientSource.SHUFFLE_FETCH_FAILURE_COUNT).value == 1)
    assert(snapshot(CelebornClientSource.SHUFFLE_DATA_LOST_COUNT).value == 1)
  }

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

  test("getMetricsSnapshot includes both counters and gauges with correct types") {
    val source = new CelebornClientSource(new CelebornConf())
    source.addGauge(CelebornClientSource.ACTIVE_SHUFFLE_COUNT) { () => 7 }
    source.incCounter(CelebornClientSource.REGISTER_SHUFFLE_COUNT)

    val snapshot = source.getMetricsSnapshot()
    assert(snapshot.contains(CelebornClientSource.REGISTER_SHUFFLE_COUNT))
    assert(snapshot(CelebornClientSource.REGISTER_SHUFFLE_COUNT).metricType == MetricType.Counter)
    assert(snapshot.contains(CelebornClientSource.REGISTER_SHUFFLE_FAIL_COUNT))
    assert(snapshot.contains(CelebornClientSource.UNREGISTER_SHUFFLE_COUNT))
    assert(snapshot.contains(CelebornClientSource.REVIVE_REQUEST_COUNT))
    assert(snapshot.contains(CelebornClientSource.REVIVE_FAIL_COUNT))
    assert(snapshot.contains(CelebornClientSource.SLOT_RESERVATION_FAIL_COUNT))
    assert(snapshot.contains(CelebornClientSource.SHUFFLE_FETCH_FAILURE_COUNT))
    assert(snapshot.contains(CelebornClientSource.SHUFFLE_DATA_LOST_COUNT))
    assert(snapshot(CelebornClientSource.ACTIVE_SHUFFLE_COUNT).value == 7)
    assert(snapshot(CelebornClientSource.ACTIVE_SHUFFLE_COUNT).metricType == MetricType.Gauge)
  }
}
