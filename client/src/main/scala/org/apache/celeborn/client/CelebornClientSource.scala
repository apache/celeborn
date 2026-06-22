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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}

/**
 * Metrics source for the Celeborn client
 */
class CelebornClientSource(conf: CelebornConf) extends AbstractSource(conf, Role.CLIENT) {
  override val sourceName = "client"

  import CelebornClientSource._

  addCounter(REGISTER_SHUFFLE_COUNT)
  addCounter(REGISTER_SHUFFLE_FAIL_COUNT)
  addCounter(UNREGISTER_SHUFFLE_COUNT)
  addCounter(REVIVE_REQUEST_COUNT)
  addCounter(REVIVE_FAIL_COUNT)
  addCounter(SLOT_RESERVATION_FAIL_COUNT)
  addCounter(SHUFFLE_FETCH_FAILURE_COUNT)
  addCounter(SHUFFLE_DATA_LOST_COUNT)

  def getMetricsSnapshot(): Map[String, ClientMetric] = {
    val counterMetrics = counters().map(c =>
      c.name -> ClientMetric(c.counter.getCount, MetricType.Counter))
    val gaugeMetrics = gauges().map(g =>
      g.name -> ClientMetric(g.gauge.getValue.asInstanceOf[Number].longValue(), MetricType.Gauge))
    (counterMetrics ++ gaugeMetrics).toMap
  }

  // start cleaner thread
  startCleaner()
}

object CelebornClientSource {
  // worker health
  val EXCLUDED_WORKER_COUNT = "ClientExcludedWorkerCount"
  val SHUTTING_WORKER_COUNT = "ClientShuttingWorkerCount"

  // shuffle lifecycle
  val ACTIVE_SHUFFLE_COUNT = "ClientActiveShuffleCount"
  val REGISTER_SHUFFLE_COUNT = "ClientRegisterShuffleCount"
  val REGISTER_SHUFFLE_FAIL_COUNT = "ClientRegisterShuffleFailCount"
  val UNREGISTER_SHUFFLE_COUNT = "ClientUnregisterShuffleCount"

  // write path
  val REVIVE_REQUEST_COUNT = "ClientReviveRequestCount"
  val REVIVE_FAIL_COUNT = "ClientReviveFailCount"
  val SLOT_RESERVATION_FAIL_COUNT = "ClientSlotReservationFailCount"

  // data integrity
  val SHUFFLE_FETCH_FAILURE_COUNT = "ClientShuffleFetchFailureCount"
  val SHUFFLE_DATA_LOST_COUNT = "ClientShuffleDataLostCount"
}
