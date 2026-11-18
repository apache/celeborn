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
  import CelebornClientSource._

  override val sourceName = "client"

  Seq(
    REGISTERED_SHUFFLE_COUNT,
    REVIVE_COUNT,
    SHUFFLE_FETCH_FAILURE_COUNT,
    STAGE_RERUN_COUNT,
    SHUFFLE_DATA_LOST_COUNT,
    RESERVE_SLOTS_FAILURE_COUNT,
    REQUEST_SLOTS_FAILURE_COUNT).foreach(name => addCounter(name))

  def getMetricsSnapshot(): Map[String, ClientMetric] = {
    val snapshot = Map.newBuilder[String, ClientMetric]
    gauges().foreach { g =>
      snapshot += g.name -> ClientMetric(
        g.gauge.getValue.asInstanceOf[Number].longValue(),
        MetricType.Gauge)
    }
    counters().foreach { c =>
      snapshot += c.name -> ClientMetric(c.counter.getCount, MetricType.Counter)
    }
    snapshot.result()
  }

  def stop(): Unit = metricsCleaner.shutdown()
}

object CelebornClientSource {
  val EXCLUDED_WORKER_COUNT = "ClientExcludedWorkerCount"
  val SHUTTING_WORKER_COUNT = "ClientShuttingWorkerCount"
  val ACTIVE_SHUFFLE_COUNT = "ClientActiveShuffleCount"

  val REGISTERED_SHUFFLE_COUNT = "ClientRegisteredShuffleCount"
  val REVIVE_COUNT = "ClientReviveCount"
  val SHUFFLE_FETCH_FAILURE_COUNT = "ClientShuffleFetchFailureCount"
  val STAGE_RERUN_COUNT = "ClientStageRerunCount"
  val SHUFFLE_DATA_LOST_COUNT = "ClientShuffleDataLostCount"
  val RESERVE_SLOTS_FAILURE_COUNT = "ClientReserveSlotsFailureCount"
  val REQUEST_SLOTS_FAILURE_COUNT = "ClientRequestSlotsFailureCount"
}
