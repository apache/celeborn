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

import java.util.{Map => JMap}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}
import org.apache.celeborn.common.util.{JavaUtils, Utils}

class ApplicationMetricsSource(conf: CelebornConf)
  extends AbstractSource(conf, Role.MASTER) with Logging {
  override val sourceName = "application"

  private val masterClientMetricsEnabled = conf.masterClientMetricsEnabled
  private val removedAppRetentionMs = conf.masterClientMetricsRemovedAppRetentionMs

  // Tracking applications that have been terminated
  private val removedAppIds =
    JavaUtils.newConcurrentHashMap[String, java.lang.Long]()

  private val seriesCardinalityWarnThreshold =
    conf.masterClientMetricsSeriesCardinalityWarnThreshold
  private val seriesCardinalityWarned = new AtomicBoolean(false)

  if (masterClientMetricsEnabled) {
    startRemovedAppCleaner()
  }

  private def startRemovedAppCleaner(): Unit = {
    val cleanTask: Runnable = new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val cutoff = System.currentTimeMillis() - removedAppRetentionMs
        removedAppIds.entrySet().asScala.foreach { entry =>
          if (entry.getValue < cutoff) {
            removedAppIds.remove(entry.getKey, entry.getValue)
          }
        }
      }
    }
    metricsCleaner.scheduleWithFixedDelay(
      cleanTask,
      removedAppRetentionMs,
      removedAppRetentionMs,
      TimeUnit.MILLISECONDS)
  }

  def updateApplicationMetrics(
      appId: String,
      metricLabels: Map[String, String],
      metrics: JMap[String, ClientMetric]): Unit = {
    if (!masterClientMetricsEnabled || metricLabels.isEmpty) {
      return
    }

    if (removedAppIds.containsKey(appId)) {
      return
    }

    metrics.asScala.foreach { case (name, metric) =>
      metric.metricType match {
        case MetricType.Gauge =>
          addOrUpdateGaugeForApp(name, metricLabels, appId, metric.value)
        case MetricType.Counter =>
          addOrUpdateCounterForApp(name, metricLabels, appId, metric.value)
      }
    }

    if (removedAppIds.containsKey(appId)) {
      removeAppFromMetrics(appId)
    }

    warnIfSeriesCardinalityHigh()
  }

  def removeApplicationMetrics(appId: String): Unit = {
    if (masterClientMetricsEnabled) {
      removedAppIds.put(appId, System.currentTimeMillis())
    }
    removeAppFromMetrics(appId)
  }

  private def warnIfSeriesCardinalityHigh(): Unit = {
    val trackedSeries = gauges().size + counters().size
    if (trackedSeries > seriesCardinalityWarnThreshold && seriesCardinalityWarned.compareAndSet(
        false,
        true)) {
      logWarning(
        s"Client metrics are tracking $trackedSeries distinct series, exceeding " +
          s"$seriesCardinalityWarnThreshold. Client metric series are keyed by " +
          s"'${CelebornConf.CLIENT_METRICS_APP_LABELS.key}' and are only reclaimed when an " +
          "application is lost, so high-cardinality labels can grow memory without bound. " +
          "Ensure these labels are low-cardinality (e.g. env/team), not per-application values.")
    }
  }
}
