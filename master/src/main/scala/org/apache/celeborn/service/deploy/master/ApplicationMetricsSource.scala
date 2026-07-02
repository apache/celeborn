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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.{ClientMetric, MetricType}
import org.apache.celeborn.common.metrics.source.{AbstractSource, Role}
import org.apache.celeborn.common.util.JavaUtils

/**
 * Holds the client-side metrics that applications report in their heartbeat and re-exposes them on
 * the master's Prometheus endpoint, labeled by `applicationId`. Both the registrations and any
 * cached state are dropped when the application is terminated.
 */
class ApplicationMetricsSource(conf: CelebornConf)
  extends AbstractSource(conf, Role.MASTER) with Logging {
  override val sourceName = "application"

  // applicationId -> (metricName -> latest reported gauge value)
  private val appGaugeCache =
    JavaUtils.newConcurrentHashMap[String, ConcurrentHashMap[String, java.lang.Long]]()

  // applicationId -> (metricName -> last reported counter value, used to compute deltas)
  private val appCounterPrev =
    JavaUtils.newConcurrentHashMap[String, ConcurrentHashMap[String, java.lang.Long]]()

  startCleaner()

  def updateApplicationMetrics(appId: String, metrics: JMap[String, ClientMetric]): Unit = {
    if (metrics.isEmpty) return
    metrics.asScala.foreach { case (name, metric) =>
      val labels = Map(applicationLabel -> appId)
      metric.metricType match {
        case MetricType.Gauge => updateGauge(appId, name, labels, metric.value)
        case MetricType.Counter => updateCounter(appId, name, labels, metric.value)
      }
    }
  }

  private def updateGauge(
      appId: String,
      name: String,
      labels: Map[String, String],
      value: Long): Unit = {
    val cache = appGaugeCache.computeIfAbsent(appId, _ => JavaUtils.newConcurrentHashMap())
    cache.put(name, value)
    if (!gaugeExists(name, labels)) {
      addGauge(name, labels) { () =>
        Option(appGaugeCache.get(appId))
          .flatMap(m => Option(m.get(name)))
          .map(_.longValue())
          .getOrElse(0L)
      }
    }
  }

  private def updateCounter(
      appId: String,
      name: String,
      labels: Map[String, String],
      newValue: Long): Unit = {
    val prev = appCounterPrev.computeIfAbsent(appId, _ => JavaUtils.newConcurrentHashMap())
    if (!counterExists(name, labels)) {
      addCounter(name, labels)
    }
    val prevValue = prev.getOrDefault(name, 0L)
    val delta = newValue - prevValue
    if (delta > 0) {
      incCounter(name, delta, labels)
    }
    prev.put(name, newValue)
  }

  def removeApplicationMetrics(appId: String): Unit = {
    val labels = Map(applicationLabel -> appId)
    val gaugeCache = appGaugeCache.remove(appId)
    if (gaugeCache != null) {
      gaugeCache.keySet().asScala.foreach(name => removeGauge(name, labels))
    }
    val counterPrev = appCounterPrev.remove(appId)
    if (counterPrev != null) {
      counterPrev.keySet().asScala.foreach(name => removeCounter(name, labels))
    }
  }
}
