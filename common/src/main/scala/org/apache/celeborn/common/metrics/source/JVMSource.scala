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

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.codahale.metrics.jvm.{BufferPoolMetricSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet, ThreadStatesGaugeSet}

import org.apache.celeborn.common.CelebornConf

class JVMSource(conf: CelebornConf, role: String) extends AbstractSource(conf, role) {
  override val sourceName = "JVM"

  import JVMSource._

  private val gcNames = ManagementFactory.getGarbageCollectorMXBeans.asScala.map(bean =>
    WHITESPACE.matcher(bean.getName).replaceAll("-"))
  private val poolNames = ManagementFactory.getMemoryPoolMXBeans.asScala.map(bean =>
    WHITESPACE.matcher(bean.getName).replaceAll("-"))

  /**
   * Add jvm metric prefix, remove pool info from name and obtain the pool name as labels if needed
   * @param metricName metric name from MetricSet
   * @param targets keywords need to be replaced
   * @param prefix prefix for new metric name
   * @param replacement replacement for pool name
   * @return new metric without target, labels if exists
   */
  def handleJVMMetricName(
      metricName: String,
      targets: mutable.Buffer[String],
      prefix: String,
      replacement: String): (String, Map[String, String]) = {
    for (target <- targets) {
      if (metricName.contains(target)) {
        val labels = Map("name" -> target)
        var replaceTarget = target
        if (replacement.isEmpty) {
          replaceTarget = target + "."
        }
        return (MetricRegistry.name(prefix, metricName.replace(replaceTarget, replacement)), labels)
      }
    }
    (MetricRegistry.name(prefix, metricName), Map.empty[String, String])
  }

  // all metrics in MetricSet are gauges
  Seq(new GarbageCollectorMetricSet()).map(_.getMetrics.asScala.map {
    case (name: String, metric: Gauge[_]) =>
      val newMetrics = handleJVMMetricName(name, gcNames, JVM_METRIC_PREFIX, "gc")
      addGauge(newMetrics._1, newMetrics._2, metric)
    case (name, metric) => new IllegalArgumentException(s"Unknown metric type: $name: $metric")
  })

  Seq(new MemoryUsageGaugeSet()).map(_.getMetrics.asScala.map {
    case (name: String, metric: Gauge[_]) =>
      val newMetrics = handleJVMMetricName(name, poolNames, JVM_METRIC_MEMORY_PREFIX, "")
      addGauge(newMetrics._1, newMetrics._2, metric)
    case (name, metric) => new IllegalArgumentException(s"Unknown metric type: $name: $metric")
  })

  Seq(
    new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer)).map(
    _.getMetrics.asScala.map {
      case (name: String, metric: Gauge[_]) =>
        addGauge(MetricRegistry.name(JVM_METRIC_PREFIX, name), metric)
      case (name, metric) => new IllegalArgumentException(s"Unknown metric type: $name: $metric")
    })

  Seq(new ThreadStatesGaugeSet()).map(_.getMetrics.asScala.map {
    case (name: String, metric: Gauge[_]) =>
      addGauge(MetricRegistry.name(JVM_METRIC_THREAD_PREFIX, name), metric)
    case (name, metric) => new IllegalArgumentException(s"Unknown metric type: $name: $metric")
  })

  // start cleaner
  startCleaner()
}

object JVMSource {
  private val JVM_METRIC_PREFIX = "jvm"
  private val JVM_METRIC_MEMORY_PREFIX = JVM_METRIC_PREFIX + ".memory"
  private val JVM_METRIC_THREAD_PREFIX = JVM_METRIC_PREFIX + ".thread"

  private val WHITESPACE = "\\s+".r.pattern
}
