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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import com.codahale.metrics._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.{ResettableSlidingWindowReservoir, RssHistogram, RssTimer}
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

case class NamedCounter(name: String, counter: Counter)

case class NamedGauge[T](name: String, gauge: Gauge[T])

case class NamedHistogram(name: String, histogram: Histogram)

case class NamedTimer(name: String, timer: Timer)

abstract class AbstractSource(conf: CelebornConf, role: String)
  extends Source with Logging {
  override val metricRegistry = new MetricRegistry()

  val metricsSlidingWindowSize: Int = conf.metricsSlidingWindowSize

  val metricsSampleRate: Double = conf.metricsSampleRate

  val metricsCollectCriticalEnabled: Boolean = conf.metricsCollectCriticalEnabled

  final val metricsCapacity = conf.metricsCapacity

  val innerMetrics: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()

  val timerSupplier = new TimerSupplier(metricsSlidingWindowSize)

  val metricsCleaner: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"worker-metrics-cleaner")

  protected val namedGauges: java.util.List[NamedGauge[_]] =
    new java.util.ArrayList[NamedGauge[_]]()

  def addGauge[T](name: String, f: Unit => T): Unit = {
    val supplier: MetricRegistry.MetricSupplier[Gauge[_]] = new GaugeSupplier[T](f)
    val gauge = metricRegistry.gauge(name, supplier)
    namedGauges.add(NamedGauge(name, gauge))
  }

  def addGauge[T](name: String, guage: Gauge[T]): Unit = {
    namedGauges.add(NamedGauge(name, guage))
  }

  protected val namedTimers =
    new ConcurrentHashMap[String, (NamedTimer, ConcurrentHashMap[String, Long])]()

  def addTimer(name: String): Unit = {
    val namedTimer = NamedTimer(name, metricRegistry.timer(name, timerSupplier))
    namedTimers.putIfAbsent(name, (namedTimer, new ConcurrentHashMap[String, Long]()))
  }

  protected val namedCounters: ConcurrentHashMap[String, NamedCounter] =
    new ConcurrentHashMap[String, NamedCounter]()

  def addCounter(name: String): Unit = {
    namedCounters.put(name, NamedCounter(name, metricRegistry.counter(name)))
  }

  protected def counters(): List[NamedCounter] = {
    namedCounters.values().asScala.toList
  }

  def gauges(): List[NamedGauge[_]] = {
    namedGauges.asScala.toList
  }

  protected def histograms(): List[NamedHistogram] = {
    List.empty[NamedHistogram]
  }

  protected def timers(): List[NamedTimer] = {
    namedTimers.values().asScala.toList.map(_._1)
  }

  def needSample(): Boolean = {
    if (metricsSampleRate >= 1) {
      true
    } else if (metricsSampleRate <= 0) {
      false
    } else {
      Random.nextDouble() <= metricsSampleRate
    }
  }

  override def sample[T](metricsName: String, key: String)(f: => T): T = {
    val sample = needSample()
    var r: Any = null
    try {
      if (sample) {
        doStartTimer(metricsName, key)
      }
      r = f
    } finally {
      if (sample) {
        doStopTimer(metricsName, key)
      }
    }
    r.asInstanceOf[T]
  }

  override def startTimer(metricsName: String, key: String): Unit = {
    if (needSample()) {
      doStartTimer(metricsName, key)
    }
  }

  override def stopTimer(metricsName: String, key: String): Unit = {
    doStopTimer(metricsName, key)
  }

  def doStartTimer(metricsName: String, key: String): Unit = {
    val pair = namedTimers.get(metricsName)
    if (pair != null) {
      pair._2.put(key, System.nanoTime())
    } else {
      logWarning(s"Metric $metricsName not found!")
    }
  }

  protected def doStopTimer(metricsName: String, key: String): Unit = {
    try {
      val (namedTimer, map) = namedTimers.get(metricsName)
      val startTime = Option(map.remove(key))
      startTime match {
        case Some(t) =>
          namedTimer.timer.update(System.nanoTime() - t, TimeUnit.NANOSECONDS)
          if (namedTimer.timer.getCount % metricsSlidingWindowSize == 0) {
            recordTimer(namedTimer)
          }
        case None =>
      }
    } catch {
      case e: Exception =>
        logWarning("Exception encountered in Metrics StopTimer", e)
    }
  }

  override def incCounter(metricsName: String, incV: Long = 1): Unit = {
    val counter = namedCounters.get(metricsName)
    if (counter != null) {
      counter.counter.inc(incV)
    } else {
      logWarning(s"Metric $metricsName not found!")
    }
  }

  private def clearOldValues(map: ConcurrentHashMap[String, Long]): Unit = {
    if (map.size > 5000) {
      // remove values has existed more than 15 min
      // 50000 values may be 1MB more or less
      val threshTime = System.nanoTime() - 900000000000L
      val it = map.entrySet().iterator
      while (it.hasNext) {
        val entry = it.next()
        if (entry.getValue < threshTime) {
          it.remove()
        }
      }
    }
  }

  protected def startCleaner(): Unit = {
    val cleanTask: Runnable = new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        namedTimers.values.asScala.toArray.map(_._2).foreach(clearOldValues)
      }
    }
    metricsCleaner.scheduleWithFixedDelay(cleanTask, 10, 10, TimeUnit.MINUTES)
  }

  private def updateInnerMetrics(str: String): Unit = {
    innerMetrics.synchronized {
      if (innerMetrics.size() >= metricsCapacity) {
        innerMetrics.remove()
      }
      innerMetrics.offer(str)
    }
  }

  def recordCounter(nc: NamedCounter): Unit = {
    val timestamp = System.currentTimeMillis
    updateInnerMetrics(s"${normalizeKey(nc.name)}Count$label ${nc.counter.getCount} $timestamp\n")
  }

  def recordGauge(ng: NamedGauge[_]): Unit = {
    val timestamp = System.currentTimeMillis
    val sb = new StringBuilder
    sb.append(s"${normalizeKey(ng.name)}Value$label ${ng.gauge.getValue} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordHistogram(nh: NamedHistogram): Unit = {
    val timestamp = System.currentTimeMillis
    val sb = new mutable.StringBuilder
    val snapshot = nh.histogram.getSnapshot
    val prefix = normalizeKey(nh.name)
    sb.append(s"${prefix}Count$label ${nh.histogram.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  def recordTimer(nt: NamedTimer): Unit = {
    val timestamp = System.currentTimeMillis
    val sb = new mutable.StringBuilder
    val snapshot = nt.timer.getSnapshot
    val prefix = normalizeKey(nt.name)
    sb.append(s"${prefix}Count$label ${nt.timer.getCount} $timestamp\n")
    sb.append(s"${prefix}Max$label ${reportNanosAsMills(snapshot.getMax)} $timestamp\n")
    sb.append(s"${prefix}Mean$label ${reportNanosAsMills(snapshot.getMean)} $timestamp\n")
    sb.append(s"${prefix}Min$label ${reportNanosAsMills(snapshot.getMin)} $timestamp\n")
    sb.append(s"${prefix}50thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.getMedian)} $timestamp\n")
    sb.append(s"${prefix}75thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get75thPercentile)} $timestamp\n")
    sb.append(s"${prefix}95thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get95thPercentile)} $timestamp\n")
    sb.append(s"${prefix}98thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get98thPercentile)} $timestamp\n")
    sb.append(s"${prefix}99thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get99thPercentile)} $timestamp\n")
    sb.append(s"${prefix}999thPercentile$label" +
      s" ${reportNanosAsMills(snapshot.get999thPercentile)} $timestamp\n")

    updateInnerMetrics(sb.toString())
  }

  override def getMetrics(): String = {
    counters().foreach(c => recordCounter(c))
    gauges().foreach(g => recordGauge(g))
    histograms().foreach(h => {
      recordHistogram(h)
      h.asInstanceOf[RssHistogram].reservoir
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    timers().foreach(t => {
      recordTimer(t)
      t.timer.asInstanceOf[RssTimer].reservoir
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    val sb = new mutable.StringBuilder
    innerMetrics.synchronized {
      while (!innerMetrics.isEmpty) {
        sb.append(innerMetrics.poll())
      }
      innerMetrics.clear()
    }
    sb.toString()
  }

  protected def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }

  protected def reportNanosAsMills(value: Double): Double = {
    BigDecimal(value / 1000000).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  val label = s"""{role="$role"}"""
}

class TimerSupplier(val slidingWindowSize: Int)
  extends MetricRegistry.MetricSupplier[Timer] {
  override def newMetric(): Timer = {
    new RssTimer(new ResettableSlidingWindowReservoir(slidingWindowSize))
  }
}

class GaugeSupplier[T](f: Unit => T) extends MetricRegistry.MetricSupplier[Gauge[_]] {
  override def newMetric(): Gauge[T] = {
    new Gauge[T] {
      override def getValue: T = f()
    }
  }
}
