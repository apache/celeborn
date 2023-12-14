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

import scala.collection.mutable.ArrayBuffer

import com.codahale.metrics.MetricRegistry
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import io.netty.channel.ChannelHandler.Sharable

import org.apache.celeborn.common.metrics.{CelebornHistogram, CelebornTimer, ResettableSlidingWindowReservoir}
import org.apache.celeborn.common.metrics.source.{AbstractSource, NamedCounter, NamedGauge, NamedHistogram, NamedTimer, Source}

object JsonConverter {
  val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def toPrettyJson(value: Any): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
  }
}

case class MetricData(
    name: String,
    value: Any,
    timestampMs: Long,
    labelNames: ArrayBuffer[String],
    labelValues: ArrayBuffer[String])

class JsonServlet(
    val property: Properties,
    val registry: MetricRegistry,
    val sources: Seq[Source],
    val servletPath: String,
    val jsonPrettyEnabled: Boolean) extends AbstractServlet(sources) {

  override def getMetricsSnapshot: String = {
    val metricDatas = new ArrayBuffer[MetricData]
    try {
      sources.map(source => metricDatas ++= getMetrics(source))
      if (jsonPrettyEnabled) {
        JsonConverter.toPrettyJson(metricDatas.map(_.asInstanceOf[Any]))
      } else {
        JsonConverter.toJson(metricDatas.map(_.asInstanceOf[Any]))
      }
    } catch {
      case e: Throwable =>
        logError("failed to get json data for metrics", e)
        JsonConverter.toJson(new ArrayBuffer())
    }
  }

  override def createHttpRequestHandler(): ServletHttpRequestHandler = {
    new JsonHttpRequestHandler(servletPath, this)
  }

  override def stop(): Unit = {}

  def getMetrics(source: Source): ArrayBuffer[MetricData] = {
    val metricDatas = new ArrayBuffer[MetricData]
    val absSource = source.asInstanceOf[AbstractSource]
    absSource.counters().foreach(c => recordCounter(absSource, c, metricDatas))
    absSource.gauges().foreach(g => recordGauge(absSource, g, metricDatas))
    absSource.histograms().foreach(h => {
      recordHistogram(absSource, h, metricDatas)
      h.asInstanceOf[CelebornHistogram].reservoir
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    absSource.timers().foreach(t => {
      recordTimer(absSource, t, metricDatas)
      t.timer.asInstanceOf[CelebornTimer].reservoir
        .asInstanceOf[ResettableSlidingWindowReservoir].reset()
    })
    metricDatas
  }

  def recordCounter(
      absSource: AbstractSource,
      nc: NamedCounter,
      metricDatas: ArrayBuffer[MetricData]): Unit = {
    val timestamp = System.currentTimeMillis
    val labelNames = new ArrayBuffer[String]
    val labelValues = new ArrayBuffer[String]
    nc.labels.map { case (k, v) =>
      labelNames += k
      labelValues += v
    }
    val metricData =
      MetricData(
        nc.name,
        nc.counter.getCount,
        timestamp,
        labelNames,
        labelValues)
    updateInnerMetrics(absSource, metricData, metricDatas)
  }

  def recordGauge(
      absSource: AbstractSource,
      ng: NamedGauge[_],
      metricDatas: ArrayBuffer[MetricData]): Unit = {
    val timestamp = System.currentTimeMillis
    val labelNames = new ArrayBuffer[String]
    val labelValues = new ArrayBuffer[String]
    ng.labels.map { case (k, v) =>
      labelNames += k
      labelValues += v
    }
    val metricData = MetricData(ng.name, ng.gauge.getValue, timestamp, labelNames, labelValues)
    updateInnerMetrics(absSource, metricData, metricDatas)
  }

  def recordHistogram(
      absSource: AbstractSource,
      nh: NamedHistogram,
      metricDatas: ArrayBuffer[MetricData]): Unit = {
    val timestamp = System.currentTimeMillis
    val labelNames = new ArrayBuffer[String]
    val labelValues = new ArrayBuffer[String]
    nh.labels.map { case (k, v) =>
      labelNames += k
      labelValues += v
    }
    val snapshot = nh.histogram.getSnapshot
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_Count",
        nh.histogram.getCount,
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_Max",
        absSource.reportNanosAsMills(snapshot.getMax),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_Mean",
        absSource.reportNanosAsMills(snapshot.getMean),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_Min",
        absSource.reportNanosAsMills(snapshot.getMin),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_50thPercentile",
        absSource.reportNanosAsMills(snapshot.getMedian),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_75thPercentile",
        absSource.reportNanosAsMills(snapshot.get75thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_95thPercentile",
        absSource.reportNanosAsMills(snapshot.get95thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_98thPercentile",
        absSource.reportNanosAsMills(snapshot.get98thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_99thPercentile",
        absSource.reportNanosAsMills(snapshot.get99thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nh.name}_999thPercentile",
        absSource.reportNanosAsMills(snapshot.get999thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
  }

  def recordTimer(
      absSource: AbstractSource,
      nt: NamedTimer,
      metricDatas: ArrayBuffer[MetricData]): Unit = {
    val timestamp = System.currentTimeMillis
    val labelNames = new ArrayBuffer[String]
    val labelValues = new ArrayBuffer[String]
    nt.labels.map { case (k, v) =>
      labelNames += k
      labelValues += v
    }
    val snapshot = nt.timer.getSnapshot
    updateInnerMetrics(
      absSource,
      MetricData(s"${nt.name}_Count", nt.timer.getCount, timestamp, labelNames, labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_Max",
        absSource.reportNanosAsMills(snapshot.getMax),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_Mean",
        absSource.reportNanosAsMills(snapshot.getMean),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_Min",
        absSource.reportNanosAsMills(snapshot.getMin),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_50thPercentile",
        absSource.reportNanosAsMills(snapshot.getMedian),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_75thPercentile",
        absSource.reportNanosAsMills(snapshot.get75thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_95thPercentile",
        absSource.reportNanosAsMills(snapshot.get95thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_98thPercentile",
        absSource.reportNanosAsMills(snapshot.get98thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_99thPercentile",
        absSource.reportNanosAsMills(snapshot.get99thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
    updateInnerMetrics(
      absSource,
      MetricData(
        s"${nt.name}_999thPercentile",
        absSource.reportNanosAsMills(snapshot.get999thPercentile),
        timestamp,
        labelNames,
        labelValues),
      metricDatas)
  }

  private def updateInnerMetrics(
      absSource: AbstractSource,
      metricData: MetricData,
      metricDatas: ArrayBuffer[MetricData]): Unit = {
    if (metricDatas.size < absSource.metricsCapacity) {
      metricDatas += metricData
    }
  }
}

@Sharable
class JsonHttpRequestHandler(path: String, jsonServlet: JsonServlet)
  extends ServletHttpRequestHandler(path) {

  override def handleRequest(uri: String): String = {
    jsonServlet.getMetricsSnapshot
  }
}
