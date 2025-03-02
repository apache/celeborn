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

package org.apache.celeborn.common.metrics

import java.util.Properties
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{METRICS_JSON_PATH, METRICS_PROMETHEUS_PATH}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.sink.{JsonServlet, PrometheusServlet, Sink}
import org.apache.celeborn.common.metrics.source.Source
import org.apache.celeborn.common.util.Utils

class MetricsSystem(
    val instance: String,
    conf: CelebornConf) extends Logging {
  private[this] val metricsConfig = new MetricsConfig(conf)

  private val sinks = new ArrayBuffer[Sink]
  private val sources = new CopyOnWriteArrayList[Source]
  private val registry = new MetricRegistry()
  private val prometheusServletPath = conf.get(METRICS_PROMETHEUS_PATH)
  private val jsonServletPath = conf.get(METRICS_JSON_PATH)

  private var prometheusServlet: Option[PrometheusServlet] = None
  private var jsonServlet: Option[JsonServlet] = None

  var running: Boolean = false

  metricsConfig.initialize()

  def getServletContextHandlers: Array[ServletContextHandler] = {
    require(running, "Can only call getServletHandlers on a running MetricsSystem")
    prometheusServlet.map(_.getHandlers(conf)).getOrElse(Array()) ++
      jsonServlet.map(_.getHandlers(conf)).getOrElse(Array())
  }

  def start(registerStaticSources: Boolean = true) {
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    if (registerStaticSources) {
      registerSources()
    }
    registerSinks()
    sinks.foreach(_.start())
  }

  def stop() {
    if (running) {
      sinks.foreach(_.stop())
    } else {
      logWarning("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report() {
    sinks.foreach(_.report())
  }

  private def buildRegistryName(source: Source): String = {
    MetricRegistry.name(source.sourceName)
  }

  def getSourcesByName(sourceName: String): Seq[Source] =
    sources.asScala.filter(_.sourceName == sourceName).toSeq

  def registerSource(source: Source) {
    sources.add(source)
    try {
      val regName = buildRegistryName(source)
      registry.register(regName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def removeSource(source: Source) {
    sources.remove(source)
    val regName = buildRegistryName(source)
    registry.removeMatching(new MetricFilter {
      def matches(name: String, metric: Metric): Boolean = name.startsWith(regName)
    })
  }

  private def registerSources() {
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SOURCE_REGEX)

    // Register all the sources related to instance
    sourceConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      try {
        val source = Utils.classForName(classPath).getDeclaredConstructor().newInstance()
        registerSource(source.asInstanceOf[Source])
      } catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantiated", e)
      }
    }
  }

  protected[celeborn] def registerSink(sink: Sink): Unit = {
    sinks += sink
  }

  private def registerSinks() {
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricsSystem.SINK_REGEX)

    sinkConfigs.foreach { kv =>
      val classPath = kv._2.getProperty("class")
      if (null != classPath) {
        try {
          if (kv._1 == "prometheusServlet") {
            val servlet = Utils.classForName(classPath)
              .getConstructor(
                classOf[Properties],
                classOf[MetricRegistry],
                classOf[Seq[Source]],
                classOf[String])
            prometheusServlet = Some(servlet.newInstance(
              kv._2,
              registry,
              sources.asScala.toSeq,
              prometheusServletPath).asInstanceOf[PrometheusServlet])
          } else if (kv._1 == "jsonServlet") {
            val servlet = Utils.classForName(classPath)
              .getConstructor(
                classOf[Properties],
                classOf[MetricRegistry],
                classOf[Seq[Source]],
                classOf[String],
                classOf[Boolean])
            jsonServlet = Some(servlet.newInstance(
              kv._2,
              registry,
              sources.asScala.toSeq,
              jsonServletPath,
              conf.metricsJsonPrettyEnabled.asInstanceOf[Object]).asInstanceOf[JsonServlet])
          } else {
            val sink = Utils.classForName(classPath)
              .getConstructor(classOf[Properties], classOf[MetricRegistry])
              .newInstance(kv._2, registry)
            sinks += sink.asInstanceOf[Sink]
          }
        } catch {
          case e: Exception =>
            logError("Sink class " + classPath + " cannot be instantiated")
            throw e
        }
      }
    }
  }

}

object MetricsSystem {
  val SINK_REGEX: Regex = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX: Regex = "^org.apache.celeborn.common.metrics.source\\.(.+)\\.(.+)".r

  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }

  def createMetricsSystem(
      instance: String,
      conf: CelebornConf): MetricsSystem = {
    new MetricsSystem(instance, conf)
  }
}
