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

import scala.collection.mutable

import com.codahale.metrics.MetricRegistry
import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.celeborn.common.metrics.sink.PrometheusServlet.{HeaderLine, MetricFamily}
import org.apache.celeborn.common.metrics.source.Source
import org.apache.celeborn.server.common.http.HttpUtils
import org.apache.celeborn.server.common.http.HttpUtils.ServletParams

class PrometheusServlet(
    val property: Properties,
    val registry: MetricRegistry,
    val sources: Seq[Source],
    val servletPath: String) extends AbstractServlet(sources) {

  override def createServletHandler(): ServletContextHandler = {
    HttpUtils.createServletHandler(
      servletPath,
      new ServletParams(_ => getMetricsSnapshot, "text/plain"))
  }

  /**
   * Sources emit a # HELP / # TYPE pair before every sample and iterate metrics in hash order,
   * so samples of one metric family can be repeated and interleaved with other families. The
   * Prometheus exposition format requires a single # HELP / # TYPE per family with all of its
   * samples grouped together, so regroup the output by metric family.
   */
  override def getMetricsSnapshot: String = {
    val raw = super.getMetricsSnapshot
    val families = mutable.LinkedHashMap[String, MetricFamily]()

    raw.linesIterator.foreach {
      case line @ HeaderLine(kind, metric) =>
        val family = families.getOrElseUpdate(metric, new MetricFamily)
        if (kind == "HELP" && family.help == null) {
          family.help = line
        } else if (kind == "TYPE" && family.metricType == null) {
          family.metricType = line
        }
      case line if line.nonEmpty =>
        val metric = line.takeWhile(c => c != '{' && !c.isWhitespace)
        families.getOrElseUpdate(metric, new MetricFamily).samples += line
      case _ =>
    }

    val out = new StringBuilder(raw.length)
    families.valuesIterator.foreach { family =>
      if (family.help != null) out.append(family.help).append('\n')
      if (family.metricType != null) out.append(family.metricType).append('\n')
      family.samples.foreach(out.append(_).append('\n'))
    }
    out.result()
  }
}

object PrometheusServlet {
  private val HeaderLine = "^#\\s+(TYPE|HELP)\\s+(\\S+)\\b.*$".r

  private class MetricFamily {
    var help: String = _
    var metricType: String = _
    val samples = mutable.ArrayBuffer[String]()
  }
}
