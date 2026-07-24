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

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter

class JmxSink(val property: Properties, val registry: MetricRegistry) extends Sink {

  // Publish MBeans under a configurable, Celeborn-specific JMX domain (defaulting to
  // `celeborn`) rather than JmxReporter's global default domain `metrics`. This avoids
  // MBean name collisions when other components using Dropwizard metrics run in the
  // same JVM. The domain can be overridden via `*.sink.jmx.domain=<domain>`.
  val domain: String =
    Option(property.getProperty(JmxSink.JMX_DOMAIN_KEY))
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(JmxSink.JMX_DEFAULT_DOMAIN)

  val reporter: JmxReporter = JmxReporter.forRegistry(registry)
    .inDomain(domain)
    .build()

  override def start(): Unit = {
    reporter.start()
  }

  override def stop(): Unit = {
    reporter.stop()
  }

  override def report(): Unit = {}
}

object JmxSink {
  val JMX_DOMAIN_KEY = "domain"
  val JMX_DEFAULT_DOMAIN = "celeborn"
}
