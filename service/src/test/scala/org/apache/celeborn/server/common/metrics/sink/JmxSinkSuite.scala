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

package org.apache.celeborn.server.common.metrics.sink

import java.lang.management.ManagementFactory
import java.util.Properties
import javax.management.ObjectName

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.sink.JmxSink
import org.apache.celeborn.common.metrics.source.JVMSource
import org.apache.celeborn.common.network.TestHelper

class JmxSinkSuite extends CelebornFunSuite {

  private def newMetricsSystem(): MetricsSystem = {
    val celebornConf = new CelebornConf()
    celebornConf
      .set(CelebornConf.METRICS_ENABLED.key, "true")
      .set(
        CelebornConf.METRICS_CONF.key,
        TestHelper.getResourceAsAbsolutePath("/metrics-jmx.properties"))
    val metricsSystem = MetricsSystem.createMetricsSystem("test", celebornConf)
    metricsSystem.registerSource(new JVMSource(celebornConf, "test"))
    metricsSystem
  }

  test("test load jmx sink case") {
    val metricsSystem = newMetricsSystem()
    metricsSystem.start(true)

    try {
      // JmxSink has no dedicated branch in MetricsSystem.registerSinks, so it must be
      // instantiated reflectively via its (Properties, MetricRegistry) constructor.
      assert(metricsSystem.sinks.exists(_.isInstanceOf[JmxSink]))
    } finally {
      metricsSystem.stop()
    }
  }

  test("test jmx sink registers and unregisters MBeans lifecycle case") {
    val metricsSystem = newMetricsSystem()
    val mBeanServer = ManagementFactory.getPlatformMBeanServer
    // JmxSink publishes metrics under the Celeborn-specific default domain.
    val jmxDomainPattern = new ObjectName(s"${JmxSink.JMX_DEFAULT_DOMAIN}:*")

    val beforeStart = mBeanServer.queryNames(jmxDomainPattern, null).asScala
    var registeredByJmxSink = Set.empty[ObjectName]

    try {
      // start() runs registerSinks() (reflection-based loading) followed by Sink.start(),
      // which makes the JmxSink's JmxReporter register the registry metrics as MBeans.
      metricsSystem.start(true)
      // report() is a no-op for JmxSink (JmxReporter reports on registration) but must be safe.
      metricsSystem.report()

      val afterStart = mBeanServer.queryNames(jmxDomainPattern, null).asScala
      registeredByJmxSink = (afterStart -- beforeStart).toSet

      assert(metricsSystem.sinks.exists(_.isInstanceOf[JmxSink]))
      assert(
        registeredByJmxSink.nonEmpty,
        s"JmxSink.start() should register metric MBeans under the " +
          s"'${JmxSink.JMX_DEFAULT_DOMAIN}' domain")
    } finally {
      metricsSystem.stop()
    }

    val afterStop = mBeanServer.queryNames(jmxDomainPattern, null).asScala
    assert(
      registeredByJmxSink.forall(name => !afterStop.contains(name)),
      "JmxSink.stop() should unregister the MBeans it registered")
  }

  test("test jmx sink honors configured domain case") {
    // Use a unique domain per run so the assertions are robust against MBeans that may
    // already exist in (or were leaked by a prior failed run into) the JVM-global
    // MBeanServer, and assert on the before/after delta rather than absolute presence.
    val domain = s"celeborn-jmx-test-${java.util.UUID.randomUUID()}"
    val properties = new Properties()
    properties.setProperty(JmxSink.JMX_DOMAIN_KEY, domain)
    val registry = new MetricRegistry()
    registry.counter("test-counter").inc()

    val sink = new JmxSink(properties, registry)
    assert(sink.domain == domain)

    val mBeanServer = ManagementFactory.getPlatformMBeanServer
    val jmxDomainPattern = new ObjectName(s"$domain:*")

    val beforeStart = mBeanServer.queryNames(jmxDomainPattern, null).asScala
    var registeredByJmxSink = Set.empty[ObjectName]

    try {
      sink.start()
      sink.report()
      val afterStart = mBeanServer.queryNames(jmxDomainPattern, null).asScala
      registeredByJmxSink = (afterStart -- beforeStart).toSet
      assert(
        registeredByJmxSink.nonEmpty,
        s"JmxSink should register MBeans under the configured '$domain' domain")
    } finally {
      sink.stop()
    }

    val afterStop = mBeanServer.queryNames(jmxDomainPattern, null).asScala
    assert(
      registeredByJmxSink.forall(name => !afterStop.contains(name)),
      "JmxSink.stop() should unregister the MBeans under the configured domain")
  }
}
