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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.sink.LoggerSink
import org.apache.celeborn.common.metrics.source.JVMSource
import org.apache.celeborn.common.network.TestHelper

class LoggerSinkSuite extends CelebornFunSuite {
  test("test load logger sink case") {
    val celebornConf = new CelebornConf()
    celebornConf
      .set(CelebornConf.METRICS_ENABLED.key, "true")
      .set(
        CelebornConf.METRICS_CONF.key,
        TestHelper.getResourceAsAbsolutePath("/metrics2.properties"))
    val metricsSystem = MetricsSystem.createMetricsSystem("test", celebornConf)
    metricsSystem.registerSource(new JVMSource(celebornConf, "test"))
    metricsSystem.start(true)

    var hasLoggerSink = false
    metricsSystem.sinks.foreach { sink =>
      sink.isInstanceOf[LoggerSink] match {
        case true =>
          hasLoggerSink = true
        case false =>
      }
    }

    metricsSystem.stop()

    assert(hasLoggerSink)
  }

  test("test logger sink configs case") {
    val celebornConf = new CelebornConf()
    celebornConf
      .set(CelebornConf.METRICS_ENABLED.key, "true")
      .set(
        CelebornConf.METRICS_CONF.key,
        TestHelper.getResourceAsAbsolutePath("/metrics2.properties"))
    celebornConf.set("celeborn.metrics.loggerSink.scrape.interval", "10s")
    celebornConf.set("celeborn.metrics.loggerSink.output.enabled", "true")
    val metricsSystem = MetricsSystem.createMetricsSystem("test", celebornConf)
    metricsSystem.registerSource(new JVMSource(celebornConf, "test"))
    metricsSystem.start(true)

    metricsSystem.sinks.foreach { sink =>
      sink.isInstanceOf[LoggerSink] match {
        case true =>
          val loggerSink = sink.asInstanceOf[LoggerSink]
          assert(loggerSink.metricsLoggerSinkScrapeOutputEnabled == true)
          assert(loggerSink.metricsLoggerSinkScrapeInterval == 10000)
        case false =>
      }
    }

    metricsSystem.stop()
  }

  test("test logger sink validity case") {
    val celebornConf = new CelebornConf()
    celebornConf
      .set(CelebornConf.METRICS_ENABLED.key, "true")
      .set(
        CelebornConf.METRICS_CONF.key,
        TestHelper.getResourceAsAbsolutePath("/metrics2.properties"))
    celebornConf.set("celeborn.metrics.loggerSink.scrape.interval", "3s")
    celebornConf.set("celeborn.metrics.loggerSink.output.enabled", "true")
    val metricsSystem = MetricsSystem.createMetricsSystem("test", celebornConf)
    val jvmSource = new JVMSource(celebornConf, "test")
    metricsSystem.registerSource(jvmSource)
    metricsSystem.start(true)

    jvmSource.timerMetrics.add("test1")
    jvmSource.timerMetrics.add("test2")
    jvmSource.timerMetrics.add("test3")
    jvmSource.timerMetrics.add("test4")
    jvmSource.timerMetrics.add("test5")
    Thread.sleep(100)
    assert(jvmSource.timerMetrics.size() != 0)
    Thread.sleep(10000)
    metricsSystem.stop()

    assert(jvmSource.timerMetrics.size() == 0)
  }

  test("test logger sink output case") {
    val celebornConf = new CelebornConf()
    celebornConf
      .set(CelebornConf.METRICS_ENABLED.key, "true")
      .set(
        CelebornConf.METRICS_CONF.key,
        TestHelper.getResourceAsAbsolutePath("/metrics2.properties"))
    celebornConf.set("celeborn.metrics.loggerSink.scrape.interval", "1s")
    celebornConf.set("celeborn.metrics.loggerSink.output.enabled", "true")
    val logAppender = new LogAppender("test logger sink appender")
    withLogAppender(logAppender) {
      val metricsSystem = MetricsSystem.createMetricsSystem("test", celebornConf)
      metricsSystem.registerSource(new JVMSource(celebornConf, "test"))
      metricsSystem.start(true)

      Thread.sleep(10000)
      metricsSystem.stop()
    }
    val validOutput = logAppender.loggingEvents
      .filter(_.getMessage.getFormattedMessage.contains("scraped metrics"))
    assert(validOutput.size > 0)
  }
}
