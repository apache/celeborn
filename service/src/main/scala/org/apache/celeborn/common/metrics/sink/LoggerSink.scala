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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.Source
import org.apache.celeborn.common.util.ThreadUtils

class LoggerSink(sources: Seq[Source], conf: CelebornConf) extends Sink with Logging {
  private val metricsLoggerSinkScrapeOutputEnabled = conf.metricsLoggerSinkScrapeSaveEnabled
  private val metricsLoggerSinkScrapeInterval = conf.metricsLoggerSinkScrapeInterval
  var metricScrapeThread: ScheduledExecutorService = null
  override def start(): Unit = {
    metricScrapeThread =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"metrics-scrape-thread")
    metricScrapeThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          if (metricsLoggerSinkScrapeOutputEnabled) {
            sources.foreach { source =>
              logInfo(s"Source ${source.sourceName} scraped metrics: ${source.getMetrics}")
            }
          }
        }
      },
      metricsLoggerSinkScrapeInterval,
      metricsLoggerSinkScrapeInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    if (metricScrapeThread != null) {
      metricScrapeThread.shutdown()
    }
  }

  override def report(): Unit = {}
}
