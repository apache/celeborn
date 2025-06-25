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

/**
 * This sink is not follow the strandard sink interface. It has the duty to clean internal state.
 * @param sources
 * @param conf
 */
class LoggerSink(sources: Seq[Source], conf: CelebornConf) extends Sink with Logging {
  val metricsLoggerSinkScrapeOutputEnabled = conf.metricsLoggerSinkScrapeOutputEnabled
  val metricsLoggerSinkScrapeInterval = conf.metricsLoggerSinkScrapeInterval
  val metricScrapeThread: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"metrics-scrape-thread")
  override def start(): Unit = {
    metricScrapeThread.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          sources.foreach { source =>
            val metricsData = source.getMetrics
            if (metricsLoggerSinkScrapeOutputEnabled) {
              // The method `source.getMetrics` will clear `timeMetric` queue.
              // This is essential because the queue can be large enough
              // to cause the worker run out of memory
              logInfo(s"Source ${source.sourceName} scraped metrics: ${metricsData}")
            }
          }
        }
      },
      metricsLoggerSinkScrapeInterval,
      metricsLoggerSinkScrapeInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(metricScrapeThread)
  }

  override def report(): Unit = {}
}
