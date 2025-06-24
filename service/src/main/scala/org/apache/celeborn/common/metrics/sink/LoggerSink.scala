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
