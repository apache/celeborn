package com.aliyun.emr.rss.common.metrics.source

import java.lang.management.ManagementFactory

import scala.collection.JavaConverters._

import com.aliyun.emr.rss.common.RssConf
import com.codahale.metrics.Gauge
import com.codahale.metrics.jvm.{BufferPoolMetricSet, GarbageCollectorMetricSet, MemoryUsageGaugeSet}

class JVMSource(rssConf: RssConf, role: String) extends AbstractSource(rssConf: RssConf, role: String) {
  override val sourceName = "JVM"

  // all of metrics of GCMetricSet and BufferPoolMetricSet are Gauge
  Seq(new GarbageCollectorMetricSet(),
    new MemoryUsageGaugeSet(),
    new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer))
    .map { x => x.getMetrics.asScala.map {
      case (name: String, metric: Gauge[_]) => addGauge(name, metric)
    }
    }
  // start cleaner
  startCleaner()
}
