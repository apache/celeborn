package com.aliyun.emr.rss.common.metrics.source

import java.lang.management.ManagementFactory
import javax.management.{MBeanServer, ObjectName}

import scala.util.control.NonFatal

import com.aliyun.emr.rss.common.RssConf
import com.codahale.metrics.Gauge

class JVMCPUSource(rssConf: RssConf, role: String) extends AbstractSource(rssConf: RssConf, role: String) {
  override val sourceName = "CPU"

  import JVMCPUSource._

  addGauge(JVMCPUTime, new Gauge[Long] {
    val mBean: MBeanServer = ManagementFactory.getPlatformMBeanServer
    val name = new ObjectName("java.lang", "type", "OperatingSystem")

    override def getValue: Long = {
      try {
        // return JVM process CPU time if the ProcessCpuTime method is available
        mBean.getAttribute(name, "ProcessCpuTime").asInstanceOf[Long]
      } catch {
        case NonFatal(_) => -1L
      }
    }
  })
  // start cleaner
  startCleaner()
}

object JVMCPUSource {
  val JVMCPUTime = "JVMCPUTime"
}
