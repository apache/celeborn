package org.apache.celeborn.service.deploy.worker.metrics

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.collection.mutable

import com.codahale.metrics.Gauge

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.source.{JVMCPUSource, WorkerMetrics}
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerSource}

class WorkerMetricSink(conf: CelebornConf) extends IWorkerMetricSink with Logging {

  private val windowSize = conf.workloadMetricSlidingWindowSize
  private val checkInterval = conf.workloadMetricCheckInterval

  private val cpuMetrics = mutable.Queue[Double]()
  private val diskRatioMetrics = mutable.Queue[Double]()
  private val directMemoryRatioMetrics = mutable.Queue[Double]()

  private var worker: Worker = _
  private[this] var scheduler: ScheduledExecutorService = _

  private lazy val cpuGauge = worker.jvmCPUResource.gauges().find(g =>
    g.name == JVMCPUSource.JVM_CPU_LOAD).get.gauge.asInstanceOf[Gauge[Double]]
  private lazy val diskRatioGauge = worker.workerSource.gauges().find(g =>
    g.name == WorkerSource.DISK_USAGE_RATIO).get.gauge.asInstanceOf[Gauge[Double]]
  private lazy val directMemoryRatioGauge = worker.workerSource.gauges().find(g =>
    g.name == WorkerSource.DIRECT_MEMORY_USAGE_RATIO).get.gauge.asInstanceOf[Gauge[Double]]
  private lazy val stats = worker.workerStatusManager.getWorkerStats

  override def stop(): Unit = {
    scheduler.shutdown()
  }

  override def start(): Unit = {
    scheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-jvm-metric-scheduler")
    scheduler.scheduleWithFixedDelay(
      () => {
        try {
          WorkerMetricSink.this.update()
          WorkerMetricSink.this.report()
        } catch {
          case e: Throwable => logError("reporting metrics failed", e)
        }
      },
      0,
      checkInterval,
      TimeUnit.MILLISECONDS)
  }

  def update(): Unit = {
    cpuMetrics.synchronized {
      cpuMetrics.enqueue(cpuGauge.getValue)
      if (cpuMetrics.size > windowSize) {
        cpuMetrics.dequeue()
      }
    }

    diskRatioMetrics.synchronized {
      diskRatioMetrics.enqueue(diskRatioGauge.getValue)
      if (diskRatioMetrics.size > windowSize) {
        diskRatioMetrics.dequeue()
      }
    }

    directMemoryRatioMetrics.synchronized {
      directMemoryRatioMetrics.enqueue(directMemoryRatioGauge.getValue)
      if (directMemoryRatioMetrics.size > windowSize) {
        directMemoryRatioMetrics.dequeue()
      }
    }

  }

  override def report(): Unit = {
    cpuMetrics.synchronized {
      if (cpuMetrics.size == windowSize) {
        stats.put(WorkerMetrics.CPU_LOAD, (cpuMetrics.sum / cpuMetrics.size).toString)
      }
    }

    diskRatioMetrics.synchronized {
      if (diskRatioMetrics.size == windowSize) {
        stats.put(WorkerMetrics.DISK_RATIO, (diskRatioMetrics.sum / diskRatioMetrics.size).toString)
      }
    }

    directMemoryRatioMetrics.synchronized {
      if (directMemoryRatioMetrics.size == windowSize) {
        stats.put(
          WorkerMetrics.DIRECT_MEMORY_RATIO,
          (directMemoryRatioMetrics.sum / directMemoryRatioMetrics.size).toString)
      }
    }
  }

  override def init(worker: Worker): Unit = {
    this.worker = worker
  }
}
