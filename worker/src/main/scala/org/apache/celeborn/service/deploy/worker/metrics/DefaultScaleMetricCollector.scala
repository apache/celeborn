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

package org.apache.celeborn.service.deploy.worker.metrics

import java.lang.management.{ManagementFactory, OperatingSystemMXBean}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.util.control.NonFatal

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.DiskStatus
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.service.deploy.worker.Worker

class DefaultScaleMetricCollector private[metrics] (
    conf: CelebornConf,
    operatingSystem: OperatingSystemMXBean)
  extends ScaleMetricCollector with Logging {

  def this(conf: CelebornConf) =
    this(conf, ManagementFactory.getOperatingSystemMXBean)

  private val memoryWindow = new MetricSlidingWindow(conf.scaleMetricWindowSize)
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("scale-metric-collector")
  private var scheduledTask: ScheduledFuture[_] = _

  @volatile private var stopped = false
  @volatile private var currentStats: Option[WorkerStats] = None

  override def init(worker: Worker): Unit = synchronized {
    require(!stopped && scheduledTask == null, "Metric collector can only be initialized once")
    scheduledTask = executor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = collectOnce(worker)
      },
      conf.scaleMetricInterval,
      conf.scaleMetricInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = synchronized {
    stopped = true
    currentStats = None
    if (scheduledTask != null) {
      scheduledTask.cancel(true)
    }
    executor.shutdownNow()
  }

  override def currentWorkerStats(): Option[WorkerStats] = {
    if (stopped) None
    else currentStats
  }

  private[metrics] def collectOnce(worker: Worker): Unit = {
    try {
      val metrics = memoryMetrics(worker) ++ systemLoadMetrics() ++ diskMetrics(worker)
      currentStats = Some(WorkerStats(metrics))
    } catch {
      case NonFatal(e) =>
        logWarning("Failed to collect worker scaling metrics", e)
    }
  }

  private def memoryMetrics(worker: Worker): Map[String, Double] = {
    val capacity = worker.memoryManager.maxDirectMemory
    val used = worker.memoryManager.getMemoryUsage
    memoryWindow.update(used)
    Map(WorkerStats.NettyMemoryUsedRatio -> (memoryWindow.average / capacity))
  }

  private def systemLoadMetrics(): Map[String, Double] = {
    val load = operatingSystem.getSystemLoadAverage
    val processors = operatingSystem.getAvailableProcessors
    Map(WorkerStats.LastMinuteSystemLoadRatio -> (load / processors))
  }

  private def diskMetrics(worker: Worker): Map[String, Double] = {
    val disks = worker.storageManager.localDisksSnapshot()
    val usableDisks = disks.filter(_.status != DiskStatus.IO_HANG)
    val usableBytes = usableDisks.map(_.actualUsableSpace.toDouble).sum
    val totalBytes = disks.map(_.totalSpace.toDouble).sum
    Map(
      WorkerStats.DiskUsedRatio -> (1.0 - usableBytes / totalBytes),
      WorkerStats.DiskRemainingSize -> usableBytes)
  }
}
