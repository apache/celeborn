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

package org.apache.celeborn.service.deploy.master

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.service.deploy.worker.metrics.ScaleMetricCollector

class WorkerStatsIntegrationSuite extends AnyFunSuite
  with BeforeAndAfterEach with Eventually with MiniClusterFeature {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))

  override def afterEach(): Unit = {
    try {
      if (masterInfo != null) shutdownMiniCluster()
    } finally {
      masterInfo = null
      super.afterEach()
    }
  }

  test("the default collector samples in the background and reports through worker heartbeats") {
    val (master, workers) = setupMiniClusterWithRandomPorts(
      masterConf = Map(CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key -> "10s"),
      workerConf = Map(
        CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key -> "4s",
        CelebornConf.SCALE_METRIC_INTERVAL.key -> "100ms",
        CelebornConf.SCALE_METRIC_WINDOW_SIZE.key -> "2"),
      workerNum = 1)
    assert(workers.head.registered.get())
    assert(master.statusSystem.workersMap.containsKey(workers.head.workerInfo.toUniqueId))
    eventually {
      val stats = master.workerStatsStore.get(workers.head.workerInfo).get
      assert(stats.metrics.contains(WorkerStats.NettyMemoryUsedRatio))
      assert(stats.metrics.contains(WorkerStats.DiskRemainingSize))
    }
  }

  test("custom collector metrics survive heartbeat transport and read or stop failures") {
    HeartbeatTestMetricCollector.starts.set(0)
    HeartbeatTestMetricCollector.stops.set(0)
    HeartbeatTestMetricCollector.failRead = false
    val (master, workers) = setupMiniClusterWithRandomPorts(
      masterConf = Map(CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key -> "10s"),
      workerConf = Map(
        CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key -> "4s",
        CelebornConf.SCALE_METRIC_COLLECTOR_CLASS_NAME.key ->
          classOf[HeartbeatTestMetricCollector].getName),
      workerNum = 1)
    val worker = workers.head
    val registered = master.statusSystem.workersMap.get(worker.workerInfo.toUniqueId)
    def assertReportedMetrics(): Unit = {
      val received = master.workerStatsStore.get(worker.workerInfo).get.metrics
      assert(received.keySet == HeartbeatTestMetricCollector.stats.metrics.keySet)
      HeartbeatTestMetricCollector.stats.metrics.foreach { case (name, value) =>
        if (value.isNaN) assert(received(name).isNaN)
        else assert(received(name) == value)
      }
    }
    eventually {
      assertReportedMetrics()
    }
    assert(HeartbeatTestMetricCollector.starts.get() == 1)
    val lastHeartbeat = registered.lastHeartbeat
    HeartbeatTestMetricCollector.failRead = true
    eventually {
      assert(master.workerStatsStore.get(worker.workerInfo).isEmpty)
      assert(registered.lastHeartbeat > lastHeartbeat)
      assert(worker.registered.get())
    }
    HeartbeatTestMetricCollector.failRead = false
    eventually {
      assertReportedMetrics()
    }
    shutdownMiniCluster()
    masterInfo = null
    assert(HeartbeatTestMetricCollector.stops.get() == 1)
  }
}

class HeartbeatTestMetricCollector(conf: CelebornConf) extends ScaleMetricCollector {
  override def init(worker: Worker): Unit = {
    HeartbeatTestMetricCollector.starts.incrementAndGet()
  }

  override def currentWorkerStats(): Option[WorkerStats] = {
    if (HeartbeatTestMetricCollector.failRead) throw new IllegalStateException("test read failure")
    Some(HeartbeatTestMetricCollector.stats)
  }

  override def stop(): Unit = {
    HeartbeatTestMetricCollector.stops.incrementAndGet()
    throw new IllegalStateException("test stop failure")
  }
}

object HeartbeatTestMetricCollector {
  val stats = WorkerStats(Map(
    WorkerStats.NettyMemoryUsedRatio -> 0.5,
    "" -> 1.0,
    "NaN" -> Double.NaN,
    "PositiveInfinity" -> Double.PositiveInfinity,
    "NegativeInfinity" -> Double.NegativeInfinity))
  val starts = new AtomicInteger()
  val stops = new AtomicInteger()
  @volatile var failRead = false
}
