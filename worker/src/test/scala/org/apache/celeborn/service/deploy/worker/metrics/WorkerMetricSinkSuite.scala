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

import com.codahale.metrics.Gauge
import org.mockito.Mockito._
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.metrics.source.{JVMCPUSource, NamedGauge, WorkerMetrics}
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerSource, WorkerStatusManager}

class WorkerMetricSinkSuite extends CelebornFunSuite {

  private var conf: CelebornConf = _
  private var worker: Worker = _
  private var workerSource: WorkerSource = _
  private var jvmCPUSource: JVMCPUSource = _
  private var workerStatusManager: WorkerStatusManager = _
  private var metricSink: WorkerMetricSink = _
  private var stats: java.util.Map[String, String] = _
  private val windowSize = 3
  private val checkInterval = 1000
  private var guageIndex = 0

  override def beforeAll(): Unit = {
    // Initialize configuration
    conf = new CelebornConf()
    conf.set(WORKER_WORKLOAD_METRIC_SLIDINGWINDOW_SIZE.key, windowSize.toString)
    conf.set(WORKER_WORKLOAD_METRIC_CHECK_INTERVAL.key, checkInterval.toString)

    // Create mock objects
    worker = mock(classOf[Worker])
    workerSource = mock(classOf[WorkerSource])
    jvmCPUSource = mock(classOf[JVMCPUSource])
    workerStatusManager = mock(classOf[WorkerStatusManager])
    stats = new java.util.HashMap[String, String]()

    // Set up mock behavior
    when(worker.workerSource).thenReturn(workerSource)
    when(worker.jvmCPUResource).thenReturn(jvmCPUSource)
    when(worker.workerStatusManager).thenReturn(workerStatusManager)
    when(workerStatusManager.getWorkerStats).thenReturn(stats)

    // Set up metrics
    val cpuGauge = createGauge(Array(50.0, 60.0, 70.0))
    val diskGauge = createGauge(Array(0.6, 0.7, 0.8))
    val memoryGauge = createGauge(Array(0.7, 0.8, 0.9))

    when(jvmCPUSource.gauges()).thenReturn(List(
      NamedGauge(JVMCPUSource.JVM_CPU_LOAD, cpuGauge, Map())))
    when(workerSource.gauges()).thenReturn(List(
      NamedGauge(WorkerSource.DISK_USAGE_RATIO, diskGauge, Map()),
      NamedGauge(WorkerSource.DIRECT_MEMORY_USAGE_RATIO, memoryGauge, Map())))
  }

  override def beforeEach(): Unit = {
    stats.clear()
    // Initialize WorkerMetricSink
    metricSink = new WorkerMetricSink(conf)
    metricSink.init(worker)
  }

  test("metrics should be collected and reported correctly") {
    // Update metrics three times to fill the sliding window
    for (_ <- 1 to 3) {
      metricSink.update()
      guageIndex = (guageIndex + 1) % 3
    }

    // Trigger report
    metricSink.report()

    // Verify results
    stats.get(WorkerMetrics.CPU_LOAD).toDouble shouldBe (60.0 +- 0.001)
    stats.get(WorkerMetrics.DISK_RATIO).toDouble shouldBe (0.7 +- 0.001)
    stats.get(WorkerMetrics.DIRECT_MEMORY_RATIO).toDouble shouldBe (0.8 +- 0.001)
  }

  test("metrics should not be reported before window is full") {
    // Only update twice, window size is 3
    metricSink.update()
    metricSink.update()
    metricSink.report()

    // Verify results - should be empty
    stats.isEmpty should be(true)
  }

  test("start and stop should work correctly") {
    metricSink.start()
    Thread.sleep(100) // Wait for scheduler to start

    metricSink.stop()
    // Verify scheduler is shutdown
  }

  override def afterAll(): Unit = {
    if (metricSink != null) {
      metricSink.stop()
    }
  }

  private def createGauge(value: Array[Double]): Gauge[Double] = {
    new Gauge[Double] {
      override def getValue: Double = value(guageIndex)
    }
  }
}
