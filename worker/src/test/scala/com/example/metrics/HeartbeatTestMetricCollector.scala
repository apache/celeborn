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

package com.example.metrics

import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.service.deploy.worker.metrics.{ScaleMetricCollector, ScaleMetricCollectorContext}

// Keep this collector outside org.apache.celeborn to verify the public extension API.
class HeartbeatTestMetricCollector(conf: CelebornConf) extends ScaleMetricCollector {
  override def init(context: ScaleMetricCollectorContext): Unit = {
    assert(context.maxDirectMemory > 0)
    assert(context.memoryUsage >= 0)
    val diskUsage = context.localDiskUsage()
    assert(diskUsage.usableBytes >= 0)
    assert(diskUsage.effectiveCapacityBytes >= 0)
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
    "CustomMetric" -> 1.0))
  val starts = new AtomicInteger()
  val stops = new AtomicInteger()
  @volatile var failRead = false
}
