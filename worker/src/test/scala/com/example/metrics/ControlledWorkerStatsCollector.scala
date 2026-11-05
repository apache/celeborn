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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.service.deploy.worker.metrics.{ScaleMetricCollector, ScaleMetricCollectorContext}

class ControlledWorkerStatsCollector(conf: CelebornConf) extends ScaleMetricCollector {
  val initializations = new AtomicInteger()
  private val observations = new LinkedBlockingQueue[Option[WorkerStats]]()
  @volatile private var stopped = false

  override def init(context: ScaleMetricCollectorContext): Unit = {
    initializations.incrementAndGet()
    ControlledWorkerStatsCollector.instances.add(this)
  }

  def report(stats: Option[WorkerStats]): Unit = observations.add(stats)

  override def currentWorkerStats(): Option[WorkerStats] = {
    if (stopped) return None
    // The test controls heartbeat progress through the collector's public extension interface.
    val observation = observations.poll(30, TimeUnit.SECONDS)
    require(observation != null, "Timed out waiting for the test's next observation")
    observation
  }

  override def stop(): Unit = {
    stopped = true
    observations.offer(None)
  }
}

object ControlledWorkerStatsCollector {
  val instances = new LinkedBlockingQueue[ControlledWorkerStatsCollector]()
}
