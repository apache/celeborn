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

package org.apache.celeborn.common.metrics.source

import java.util.concurrent.ThreadPoolExecutor

import com.codahale.metrics.Gauge
import org.apache.celeborn.common.CelebornConf

class ThreadPoolSource(
  threadPoolName: String,
  threadPoolExecutor: ThreadPoolExecutor,
  conf: CelebornConf,
  role: String)
  extends AbstractSource(conf, role) {
  override val sourceName = s"THREAD_POOL_$threadPoolName"

  addGauge(
    s"${threadPoolName}_active_count",
    new Gauge[Long] {
      override def getValue: Long = {
        threadPoolExecutor.getActiveCount
      }
    })

  addGauge(
    s"${threadPoolName}_pool_size",
    new Gauge[Long] {
      override def getValue: Long = {
        threadPoolExecutor.getPoolSize
      }
    })
  addGauge(
    s"${threadPoolName}_core_pool_size",
    new Gauge[Long] {
      override def getValue: Long = {
        threadPoolExecutor.getCorePoolSize
      }
    })
  addGauge(
    s"${threadPoolName}_remain_queue_capacity",
    new Gauge[Long] {
      override def getValue: Long = {
        threadPoolExecutor.getQueue.remainingCapacity()
      }
    })
  addGauge(
    s"${threadPoolName}_is_terminating",
    new Gauge[Boolean] {
      override def getValue: Boolean = {
        threadPoolExecutor.isTerminating
      }
    })
  addGauge(
    s"${threadPoolName}_is_terminated",
    new Gauge[Boolean] {
      override def getValue: Boolean = {
        threadPoolExecutor.isTerminated
      }
    })
  addGauge(
    s"${threadPoolName}_is_shutdown",
    new Gauge[Boolean] {
      override def getValue: Boolean = {
        threadPoolExecutor.isShutdown
      }
    })
  startCleaner()
}

