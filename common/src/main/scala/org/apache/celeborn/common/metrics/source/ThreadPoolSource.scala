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
    conf: CelebornConf,
    role: String)
  extends AbstractSource(conf, role) {
  override val sourceName = "ThreadPool"
  def registerSource(
      threadPoolName: String,
      threadPoolExecutor: ThreadPoolExecutor): Unit = {
    val label = Map("threadPool" -> threadPoolName)
    addGauge(
      "active_count",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getActiveCount
        }
      })
    addGauge(
      "completed_task_count",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getCompletedTaskCount
        }
      })
    addGauge(
      "task_count",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getTaskCount
        }
      })
    addGauge(
      "pool_size",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getPoolSize
        }
      })
    addGauge(
      "core_pool_size",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getCorePoolSize
        }
      })
    addGauge(
      "maximum_pool_size",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getMaximumPoolSize
        }
      })
    addGauge(
      "largest_pool_size",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getLargestPoolSize
        }
      })
    addGauge(
      "remain_queue_capacity",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getQueue.remainingCapacity()
        }
      })
    addGauge(
      "is_terminating",
      label,
      new Gauge[Boolean] {
        override def getValue: Boolean = {
          threadPoolExecutor.isTerminating
        }
      })
    addGauge(
      "is_terminated",
      label,
      new Gauge[Boolean] {
        override def getValue: Boolean = {
          threadPoolExecutor.isTerminated
        }
      })
    addGauge(
      "is_shutdown",
      label,
      new Gauge[Boolean] {
        override def getValue: Boolean = {
          threadPoolExecutor.isShutdown
        }
      })
  }

  def unregisterSource(threadPoolName: String): Unit = {
    val label = Map("threadPool" -> threadPoolName)
    removeGauge("active_count", label)
    removeGauge("completed_task_count", label)
    removeGauge("task_count", label)
    removeGauge("pool_size", label)
    removeGauge("core_pool_size", label)
    removeGauge("maximum_pool_size", label)
    removeGauge("largest_pool_size", label)
    removeGauge("remain_queue_capacity", label)
    removeGauge("is_terminating", label)
    removeGauge("is_terminated", label)
    removeGauge("is_shutdown", label)
  }
}
