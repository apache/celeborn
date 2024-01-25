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

import java.util.concurrent.{ExecutorService, ThreadPoolExecutor}

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
      "active_thread_count",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getActiveCount
        }
      })
    addGauge(
      "pending_task_count",
      label,
      new Gauge[Long] {
        override def getValue: Long = {
          threadPoolExecutor.getQueue.size()
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
    removeGauge("active_thread_count", label)
    removeGauge("pending_task_count", label)
    removeGauge("pool_size", label)
    removeGauge("core_pool_size", label)
    removeGauge("maximum_pool_size", label)
    removeGauge("largest_pool_size", label)
    removeGauge("is_terminating", label)
    removeGauge("is_terminated", label)
    removeGauge("is_shutdown", label)
    removeGauge("thread_count", label)
    removeGauge("thread_is_terminated_count", label)
    removeGauge("thread_is_shutdown_count", label)
  }

  def registerSource(threadPoolName: String, threads: Array[ExecutorService]): Unit = {
    val label = Map("threadPool" -> threadPoolName)
    addGauge(
      "thread_count",
      label,
      new Gauge[Int] {
        override def getValue: Int = {
          threads.size
        }
      })
    addGauge(
      "thread_is_terminated_count",
      label,
      new Gauge[Int] {
        override def getValue: Int = {
          threads.count(_.isTerminated)
        }
      })
    addGauge(
      "thread_is_shutdown_count",
      label,
      new Gauge[Int] {
        override def getValue: Int = {
          threads.count(_.isShutdown)
        }
      })
  }
}

object ThreadPoolSource {
  var threadPoolSource: Option[ThreadPoolSource] = None

  def apply(conf: CelebornConf, role: String): ThreadPoolSource = {
    if (threadPoolSource.isEmpty) {
      ThreadPoolSource.getClass.synchronized {
        if (threadPoolSource.isEmpty) {
          threadPoolSource = Option(new ThreadPoolSource(conf, role))
        }
      }
    }
    threadPoolSource.get
  }

  def registerSource(threadPoolName: String, threadPoolExecutor: ThreadPoolExecutor): Unit = {
    threadPoolSource.foreach(_.registerSource(threadPoolName, threadPoolExecutor))
  }

  def registerSource(threadPoolName: String, threads: Array[ExecutorService]): Unit = {
    threadPoolSource.foreach(_.registerSource(threadPoolName, threads))
  }

  def unregisterSource(threadPoolName: String): Unit = {
    threadPoolSource.foreach(_.unregisterSource(threadPoolName))
  }
}
