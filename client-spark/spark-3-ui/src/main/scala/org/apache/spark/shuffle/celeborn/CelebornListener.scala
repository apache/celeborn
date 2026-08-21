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

package org.apache.spark.shuffle.celeborn

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.util.kvstore.KVStore

/**
 * Collects Celeborn shuffle metrics into the Spark KVStore for live UI and
 * HistoryServer replay.
 */
private[celeborn] class CelebornListener(
    val kvstore: KVStore,
    val conf: SparkConf)
  extends SparkListener with Logging {

  private val totalWriteBytes = new AtomicLong(0L)
  private val totalWriteTimeMs = new AtomicLong(0L)
  private val totalReadBytes = new AtomicLong(0L)
  private val totalFetchWaitTimeMs = new AtomicLong(0L)
  private val totalTaskDurationMs = new AtomicLong(0L)

  private val lastUpdateTimestamp = new AtomicLong(-1L)
  private val updateIntervalMillis = 5000L

  def register(sc: org.apache.spark.SparkContext): Unit = {
    sc.addSparkListener(this)
    logInfo("CelebornListener registered successfully")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    Option(taskEnd.taskMetrics).foreach { metrics =>
      totalWriteBytes.addAndGet(metrics.shuffleWriteMetrics.bytesWritten)
      // writeTime is in nanoseconds; normalize to ms.
      totalWriteTimeMs.addAndGet(metrics.shuffleWriteMetrics.writeTime / 1000000L)
      totalReadBytes.addAndGet(metrics.shuffleReadMetrics.totalBytesRead)
      totalFetchWaitTimeMs.addAndGet(metrics.shuffleReadMetrics.fetchWaitTime)
      totalTaskDurationMs.addAndGet(taskEnd.taskInfo.duration)
    }
    mayUpdate()
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    val celebornProps = environmentUpdate.environmentDetails
      .getOrElse("Spark Properties", Seq.empty)
      .filter { case (k, _) => k.startsWith("spark.celeborn.") }
      .sortBy(_._1)
    if (celebornProps.nonEmpty) {
      kvstore.write(new CelebornPropertiesUIData(celebornProps.toList))
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    mayUpdate(force = true)
    logInfo("CelebornListener: application ended, final flush completed")
  }

  private def mayUpdate(force: Boolean = false): Unit = {
    val now = System.currentTimeMillis()
    val last = lastUpdateTimestamp.get()
    if (!force && (last != -1L && (now - last) < updateIntervalMillis)) {
      return
    }
    if (lastUpdateTimestamp.compareAndSet(last, now) || force) {
      flushAggregations()
    }
  }

  private def flushAggregations(): Unit = {
    try {
      kvstore.write(AggregatedTaskInfoUIData(
        totalWriteBytes.get(),
        totalWriteTimeMs.get(),
        totalReadBytes.get(),
        totalFetchWaitTimeMs.get(),
        totalTaskDurationMs.get()))
    } catch {
      case e: Exception =>
        logWarning("Failed to flush CelebornListener aggregations", e)
    }
  }
}
