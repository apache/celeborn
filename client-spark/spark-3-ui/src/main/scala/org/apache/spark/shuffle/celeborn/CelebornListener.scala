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
 *
 * When `requirePluginOptIn` is true (History Server replay), collection stays
 * disabled until the application's recorded `spark.plugins` or
 * `spark.plugins.defaultList` contains [[CelebornPlugin]], and an enable marker
 * is persisted so `setupUI` can decide whether to attach the Celeborn tab.
 */
private[celeborn] class CelebornListener(
    val kvstore: KVStore,
    val conf: SparkConf,
    requirePluginOptIn: Boolean = false)
  extends SparkListener with Logging {

  @volatile private var pluginEnabled = !requirePluginOptIn

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
    if (!pluginEnabled) {
      return
    }
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

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    // Flush per-job so that tasks finishing within the throttle interval of the
    // previous flush are not lost when the application stays alive but idle.
    mayUpdate(force = true)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    val sparkProps = environmentUpdate.environmentDetails
      .getOrElse("Spark Properties", Seq.empty)
    if (!pluginEnabled) {
      val pluginClass = classOf[CelebornPlugin].getName
      // Spark loads plugins from both keys: `spark.plugins.defaultList` allows a
      // default plugin list in the config file that `spark.plugins` does not overwrite.
      val optedIn = sparkProps.exists { case (k, v) =>
        (k == "spark.plugins" || k == "spark.plugins.defaultList") &&
          v.split(",").exists(_.trim == pluginClass)
      }
      if (optedIn) {
        pluginEnabled = true
        kvstore.write(new CelebornExtensionEnabledUIData())
      } else {
        return
      }
    }
    val celebornProps = sparkProps
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

  /** Flushes the current aggregations immediately, bypassing the throttle. */
  def flush(): Unit = {
    lastUpdateTimestamp.set(System.currentTimeMillis())
    flushAggregations()
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
    if (!pluginEnabled) {
      return
    }
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
