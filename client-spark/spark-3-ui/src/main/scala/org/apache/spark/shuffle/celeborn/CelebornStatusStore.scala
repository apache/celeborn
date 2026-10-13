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

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.util.kvstore.{KVIndex, KVStore, KVStoreView}

private[celeborn] case class AggregatedTaskInfoUIData(
    shuffleWriteBytes: Long,
    shuffleWriteTimeMs: Long,
    shuffleReadBytes: Long,
    shuffleFetchWaitTimeMs: Long,
    taskDurationMs: Long) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[AggregatedTaskInfoUIData].getName
}

private[celeborn] class CelebornPropertiesUIData(
    val info: Seq[(String, String)]) {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornPropertiesUIData].getName
}

/**
 * Marker persisted during History Server replay when the application's recorded
 * `spark.plugins` or `spark.plugins.defaultList` contains [[CelebornPlugin]].
 * `setupUI` runs on a fresh plugin
 * instance (FsHistoryProvider loads plugins again after replay), so the opt-in
 * state has to be passed through the KVStore rather than in memory.
 */
private[celeborn] class CelebornExtensionEnabledUIData {

  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornExtensionEnabledUIData].getName
}

private[celeborn] class CelebornStatusStore(store: KVStore) {

  def aggregatedTaskInfo(): AggregatedTaskInfoUIData = {
    val kClass = classOf[AggregatedTaskInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => AggregatedTaskInfoUIData(0L, 0L, 0L, 0L, 0L)
    }
  }

  def celebornProperties(): CelebornPropertiesUIData = {
    val kClass = classOf[CelebornPropertiesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornPropertiesUIData(Seq.empty)
    }
  }

  def extensionEnabled(): Boolean = {
    val kClass = classOf[CelebornExtensionEnabledUIData]
    try {
      store.read(kClass, kClass.getName)
      true
    } catch {
      case _: NoSuchElementException => false
    }
  }

  /** Build info summary, or an empty entity if not yet written. */
  def buildInfo(): CelebornBuildInfoUIData = {
    val kClass = classOf[CelebornBuildInfoUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornBuildInfoUIData(Seq.empty)
    }
  }

  /** All recorded shuffle assignments (shuffle -> worker topology), newest last. */
  def assignmentInfos(): Seq[CelebornShuffleAssignmentUIData] = {
    viewToSeq(store.view(classOf[CelebornShuffleAssignmentUIData]))
  }

  /** Per-policy fallback counts snapshot, or empty if no fallback recorded. */
  def fallbackStats(): CelebornFallbackStatsUIData = {
    val kClass = classOf[CelebornFallbackStatsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornFallbackStatsUIData(new java.util.HashMap[String, java.lang.Long]())
    }
  }

  /** Reassign status snapshot, or all-false if no reassign recorded. */
  def reassignStats(): CelebornReassignStatsUIData = {
    val kClass = classOf[CelebornReassignStatsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornReassignStatsUIData(false, false, false, 0L)
    }
  }

  /** Aggregated write-path timing breakdown, or all-zero if none recorded. */
  def writeTimes(): CelebornWriteTimesUIData = {
    val kClass = classOf[CelebornWriteTimesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException => new CelebornWriteTimesUIData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  /** Per-worker push stats, newest last. */
  def perWorkerWriteStats(): Seq[CelebornPerWorkerWriteStatsUIData] = {
    viewToSeq(store.view(classOf[CelebornPerWorkerWriteStatsUIData]))
  }

  /** Aggregated read-path timing breakdown, or all-zero if none recorded. */
  def readTimes(): CelebornReadTimesUIData = {
    val kClass = classOf[CelebornReadTimesUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornReadTimesUIData(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    }
  }

  /** Per-worker read stats, newest last. */
  def perWorkerReadStats(): Seq[CelebornPerWorkerReadStatsUIData] = {
    viewToSeq(store.view(classOf[CelebornPerWorkerReadStatsUIData]))
  }

  /** Per-shuffle write metrics snapshot, or empty if none recorded. */
  def aggregatedWriteMetrics(): CelebornAggregatedWriteMetricsUIData = {
    val kClass = classOf[CelebornAggregatedWriteMetricsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornAggregatedWriteMetricsUIData(new java.util.HashMap[
          Int,
          AggregatedShuffleWriteMetric]())
    }
  }

  /** Per-shuffle read metrics snapshot, or empty if none recorded. */
  def aggregatedReadMetrics(): CelebornAggregatedReadMetricsUIData = {
    val kClass = classOf[CelebornAggregatedReadMetricsUIData]
    try {
      store.read(kClass, kClass.getName)
    } catch {
      case _: NoSuchElementException =>
        new CelebornAggregatedReadMetricsUIData(new java.util.HashMap[
          Int,
          AggregatedShuffleReadMetric]())
    }
  }

  private def viewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    import scala.collection.JavaConverters._
    org.apache.spark.util.Utils.tryWithResource(view.closeableIterator())(iter =>
      iter.asScala.toList)
  }
}
