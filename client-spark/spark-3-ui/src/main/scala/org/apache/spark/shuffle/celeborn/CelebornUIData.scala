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
import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVIndex

/**
 * KVStore entity holding Celeborn build/version info for the UI summary.
 *  Singleton: keyed by the class name.
 */
private[celeborn] class CelebornBuildInfoUIData(val info: Seq[(String, String)]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornBuildInfoUIData].getName
}

/**
 * Per-shuffle assignment record. `appShuffleId` is the natural key (multiple rows).
 *  @KVIndexParam makes appShuffleId the primary index; no separate @KVIndex id needed
 *  (having both causes "Duplicate index __main__").
 */
private[celeborn] class CelebornShuffleAssignmentUIData(
    @KVIndexParam val appShuffleId: Int,
    val celebornShuffleId: Int,
    val workers: java.util.List[String],
    val numPartitions: Int,
    val timestamp: Long)

/** Snapshot of per-policy fallback counts. Singleton, overwritten on each fallback. */
private[celeborn] class CelebornFallbackStatsUIData(
    val counts: java.util.Map[String, java.lang.Long]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornFallbackStatsUIData].getName
}

/**
 * Reassign status snapshot: whether partition split / block send failure / stage retry
 *  have been triggered. Singleton, overwritten on each reassign event. Mirrors Uniffle's
 *  ReassignInfoUIData.
 */
private[celeborn] class CelebornReassignStatsUIData(
    val partitionSplit: Boolean,
    val reviveTriggered: Boolean,
    val stageRetry: Boolean,
    val timestamp: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornReassignStatsUIData].getName
}

/** Per-shuffle write metrics aggregated from onTaskEnd TaskMetrics. Keyed by shuffleDepId. */
private[celeborn] class CelebornAggregatedWriteMetricsUIData(
    val metrics: java.util.Map[Int, AggregatedShuffleWriteMetric]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornAggregatedWriteMetricsUIData].getName
}

/**
 * Per-shuffle write metrics (sourced from Spark native ShuffleWriteMetrics).
 *  Mutable fields updated by the single status-queue thread; serialized as a snapshot.
 */
private[celeborn] class AggregatedShuffleWriteMetric(
    var bytesWritten: Long,
    var recordsWritten: Long,
    var writeTimeMs: Long) {
  def this() = this(0L, 0L, 0L)
}

/** Per-shuffle read metrics aggregated from CelebornReadMetricsEvent. Keyed by shuffleId. */
private[celeborn] class CelebornAggregatedReadMetricsUIData(
    val metrics: java.util.Map[Int, AggregatedShuffleReadMetric]) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornAggregatedReadMetricsUIData].getName
}

/**
 * Per-shuffle read metrics: bytes/chunks come from the event's WorkerReadCost entries,
 *  fetchTimeMs sums the read-path timing breakdown (chunk wait + decompress +
 *  deserialize + copy). Mutable fields updated by the single status-queue thread;
 *  serialized as a snapshot. Mirrors [[AggregatedShuffleWriteMetric]].
 */
private[celeborn] class AggregatedShuffleReadMetric(
    var bytesRead: Long,
    var chunkCount: Long,
    var fetchTimeMs: Long) {
  def this() = this(0L, 0L, 0L)
}

/**
 * Aggregated write-path timing breakdown (ms) across all tasks, mirroring Uniffle's
 *  ShuffleWriteTimes. Singleton, overwritten on each CelebornWriteMetricsEvent.
 */
private[celeborn] class CelebornWriteTimesUIData(
    val copyTimeMs: Long,
    val serializeTimeMs: Long,
    val compressTimeMs: Long,
    val queueWaitTimeMs: Long,
    val queueStallTimeMs: Long,
    val inflightWaitTimeMs: Long,
    val drainWaitTimeMs: Long,
    val slowPushCount: Long,
    val maxPushRttMs: Long,
    val uncompressedBytes: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornWriteTimesUIData].getName
}

/**
 * Per-worker push stats aggregated from CelebornWriteMetricsEvent's PushWorkerStats.
 *  `workerId` is the natural key (multiple rows).
 */
private[celeborn] class CelebornPerWorkerWriteStatsUIData(
    @KVIndexParam val workerId: String,
    val pushCount: Long,
    val pushBytes: Long,
    val totalPushRttNanos: Long,
    val softSplitCount: Long,
    val hardSplitCount: Long,
    val primaryCongestedCount: Long,
    val replicaCongestedCount: Long,
    val lastPushFailureReason: String)

/** Aggregated read-path timing breakdown (ms) from CelebornReadMetricsEvent. Singleton. */
private[celeborn] class CelebornReadTimesUIData(
    val decompressTimeMs: Long,
    val chunkWaitTimeMs: Long,
    val deserializeTimeMs: Long,
    val copyTimeMs: Long,
    val retryCount: Long,
    val retryWaitTimeMs: Long,
    val peerSwitchCount: Long,
    val excludeCount: Long,
    val slowChunkCount: Long,
    val maxChunkRttMs: Long) {
  @JsonIgnore
  @KVIndex
  def id: String = classOf[CelebornReadTimesUIData].getName
}

/** Per-worker read cost aggregated from CelebornReadMetricsEvent. `workerId` is the key. */
private[celeborn] class CelebornPerWorkerReadStatsUIData(
    @KVIndexParam val workerId: String,
    val chunkCount: Long,
    val bytes: Long,
    val totalRttNanos: Long,
    val maxRttNanos: Long)
