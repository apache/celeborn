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
import org.apache.spark.shuffle.celeborn.events._
import org.apache.spark.shuffle.celeborn.ui.CelebornUIUtils
import org.apache.spark.status.{ElementTrackingStore, KVUtils}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

/**
 * Collects Celeborn shuffle metrics into the Spark KVStore for live UI and
 * HistoryServer replay.
 *
 * When `requirePluginOptIn` is true (History Server replay), collection stays
 * disabled until the application's recorded `spark.plugins` or
 * `spark.plugins.defaultList` contains [[CelebornPlugin]], and an enable marker
 * is persisted so `setupUI` can decide whether to attach the Celeborn tab.
 *
 * Data sources:
 *  - onTaskEnd: Spark native TaskMetrics (already written by Celeborn writer/reader),
 *    aggregated globally and per shuffle dependency.
 *  - onOtherEvent: CelebornBuildInfoEvent / CelebornShuffleAssignmentEvent /
 *    CelebornFallbackEvent / CelebornReassignEvent / CelebornWriteMetricsEvent /
 *    CelebornReadMetricsEvent posted from the driver-side plugin / SparkShuffleManager /
 *    FallbackPolicyRunner and forwarded through the MapperEnd / read-metrics RPCs.
 *    These custom events enter the event log via the status queue, so HistoryServer
 *    replay rebuilds them here as well.
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

  // Per-shuffle write aggregation (key = shuffleDepId, only ShuffleMapStage has one).
  private val perShuffleWrite =
    new java.util.concurrent.ConcurrentHashMap[Int, AggregatedShuffleWriteMetric]
  // Per-shuffle read aggregation (key = shuffleId from CelebornReadMetricsEvent), so the
  // read side can be joined with perShuffleWrite on the per-shuffle UI table.
  private val perShuffleRead =
    new java.util.concurrent.ConcurrentHashMap[Int, AggregatedShuffleReadMetric]
  // Aggregated write-path timing breakdown (ms) from CelebornWriteMetricsEvent. Uses AtomicLong
  // so concurrent events on the status queue accumulate safely.
  private val aggCopyTimeMs = new AtomicLong(0L)
  private val aggSerializeTimeMs = new AtomicLong(0L)
  private val aggCompressTimeMs = new AtomicLong(0L)
  private val aggQueueWaitTimeMs = new AtomicLong(0L)
  private val aggQueueStallTimeMs = new AtomicLong(0L)
  private val aggInflightWaitTimeMs = new AtomicLong(0L)
  private val aggDrainWaitTimeMs = new AtomicLong(0L)
  private val aggSlowPushCount = new AtomicLong(0L)
  private val aggMaxPushRttMs = new AtomicLong(0L)
  private val aggUncompressedBytes = new AtomicLong(0L)
  // Per-worker push stats (workerId -> mutable accumulator), merged from each event.
  private val perWorkerWriteStats =
    new java.util.concurrent.ConcurrentHashMap[String, PerWorkerWriteAccumulator]
  // Aggregated read-path timing breakdown (ms) from CelebornReadMetricsEvent.
  private val aggDecompressTimeMs = new AtomicLong(0L)
  private val aggChunkWaitTimeMs = new AtomicLong(0L)
  private val aggDeserializeTimeMs = new AtomicLong(0L)
  private val aggCopyTimeMsRead = new AtomicLong(0L)
  private val aggRetryCount = new AtomicLong(0L)
  private val aggRetryWaitTimeMs = new AtomicLong(0L)
  private val aggPeerSwitchCount = new AtomicLong(0L)
  private val aggExcludeCount = new AtomicLong(0L)
  private val aggSlowChunkCount = new AtomicLong(0L)
  private val aggMaxChunkRttMs = new AtomicLong(0L)
  private val perWorkerReadStats =
    new java.util.concurrent.ConcurrentHashMap[String, PerWorkerReadAccumulator]
  // stageId -> shuffleDepId (only ShuffleMapStage has a shuffleDepId). Instance-level (not
  // companion-object static) so that different applications replayed on a shared HistoryServer
  // do not cross-contaminate each other's stageId namespace.
  private val stageToShuffleMappings =
    new java.util.concurrent.ConcurrentHashMap[Int, Int]()

  private val lastUpdateTimestamp = new AtomicLong(-1L)
  private val updateIntervalMillis = 5000L

  // Cap the per-shuffle assignment rows to bound KVStore / event-log growth on long-running
  // jobs with many shuffles (mirrors Gluten's UI_RETAINED_EXECUTIONS trigger and Spark's
  // retainedStages). When the count exceeds the threshold, evict the oldest rows. Read via
  // the CelebornConf entry so the documented `spark.celeborn.client.spark.ui.retainedShuffles`
  // key (and the entry's default and `retainedShuffle` alias handling) applies; a bare-key
  // SparkConf lookup never matches the user-set key because SparkConf does exact matching.
  private val retainedShuffles: Int =
    CelebornUIUtils.celebornConf(conf).clientSparkUIRetainedShuffles

  kvstore match {
    case tracking: ElementTrackingStore =>
      // Register a final flush so the last <=5s of aggregations (within the throttle window)
      // are persisted on store close / replay end, mirroring AppStatusListener's onFlush hook.
      // Without this, the trailing metrics would be lost from both the live store rebuild and
      // the HistoryServer replay.
      tracking.onFlush {
        mayUpdate(force = true)
      }
      tracking.addTrigger(classOf[CelebornShuffleAssignmentUIData], retainedShuffles.toLong) {
        count => cleanupAssignments(tracking, count)
      }
    case _ => // InMemoryStore (tests) or other stores: no lifecycle hooks needed.
  }

  private def cleanupAssignments(tracking: ElementTrackingStore, count: Long): Unit = {
    val toDelete = count - retainedShuffles
    if (toDelete <= 0) {
      return
    }
    val view = tracking.view(classOf[CelebornShuffleAssignmentUIData])
    KVUtils.viewToSeq(view, toDelete.toInt)(_ => true).foreach { e =>
      tracking.delete(classOf[CelebornShuffleAssignmentUIData], e.appShuffleId)
    }
  }

  /**
   * Writes an entity through the trigger-checking path when the store supports it: the plain
   * KVStore.write bypasses ElementTrackingStore's eviction triggers, so the
   * retainedShuffles cap only takes effect via write(value, checkTriggers = true).
   */
  private def writeEntity(value: Any): Unit = kvstore match {
    case tracking: ElementTrackingStore =>
      tracking.write(value, checkTriggers = true)
    case _ =>
      kvstore.write(value)
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
      if (taskEnd.taskInfo != null) {
        totalTaskDurationMs.addAndGet(taskEnd.taskInfo.duration)
      }
      // Per-shuffle write attribution: ShuffleMapTask carries shuffle write and its stage
      // has a shuffleDepId mapped in onStageSubmitted.
      if (stageToShuffleMappings.containsKey(taskEnd.stageId)
        && taskEnd.taskType == "ShuffleMapTask") {
        val shuffleId = stageToShuffleMappings.get(taskEnd.stageId)
        val metric = perShuffleWrite.computeIfAbsent(
          shuffleId,
          _ => new AggregatedShuffleWriteMetric(0L, 0L, 0L))
        metric.bytesWritten += metrics.shuffleWriteMetrics.bytesWritten
        metric.recordsWritten += metrics.shuffleWriteMetrics.recordsWritten
        metric.writeTimeMs += metrics.shuffleWriteMetrics.writeTime / 1000000L
      }
    }
    mayUpdate()
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    // ShuffleMapStage carries shuffleDepId = the Spark shuffle dependency id; result stages
    // have None. Used in onTaskEnd to attribute write metrics to a shuffle.
    stageSubmitted.stageInfo.shuffleDepId.foreach { sid =>
      stageToShuffleMappings.put(stageSubmitted.stageInfo.stageId, sid.asInstanceOf[Int])
    }
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
    val celebornProps = Utils.redact(
      conf,
      sparkProps.filter { case (k, _) => k.startsWith("spark.celeborn.") })
      .sortBy(_._1)
    if (celebornProps.nonEmpty) {
      kvstore.write(new CelebornPropertiesUIData(celebornProps.toList))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case _: CelebornEvent if !pluginEnabled => // gated, same semantics as onTaskEnd
    case e: CelebornBuildInfoEvent =>
      kvstore.write(new CelebornBuildInfoUIData(e.info.toSeq.sortBy(_._1)))
      mayUpdate()
    case e: CelebornShuffleAssignmentEvent =>
      writeEntity(new CelebornShuffleAssignmentUIData(
        e.appShuffleId,
        e.celebornShuffleId,
        e.workers,
        e.numPartitions,
        e.timestamp))
      mayUpdate()
    case e: CelebornFallbackEvent =>
      kvstore.write(new CelebornFallbackStatsUIData(e.fallbackCounts))
      mayUpdate()
    case e: CelebornReassignEvent =>
      kvstore.write(new CelebornReassignStatsUIData(
        e.partitionSplit,
        e.reviveTriggered,
        e.stageRetry,
        e.timestamp))
      mayUpdate()
    case e: CelebornWriteMetricsEvent =>
      import scala.collection.JavaConverters._
      val w = e.writeMetrics
      aggCopyTimeMs.addAndGet(w.copyTimeMs)
      aggSerializeTimeMs.addAndGet(w.serializeTimeMs)
      aggCompressTimeMs.addAndGet(w.compressTimeMs)
      aggQueueWaitTimeMs.addAndGet(w.queueWaitTimeMs)
      aggQueueStallTimeMs.addAndGet(w.queueStallTimeMs)
      aggInflightWaitTimeMs.addAndGet(w.inflightWaitTimeMs)
      aggDrainWaitTimeMs.addAndGet(w.drainWaitTimeMs)
      aggSlowPushCount.addAndGet(w.slowPushCount)
      aggMaxPushRttMs.accumulateAndGet(w.maxPushRttMs, math.max)
      aggUncompressedBytes.addAndGet(w.uncompressedBytes)
      // Merge per-worker push stats.
      e.pushWorkerStats.asScala.foreach { s =>
        val acc =
          perWorkerWriteStats.computeIfAbsent(s.workerId, _ => new PerWorkerWriteAccumulator)
        acc.merge(s)
      }
      mayUpdate()
    case e: CelebornReadMetricsEvent =>
      import scala.collection.JavaConverters._
      val rm = e.readMetrics
      aggDecompressTimeMs.addAndGet(rm.decompressTimeMs)
      aggChunkWaitTimeMs.addAndGet(rm.chunkWaitTimeMs)
      aggDeserializeTimeMs.addAndGet(rm.deserializeTimeMs)
      aggCopyTimeMsRead.addAndGet(rm.copyTimeMs)
      aggRetryCount.addAndGet(rm.retryCount)
      aggRetryWaitTimeMs.addAndGet(rm.retryWaitTimeMs)
      aggPeerSwitchCount.addAndGet(rm.peerSwitchCount)
      aggExcludeCount.addAndGet(rm.excludeCount)
      aggSlowChunkCount.addAndGet(rm.slowChunkCount)
      aggMaxChunkRttMs.accumulateAndGet(rm.maxChunkRttMs, math.max)
      e.workerReadCosts.asScala.foreach { w =>
        val acc = perWorkerReadStats.computeIfAbsent(w.workerId, _ => new PerWorkerReadAccumulator)
        acc.merge(w)
      }
      // Per-shuffle read aggregation: bytes/chunks from WorkerReadCost, fetch time from
      // the read-path timing breakdown. Same accumulation pattern as perShuffleWrite.
      val shuffleRead = perShuffleRead.computeIfAbsent(
        e.shuffleId,
        _ => new AggregatedShuffleReadMetric(0L, 0L, 0L))
      shuffleRead.fetchTimeMs +=
        rm.chunkWaitTimeMs + rm.decompressTimeMs + rm.deserializeTimeMs + rm.copyTimeMs
      e.workerReadCosts.asScala.foreach { w =>
        shuffleRead.bytesRead += w.bytes
        shuffleRead.chunkCount += w.chunkCount
      }
      mayUpdate()
    case _ => // ignore unknown events
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
    import scala.collection.JavaConverters._
    try {
      val writeSnapshot = new java.util.HashMap[Int, AggregatedShuffleWriteMetric]()
      perShuffleWrite.asScala.foreach { case (k, v) =>
        writeSnapshot.put(
          k,
          new AggregatedShuffleWriteMetric(v.bytesWritten, v.recordsWritten, v.writeTimeMs))
      }
      kvstore.write(new CelebornAggregatedWriteMetricsUIData(writeSnapshot))
      // Read-side counterpart of writeSnapshot, keyed by shuffleId.
      val readSnapshot = new java.util.HashMap[Int, AggregatedShuffleReadMetric]()
      perShuffleRead.asScala.foreach { case (k, v) =>
        readSnapshot.put(
          k,
          new AggregatedShuffleReadMetric(v.bytesRead, v.chunkCount, v.fetchTimeMs))
      }
      kvstore.write(new CelebornAggregatedReadMetricsUIData(readSnapshot))
      kvstore.write(AggregatedTaskInfoUIData(
        totalWriteBytes.get(),
        totalWriteTimeMs.get(),
        totalReadBytes.get(),
        totalFetchWaitTimeMs.get(),
        totalTaskDurationMs.get()))
      // Write-path timing breakdown (ms) + per-worker push stats, from CelebornWriteMetricsEvent.
      kvstore.write(new CelebornWriteTimesUIData(
        aggCopyTimeMs.get(),
        aggSerializeTimeMs.get(),
        aggCompressTimeMs.get(),
        aggQueueWaitTimeMs.get(),
        aggQueueStallTimeMs.get(),
        aggInflightWaitTimeMs.get(),
        aggDrainWaitTimeMs.get(),
        aggSlowPushCount.get(),
        aggMaxPushRttMs.get(),
        aggUncompressedBytes.get()))
      perWorkerWriteStats.asScala.foreach { case (workerId, acc) =>
        kvstore.write(new CelebornPerWorkerWriteStatsUIData(
          workerId,
          acc.pushCount.get(),
          acc.pushBytes.get(),
          acc.totalPushRttNanos.get(),
          acc.softSplitCount.get(),
          acc.hardSplitCount.get(),
          acc.primaryCongestedCount.get(),
          acc.replicaCongestedCount.get(),
          acc.lastPushFailureReason))
      }
      // Read-path timing breakdown (ms) + per-worker read stats, from CelebornReadMetricsEvent.
      kvstore.write(new CelebornReadTimesUIData(
        aggDecompressTimeMs.get(),
        aggChunkWaitTimeMs.get(),
        aggDeserializeTimeMs.get(),
        aggCopyTimeMsRead.get(),
        aggRetryCount.get(),
        aggRetryWaitTimeMs.get(),
        aggPeerSwitchCount.get(),
        aggExcludeCount.get(),
        aggSlowChunkCount.get(),
        aggMaxChunkRttMs.get()))
      perWorkerReadStats.asScala.foreach { case (workerId, acc) =>
        kvstore.write(new CelebornPerWorkerReadStatsUIData(
          workerId,
          acc.chunkCount.get(),
          acc.bytes.get(),
          acc.totalRttNanos.get(),
          acc.maxRttNanos.get()))
      }
    } catch {
      case e: Exception =>
        logWarning("Failed to flush CelebornListener aggregations", e)
    }
  }
}

private[celeborn] object CelebornListener extends Logging {

  /** Register a listener to the status queue so its events enter the event log. */
  def register(sc: org.apache.spark.SparkContext): Unit = {
    val listener = new CelebornListener(sc.statusStore.store, sc.conf)
    sc.listenerBus.addToStatusQueue(listener)
    logInfo("CelebornListener registered to the status queue")
  }
}

private[celeborn] class PerWorkerWriteAccumulator {
  private[celeborn] val pushCount = new AtomicLong(0)
  private[celeborn] val pushBytes = new AtomicLong(0)
  private[celeborn] val totalPushRttNanos = new AtomicLong(0)
  private[celeborn] val softSplitCount = new AtomicLong(0)
  private[celeborn] val hardSplitCount = new AtomicLong(0)
  private[celeborn] val primaryCongestedCount = new AtomicLong(0)
  private[celeborn] val replicaCongestedCount = new AtomicLong(0)
  @volatile private[celeborn] var lastPushFailureReason: String = ""

  def merge(s: org.apache.celeborn.common.protocol.message.PushWorkerStats): Unit = {
    pushCount.addAndGet(s.pushCount)
    pushBytes.addAndGet(s.pushBytes)
    totalPushRttNanos.addAndGet(s.totalPushRttNanos)
    softSplitCount.addAndGet(s.softSplitCount)
    hardSplitCount.addAndGet(s.hardSplitCount)
    primaryCongestedCount.addAndGet(s.primaryCongestedCount)
    replicaCongestedCount.addAndGet(s.replicaCongestedCount)
    if (s.lastPushFailureReason != null && s.lastPushFailureReason.nonEmpty) {
      lastPushFailureReason = s.lastPushFailureReason
    }
  }
}

private[celeborn] class PerWorkerReadAccumulator {
  private[celeborn] val chunkCount = new AtomicLong(0)
  private[celeborn] val bytes = new AtomicLong(0)
  private[celeborn] val totalRttNanos = new AtomicLong(0)
  private[celeborn] val maxRttNanos = new AtomicLong(0)

  def merge(w: org.apache.celeborn.common.protocol.message.WorkerReadCost): Unit = {
    chunkCount.addAndGet(w.chunkCount)
    bytes.addAndGet(w.bytes)
    totalRttNanos.addAndGet(w.totalRttNanos)
    maxRttNanos.accumulateAndGet(w.maxRttNanos, math.max)
  }
}
