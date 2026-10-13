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

import org.apache.spark.{SparkConf, Success, TaskState}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.celeborn.events._
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.protocol.message.{PushWorkerStats, ReadMetrics, WorkerReadCost, WriteMetrics}

@RunWith(classOf[JUnit4])
class CelebornListenerSuite {

  private val pluginClass = classOf[CelebornPlugin].getName

  private def newTaskEnd(
      writeBytes: Long,
      writeTimeMs: Long,
      fetchWaitMs: Long,
      durationMs: Long): SparkListenerTaskEnd = {
    val metrics = TaskMetrics.empty
    metrics.shuffleWriteMetrics.incBytesWritten(writeBytes)
    metrics.shuffleWriteMetrics.incWriteTime(writeTimeMs * 1000000L)
    metrics.shuffleReadMetrics.incFetchWaitTime(fetchWaitMs)
    val launchTime = 1000L
    val info = new TaskInfo(
      0L,
      0,
      0,
      launchTime,
      "exec-0",
      "localhost",
      TaskLocality.PROCESS_LOCAL,
      false)
    info.markFinished(TaskState.FINISHED, launchTime + durationMs)
    SparkListenerTaskEnd(0, 0, "ShuffleMapTask", Success, info, null, metrics)
  }

  private def envUpdate(sparkProps: (String, String)*): SparkListenerEnvironmentUpdate = {
    SparkListenerEnvironmentUpdate(Map("Spark Properties" -> sparkProps.toSeq))
  }

  @Test
  def flushOnJobEndWithinThrottleInterval(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(200L, 20L, 2L, 6L))
    // The second task lands inside the throttle window: only the first is persisted so far.
    assertEquals(100L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)

    listener.onJobEnd(SparkListenerJobEnd(0, 2000L, JobSucceeded))
    assertEquals(300L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)
    assertEquals(30L, statusStore.aggregatedTaskInfo().shuffleWriteTimeMs)
    assertEquals(3L, statusStore.aggregatedTaskInfo().shuffleFetchWaitTimeMs)
    assertEquals(11L, statusStore.aggregatedTaskInfo().taskDurationMs)
  }

  @Test
  def flushTriggerPersistsFinalValuesOnReplayClose(): Unit = {
    val conf = new SparkConf().set(ASYNC_TRACKING_ENABLED, false)
    val store = new InMemoryStore()
    val tracking = new ElementTrackingStore(store, conf)
    val statusStore = new CelebornStatusStore(tracking)
    val listener = new CelebornListener(tracking, conf, requirePluginOptIn = true)
    tracking.onFlush(listener.flush())

    listener.onEnvironmentUpdate(envUpdate("spark.plugins" -> pluginClass))
    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(200L, 20L, 2L, 6L))
    // Replay finishes within the throttle window without an ApplicationEnd event.
    tracking.close(false)

    assertEquals(300L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)
    assertEquals(11L, statusStore.aggregatedTaskInfo().taskDurationMs)
  }

  @Test
  def collectionGatedOnPluginOptIn(): Unit = {
    // Without the plugin in the recorded spark.plugins: nothing is collected or persisted.
    val store1 = new InMemoryStore()
    val statusStore1 = new CelebornStatusStore(store1)
    val listener1 = new CelebornListener(store1, new SparkConf(), requirePluginOptIn = true)
    listener1.onEnvironmentUpdate(envUpdate(
      "spark.celeborn.master.endpoints" -> "host:9097"))
    listener1.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener1.onJobEnd(SparkListenerJobEnd(0, 2000L, JobSucceeded))
    listener1.flush()
    assertFalse(statusStore1.extensionEnabled())
    assertEquals(0L, statusStore1.aggregatedTaskInfo().shuffleWriteBytes)
    assertTrue(statusStore1.celebornProperties().info.isEmpty)

    // With the plugin in the recorded spark.plugins: collection and tab marker are enabled.
    val store2 = new InMemoryStore()
    val statusStore2 = new CelebornStatusStore(store2)
    val listener2 = new CelebornListener(store2, new SparkConf(), requirePluginOptIn = true)
    listener2.onEnvironmentUpdate(envUpdate(
      "spark.plugins" -> s"com.example.OtherPlugin,$pluginClass",
      "spark.celeborn.master.endpoints" -> "host:9097"))
    listener2.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    assertTrue(statusStore2.extensionEnabled())
    assertEquals(100L, statusStore2.aggregatedTaskInfo().shuffleWriteBytes)
    assertTrue(statusStore2.celebornProperties().info.exists(
      _._1 == "spark.celeborn.master.endpoints"))
  }

  @Test
  def pluginOptInViaDefaultList(): Unit = {
    // The plugin can also be loaded via spark.plugins.defaultList (e.g. from the
    // Spark default config file); replay must recognize that as opt-in too.
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf(), requirePluginOptIn = true)
    listener.onEnvironmentUpdate(envUpdate(
      "spark.plugins.defaultList" -> s"com.example.OtherPlugin, $pluginClass"))
    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    assertTrue(statusStore.extensionEnabled())
    assertEquals(100L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)
  }

  @Test
  def sensitivePropertiesAreRedacted(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onEnvironmentUpdate(envUpdate(
      "spark.celeborn.ssl.rpc_service.trustStorePassword" -> "top-secret-password",
      "spark.celeborn.storage.oss.secret.key" -> "oss-secret-key",
      "spark.celeborn.master.endpoints" -> "host1:9097,host2:9097",
      "spark.executor.memory" -> "1g"))

    val props = statusStore.celebornProperties().info.toMap

    // Sensitive values are redacted, same as Spark's Environment tab.
    assertEquals(
      Utils.REDACTION_REPLACEMENT_TEXT,
      props("spark.celeborn.ssl.rpc_service.trustStorePassword"))
    assertEquals(Utils.REDACTION_REPLACEMENT_TEXT, props("spark.celeborn.storage.oss.secret.key"))

    // Ordinary Celeborn property remains visible.
    assertEquals("host1:9097,host2:9097", props("spark.celeborn.master.endpoints"))

    // Non-Celeborn properties are filtered out.
    assertFalse(props.contains("spark.executor.memory"))
  }

  @Test
  def customEventsGatedOnPluginOptIn(): Unit = {
    // Before opt-in: custom events are ignored, nothing is written to the store.
    val store1 = new InMemoryStore()
    val statusStore1 = new CelebornStatusStore(store1)
    val listener1 = new CelebornListener(store1, new SparkConf(), requirePluginOptIn = true)
    listener1.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
      java.util.Collections.emptyList[PushWorkerStats](),
      1L))
    listener1.onOtherEvent(CelebornShuffleAssignmentEvent(
      0,
      100,
      java.util.Arrays.asList("host1:9097"),
      8,
      1L))
    listener1.onOtherEvent(CelebornBuildInfoEvent(Map("Spark Version" -> "3.5")))
    assertEquals(0L, statusStore1.writeTimes().copyTimeMs)
    assertTrue(statusStore1.assignmentInfos().isEmpty)
    assertTrue(statusStore1.buildInfo().info.isEmpty)

    // After opt-in: the same events are collected.
    listener1.onEnvironmentUpdate(envUpdate("spark.plugins" -> pluginClass))
    listener1.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
      java.util.Collections.emptyList[PushWorkerStats](),
      1L))
    assertEquals(1L, statusStore1.writeTimes().copyTimeMs)
  }

  @Test
  def writeMetricsEventAggregatedToWriteTimes(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 90L, 1000L),
      java.util.Arrays.asList(
        new PushWorkerStats("host1:9097", 10L, 1000L, 2000000L, 1L, 2L, 3L, 4L, ""),
        new PushWorkerStats("host2:9097", 20L, 2000L, 4000000L, 0L, 1L, 0L, 1L, "boom")),
      1L))
    listener.flush()

    val writeTimes = statusStore.writeTimes()
    assertEquals(1L, writeTimes.copyTimeMs)
    assertEquals(2L, writeTimes.serializeTimeMs)
    assertEquals(3L, writeTimes.compressTimeMs)
    assertEquals(4L, writeTimes.queueWaitTimeMs)
    assertEquals(5L, writeTimes.queueStallTimeMs)
    assertEquals(6L, writeTimes.inflightWaitTimeMs)
    assertEquals(7L, writeTimes.drainWaitTimeMs)
    assertEquals(8L, writeTimes.slowPushCount)
    assertEquals(90L, writeTimes.maxPushRttMs)
    assertEquals(1000L, writeTimes.uncompressedBytes)

    val workers = statusStore.perWorkerWriteStats().map(w => (w.workerId, w.pushCount)).toMap
    assertEquals(2, workers.size)
    assertEquals(10L, workers("host1:9097"))
    assertEquals(20L, workers("host2:9097"))
    // Merging a second event for the same worker accumulates instead of replacing.
    listener.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L),
      java.util.Arrays.asList(new PushWorkerStats("host1:9097", 5L, 0L, 0L, 0L, 0L, 0L, 0L, "")),
      2L))
    listener.flush()
    assertEquals(
      15L,
      statusStore.perWorkerWriteStats().find(_.workerId == "host1:9097")
        .map(_.pushCount).getOrElse(0L))
  }

  @Test
  def readMetricsEventAggregatedToReadTimes(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornReadMetricsEvent(
      0,
      new ReadMetrics(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L, 19L, 200L),
      java.util.Arrays.asList(new WorkerReadCost("host1:9097", 30L, 3000L, 5000000L, 900000L)),
      1L))
    listener.flush()

    val readTimes = statusStore.readTimes()
    assertEquals(11L, readTimes.decompressTimeMs)
    assertEquals(12L, readTimes.chunkWaitTimeMs)
    assertEquals(13L, readTimes.deserializeTimeMs)
    assertEquals(14L, readTimes.copyTimeMs)
    assertEquals(15L, readTimes.retryCount)
    assertEquals(16L, readTimes.retryWaitTimeMs)
    assertEquals(17L, readTimes.peerSwitchCount)
    assertEquals(18L, readTimes.excludeCount)
    assertEquals(19L, readTimes.slowChunkCount)
    assertEquals(200L, readTimes.maxChunkRttMs)

    assertEquals(1, statusStore.perWorkerReadStats().size)
    assertEquals(30L, statusStore.perWorkerReadStats().head.chunkCount)
    assertEquals(3000L, statusStore.perWorkerReadStats().head.bytes)
  }

  @Test
  def readMetricsEventAggregatedPerShuffle(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    // Two events for shuffle 0 (accumulate) and one for shuffle 1. ReadMetrics field order:
    // decompressTimeMs, chunkWaitTimeMs, deserializeTimeMs, copyTimeMs, ...
    listener.onOtherEvent(CelebornReadMetricsEvent(
      0,
      new ReadMetrics(1L, 2L, 3L, 4L, 0L, 0L, 0L, 0L, 0L, 0L),
      java.util.Arrays.asList(new WorkerReadCost("host1:9097", 10L, 1000L, 100L, 100L)),
      1L))
    listener.onOtherEvent(CelebornReadMetricsEvent(
      1,
      new ReadMetrics(5L, 6L, 7L, 8L, 0L, 0L, 0L, 0L, 0L, 0L),
      java.util.Arrays.asList(new WorkerReadCost("host2:9097", 20L, 2000L, 100L, 100L)),
      2L))
    listener.onOtherEvent(CelebornReadMetricsEvent(
      0,
      new ReadMetrics(11L, 12L, 13L, 14L, 0L, 0L, 0L, 0L, 0L, 0L),
      java.util.Arrays.asList(new WorkerReadCost("host1:9097", 5L, 500L, 100L, 100L)),
      3L))
    listener.flush()

    val metrics = statusStore.aggregatedReadMetrics().metrics
    assertEquals(2, metrics.size())
    val shuffle0 = metrics.get(0)
    assertEquals(1500L, shuffle0.bytesRead)
    assertEquals(15L, shuffle0.chunkCount)
    // fetchTimeMs sums chunk wait + decompress + deserialize + copy across both events.
    assertEquals((1 + 2 + 3 + 4) + (11 + 12 + 13 + 14), shuffle0.fetchTimeMs)
    val shuffle1 = metrics.get(1)
    assertEquals(2000L, shuffle1.bytesRead)
    assertEquals(20L, shuffle1.chunkCount)
    assertEquals(5 + 6 + 7 + 8, shuffle1.fetchTimeMs)
  }

  @Test
  def aggregatedReadMetricsEmptyWhenNoReadEvents(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    // Reading before any flush must not throw and must return an empty map (the page
    // renders before the first flush on a live application).
    assertTrue(statusStore.aggregatedReadMetrics().metrics.isEmpty)

    val listener = new CelebornListener(store, new SparkConf())
    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.flush()
    assertTrue(statusStore.aggregatedReadMetrics().metrics.isEmpty)
  }

  @Test
  def retainedShufflesEvictsOldestAssignments(): Unit = {
    val conf = new SparkConf()
      .set(ASYNC_TRACKING_ENABLED, false)
      .set("celeborn.client.spark.ui.retainedShuffles", "2")
    val store = new InMemoryStore()
    val tracking = new ElementTrackingStore(store, conf)
    val statusStore = new CelebornStatusStore(tracking)
    val listener = new CelebornListener(tracking, conf)

    (1 to 4).foreach { i =>
      listener.onOtherEvent(CelebornShuffleAssignmentEvent(
        i,
        100 + i,
        java.util.Arrays.asList(s"host$i:9097"),
        8,
        i.toLong))
    }
    // The addTrigger evicts the oldest rows (smallest appShuffleId) once the count
    // exceeds the retained threshold of 2.
    assertEquals(2, statusStore.assignmentInfos().size)
    assertEquals(
      Set(3, 4),
      statusStore.assignmentInfos().map(_.appShuffleId).toSet)
    tracking.close(false)
  }

  @Test
  def customEventsReplayedFromEventLogSequence(): Unit = {
    // Simulates HistoryServer replay: opt-in marker first (via environment update),
    // then the full custom-event sequence, then an application end. Everything that
    // the live listener would persist must be rebuilt by the replayed listener.
    val conf = new SparkConf().set(ASYNC_TRACKING_ENABLED, false)
    val store = new InMemoryStore()
    val tracking = new ElementTrackingStore(store, conf)
    val statusStore = new CelebornStatusStore(tracking)
    val listener = new CelebornListener(tracking, conf, requirePluginOptIn = true)

    listener.onEnvironmentUpdate(envUpdate("spark.plugins" -> pluginClass))
    listener.onOtherEvent(CelebornBuildInfoEvent(Map("Spark Version" -> "3.5.8")))
    listener.onOtherEvent(CelebornShuffleAssignmentEvent(
      0,
      100,
      java.util.Arrays.asList("host1:9097", "host2:9097"),
      16,
      1L))
    listener.onOtherEvent(CelebornFallbackEvent(
      java.util.Collections.singletonMap("pushTimeout", 2L: java.lang.Long),
      1L))
    listener.onOtherEvent(CelebornReassignEvent(partitionSplit = true, false, false, 1L))
    listener.onOtherEvent(CelebornWriteMetricsEvent(
      0,
      new WriteMetrics(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
      java.util.Arrays.asList(new PushWorkerStats("host1:9097", 1L, 10L, 100L, 0L, 0L, 0L, 0L, "")),
      1L))
    listener.onOtherEvent(CelebornReadMetricsEvent(
      0,
      new ReadMetrics(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
      java.util.Arrays.asList(new WorkerReadCost("host1:9097", 1L, 10L, 100L, 100L)),
      1L))
    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.onApplicationEnd(SparkListenerApplicationEnd(1L))

    assertTrue(statusStore.extensionEnabled())
    assertEquals(1, statusStore.buildInfo().info.size)
    assertEquals(1, statusStore.assignmentInfos().size)
    assertEquals(16, statusStore.assignmentInfos().head.numPartitions)
    assertEquals(2L, statusStore.fallbackStats().counts.get("pushTimeout"))
    assertTrue(statusStore.reassignStats().partitionSplit)
    assertFalse(statusStore.reassignStats().stageRetry)
    assertEquals(2L, statusStore.writeTimes().serializeTimeMs)
    assertEquals(3L, statusStore.readTimes().deserializeTimeMs)
    assertEquals(100L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)
    // The replayed read event rebuilds the per-shuffle read aggregation as well.
    assertEquals(10L, statusStore.aggregatedReadMetrics().metrics.get(0).bytesRead)
    assertEquals(1L, statusStore.aggregatedReadMetrics().metrics.get(0).chunkCount)
    assertEquals(10L, statusStore.aggregatedReadMetrics().metrics.get(0).fetchTimeMs)
    tracking.close(false)
  }
}
