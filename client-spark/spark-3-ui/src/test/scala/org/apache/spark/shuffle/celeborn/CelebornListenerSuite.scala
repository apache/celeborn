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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, Success, TaskEndReason, TaskKilled, TaskState}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobEnd, SparkListenerTaskEnd, TaskInfo, TaskLocality}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(classOf[JUnit4])
class CelebornListenerSuite {

  private val pluginClass = classOf[CelebornPlugin].getName

  private def newTaskEnd(
      writeBytes: Long,
      writeTimeMs: Long,
      fetchWaitMs: Long,
      durationMs: Long,
      reason: TaskEndReason = Success): SparkListenerTaskEnd = {
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
    SparkListenerTaskEnd(0, 0, "ShuffleMapTask", reason, info, null, metrics)
  }

  private def envUpdate(sparkProps: (String, String)*): SparkListenerEnvironmentUpdate = {
    SparkListenerEnvironmentUpdate(Map("Spark Properties" -> sparkProps.toSeq))
  }

  @Test
  def aggregateMapperMetricsEvents(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 100L))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 250L))
    // Unrelated events are ignored.
    listener.onOtherEvent(new SparkListenerEvent {})
    listener.flush()

    assertEquals(350L, statusStore.aggregatedTaskInfo().uncompressedShuffleBytes)
  }

  @Test
  def shuffleWriteMetricsCountOnlySuccessfulAttempts(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(200L, 20L, 2L, 6L))
    // The killed attempt wrote bytes, but its MapperEnd is never reported.
    listener.onTaskEnd(newTaskEnd(300L, 30L, 3L, 7L, TaskKilled("killed by test")))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 100L))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 200L))
    listener.flush()

    val agg = statusStore.aggregatedTaskInfo()
    // The killed attempt's write metrics are excluded; read side counts every attempt.
    assertEquals(300L, agg.shuffleWriteBytes)
    assertEquals(30L, agg.shuffleWriteTimeMs)
    assertEquals(6L, agg.shuffleFetchWaitTimeMs)
    assertEquals(18L, agg.taskDurationMs)
    assertEquals(300L, agg.uncompressedShuffleBytes)
  }

  @Test
  def mapperMetricsEventJsonRoundtrip(): Unit = {
    // SparkListenerEvent carries @JsonTypeInfo(property = "Event"), so the event log
    // records the class name and History Server replay can restore the event (the
    // same mechanism JsonProtocol uses for custom SparkListenerEvents).
    val mapper = new ObjectMapper()
    val event = new CelebornMapperMetricsEvent(1, 1024L)
    val json = mapper.writeValueAsString(event)
    assertTrue(json.contains(
      s""""Event":"${classOf[CelebornMapperMetricsEvent].getName}"""))
    val restored = mapper
      .readValue(json, classOf[SparkListenerEvent])
      .asInstanceOf[CelebornMapperMetricsEvent]
    assertEquals(1, restored.getShuffleId)
    assertEquals(1024L, restored.getUncompressedBytesWritten)
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
  def buildInfoRestoredOnReplayFromRecordedEvent(): Unit = {
    // History Server replay: the recorded event restores the build info from
    // the application itself and acts as the opt-in marker.
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf(), requirePluginOptIn = true)
    assertTrue(statusStore.buildInfo().info.isEmpty)

    listener.onOtherEvent(CelebornBuildInfoEvent(
      version = "1.0.0-test",
      commitId = "abcdef12",
      revision = "abcdef1234567890"))

    assertTrue(statusStore.extensionEnabled())
    val info = statusStore.buildInfo().info.toMap
    assertEquals(3, info.size)
    assertEquals("1.0.0-test", info("Version"))
    assertEquals("abcdef12", info("Commit Id"))
    assertEquals("abcdef1234567890", info("Revision"))
    assertTrue(statusStore.celebornProperties().info.isEmpty)
  }

  @Test
  def buildInfoEnablesCollectionOnReplay(): Unit = {
    // Metrics are collected once the build info event enabled the plugin.
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf(), requirePluginOptIn = true)

    listener.onOtherEvent(CelebornBuildInfoEvent(
      version = "1.0.0-test",
      commitId = "abcdef12",
      revision = "abcdef1234567890"))
    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))

    assertEquals(100L, statusStore.aggregatedTaskInfo().shuffleWriteBytes)
  }

  @Test
  def buildInfoPersistedByLiveListenerFromPostedEvent(): Unit = {
    // Live path uses the same code path as History Server replay.
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornBuildInfoEvent(
      version = "1.0.0-test",
      commitId = "abcdef12",
      revision = "abcdef1234567890"))

    val info = statusStore.buildInfo().info.toMap
    assertEquals(3, info.size)
    assertEquals("1.0.0-test", info("Version"))
    assertEquals("abcdef12", info("Commit Id"))
    assertEquals("abcdef1234567890", info("Revision"))
  }
}
