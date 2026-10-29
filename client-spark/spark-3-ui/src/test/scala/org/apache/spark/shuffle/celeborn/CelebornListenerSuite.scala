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
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, Success, TaskEndReason, TaskKilled, TaskState}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.Status.{ASYNC_TRACKING_ENABLED, MAX_RETAINED_STAGES}
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerEnvironmentUpdate, SparkListenerEvent, SparkListenerJobEnd, SparkListenerTaskEnd, TaskInfo, TaskLocality}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.status.KVUtils.KVStoreScalaSerializer
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

    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 100L, 40L))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 250L, 94L))
    // Unrelated events are ignored.
    listener.onOtherEvent(new SparkListenerEvent {})
    listener.flush()

    assertEquals(350L, statusStore.aggregatedTaskInfo().uncompressedShuffleBytes)
    assertEquals(134L, statusStore.aggregatedTaskInfo().compressedShuffleBytes)
  }

  @Test
  def shuffleWriteMetricsCountEveryAttempt(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onTaskEnd(newTaskEnd(100L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(200L, 20L, 2L, 6L))
    listener.onTaskEnd(newTaskEnd(300L, 30L, 3L, 7L, TaskKilled("killed by test")))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 100L, 40L))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 200L, 80L))
    listener.flush()

    val agg = statusStore.aggregatedTaskInfo()
    assertEquals(600L, agg.shuffleWriteBytes)
    assertEquals(60L, agg.shuffleWriteTimeMs)
    assertEquals(6L, agg.shuffleFetchWaitTimeMs)
    assertEquals(18L, agg.taskDurationMs)
    assertEquals(300L, agg.uncompressedShuffleBytes)
    assertEquals(120L, agg.compressedShuffleBytes)
  }

  @Test
  def compressionRatioSidesComeFromTheSameMapperEndAttemptSet(): Unit = {
    // Regression: a task failing after its writer completed already reported
    // MapperEnd. Both ratio sides derive from MapperEnd reports, so such attempts
    // are counted consistently instead of skewing the ratio.
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    // Partition 0 attempt 0: writer completed, then the task failed.
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 20L, 94L))
    // Partition 0 attempt 1 (retry) and partition 1 attempt 0: succeeded.
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 20L, 94L))
    listener.onOtherEvent(new CelebornMapperMetricsEvent(0, 20L, 94L))
    listener.onTaskEnd(newTaskEnd(94L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(94L, 10L, 1L, 5L))
    listener.onTaskEnd(newTaskEnd(94L, 10L, 1L, 5L, TaskKilled("failed after write")))
    listener.flush()

    val agg = statusStore.aggregatedTaskInfo()
    assertEquals(282L, agg.shuffleWriteBytes)
    assertEquals(60L, agg.uncompressedShuffleBytes)
    assertEquals(282L, agg.compressedShuffleBytes)
  }

  @Test
  def mapperMetricsEventJsonRoundtrip(): Unit = {
    // SparkListenerEvent carries @JsonTypeInfo(property = "Event"), so the event log
    // records the class name and History Server replay can restore the event (the
    // same mechanism JsonProtocol uses for custom SparkListenerEvents).
    val mapper = new ObjectMapper()
    val event = new CelebornMapperMetricsEvent(1, 1024L, 512L)
    val json = mapper.writeValueAsString(event)
    assertTrue(json.contains(
      s""""Event":"${classOf[CelebornMapperMetricsEvent].getName}"""))
    val restored = mapper
      .readValue(json, classOf[SparkListenerEvent])
      .asInstanceOf[CelebornMapperMetricsEvent]
    assertEquals(1, restored.getShuffleId)
    assertEquals(1024L, restored.getUncompressedBytesWritten)
    assertEquals(512L, restored.getCompressedBytesWritten)

    // Event logs written before compressedBytesWritten existed still deserialize.
    val legacy = mapper.readValue(
      s"""{"Event":"${classOf[CelebornMapperMetricsEvent].getName}",""" +
        """"shuffleId":1,"uncompressedBytesWritten":1024}""",
      classOf[SparkListenerEvent])
      .asInstanceOf[CelebornMapperMetricsEvent]
    assertEquals(1024L, legacy.getUncompressedBytesWritten)
    assertEquals(0L, legacy.getCompressedBytesWritten)
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

  @Test
  def shuffleAssignmentPersistedFromPostedEvent(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornShuffleAssignmentEvent(3, 1, Map("w1" -> 2, "w2" -> 4)))
    listener.onOtherEvent(CelebornShuffleAssignmentEvent(4, 2, Map("w2" -> 1)))

    assertEquals(
      Seq(ShuffleAssignmentUIData(3, 1, 2, 6, 2, 4), ShuffleAssignmentUIData(4, 2, 1, 1, 1, 1)),
      statusStore.shuffleAssignments())
    assertEquals(
      WorkerSlotsUIData(2, Map("w1" -> 1, "w2" -> 2), Map("w1" -> 2, "w2" -> 5)),
      statusStore.workerSlots())
  }

  @Test
  def workerSlotsCoverShuffleAssignmentsBeyondRetainedStages(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf().set(MAX_RETAINED_STAGES, 2))

    (0 until 3).foreach { id =>
      listener.onOtherEvent(CelebornShuffleAssignmentEvent(id, id, Map("w1" -> 1)))
    }

    assertEquals(Seq(1, 2), statusStore.shuffleAssignments().map(_.shuffleId))
    assertEquals(WorkerSlotsUIData(3, Map("w1" -> 3), Map("w1" -> 3)), statusStore.workerSlots())
  }

  @Test
  def shuffleAssignmentKVStoreRoundTrip(): Unit = {
    // History Server disk stores persist UI data through this serializer.
    val serializer = new KVStoreScalaSerializer()
    val assignment = ShuffleAssignmentUIData(5, 1, 2, 4, 1, 3)
    val workerSlots = WorkerSlotsUIData(1, Map("w1" -> 1, "w2" -> 1), Map("w1" -> 3, "w2" -> 1))

    assertEquals(
      assignment,
      serializer.deserialize(serializer.serialize(assignment), classOf[ShuffleAssignmentUIData]))
    val restored =
      serializer.deserialize(serializer.serialize(workerSlots), classOf[WorkerSlotsUIData])
    assertEquals(workerSlots, restored)
    assertEquals(4, restored.workerSlots.values.sum)
  }

  @Test
  def workerRevivesAggregatedFromPostedEvents(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(CelebornPartitionReviveEvent(
      3,
      1,
      Map("w1" -> Map("SOFT_SPLIT" -> 2), "w2" -> Map("PUSH_DATA_TIMEOUT_REPLICA" -> 1))))
    listener.onOtherEvent(CelebornPartitionReviveEvent(
      4,
      2,
      Map("w1" -> Map("SOFT_SPLIT" -> 1, "HARD_SPLIT" -> 1))))

    assertEquals(
      WorkerRevivesUIData(Map(
        "w1" -> Map("SOFT_SPLIT" -> 3, "HARD_SPLIT" -> 1),
        "w2" -> Map("PUSH_DATA_TIMEOUT_REPLICA" -> 1))),
      statusStore.workerRevives())
  }

  @Test
  def partitionReviveEventJsonRoundTrip(): Unit = {
    // Same mapper setup as Spark's JsonProtocol for events it does not know.
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val event = CelebornPartitionReviveEvent(3, 1, Map("w1" -> Map("HARD_SPLIT" -> 2)))
    val restored = mapper
      .readValue(mapper.writeValueAsString(event), classOf[SparkListenerEvent])
      .asInstanceOf[CelebornPartitionReviveEvent]

    val store = new InMemoryStore()
    new CelebornListener(store, new SparkConf()).onOtherEvent(restored)
    assertEquals(
      WorkerRevivesUIData(Map("w1" -> Map("HARD_SPLIT" -> 2))),
      new CelebornStatusStore(store).workerRevives())
  }

  @Test
  def workerRevivesKVStoreRoundTrip(): Unit = {
    val serializer = new KVStoreScalaSerializer()
    val revives = WorkerRevivesUIData(Map(
      "w1" -> Map("SOFT_SPLIT" -> 3, "HARD_SPLIT" -> 1),
      "w2" -> Map("PUSH_DATA_TIMEOUT_REPLICA" -> 1)))

    val restored =
      serializer.deserialize(serializer.serialize(revives), classOf[WorkerRevivesUIData])
    assertEquals(revives, restored)
    assertEquals(4, restored.workerRevives("w1").values.sum)
  }
}
