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

package org.apache.celeborn.service.deploy.memory

import scala.concurrent.duration.DurationInt

import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE, WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE}
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager.{MemoryPressureListener, ServingState}

class MemoryManagerSuite extends CelebornFunSuite {

  // reset the memory manager before each test
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    MemoryManager.reset()
  }

  test("Init MemoryManager with invalid configuration") {
    val conf = new CelebornConf().set(WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE, 0.95)
      .set(WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE, 0.85)
    val caught =
      intercept[IllegalArgumentException] {
        MemoryManager.initialize(conf)
      }
    assert(
      caught.getMessage == s"Invalid config, ${WORKER_DIRECT_MEMORY_RATIO_PAUSE_REPLICATE.key}(0.85) " +
        s"should be greater than ${WORKER_DIRECT_MEMORY_RATIO_PAUSE_RECEIVE.key}(0.95)")
  }

  test("[CELEBORN-888] Test MemoryManager#currentServingState trigger case") {
    val conf = new CelebornConf()
    try {
      val memoryManager = MemoryManager.initialize(conf)
      val maxDirectorMemory = memoryManager.maxDirectMemory
      val pushThreshold =
        (conf.workerDirectMemoryRatioToPauseReceive * maxDirectorMemory).longValue()
      val replicateThreshold =
        (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectorMemory).longValue()
      val resumeThreshold = (conf.workerDirectMemoryRatioToResume * maxDirectorMemory).longValue()

      // use sortMemoryCounter to trigger each state
      val memoryCounter = memoryManager.getSortMemoryCounter

      // default state
      assert(ServingState.NONE_PAUSED == memoryManager.currentServingState())
      // reach pause push data threshold
      memoryCounter.set(pushThreshold + 1)
      assert(ServingState.PUSH_PAUSED == memoryManager.currentServingState())
      // reach pause replicate data threshold
      memoryCounter.set(replicateThreshold + 1)
      assert(ServingState.PUSH_AND_REPLICATE_PAUSED == memoryManager.currentServingState())
      // touch pause push data threshold again
      memoryCounter.set(pushThreshold + 1)
      assert(MemoryManager.ServingState.PUSH_PAUSED == memoryManager.currentServingState())
      // between pause push data threshold and resume data threshold
      memoryCounter.set(resumeThreshold + 2)
      assert(MemoryManager.ServingState.PUSH_PAUSED == memoryManager.currentServingState())
      // touch resume data threshold
      memoryCounter.set(0)
      assert(MemoryManager.ServingState.NONE_PAUSED == memoryManager.currentServingState())
    } catch {
      case e: Exception => throw e
    } finally {
      MemoryManager.reset()
    }
  }

  test("[CELEBORN-882] Test MemoryManager check memory thread logic") {
    val conf = new CelebornConf()
    val memoryManager = MemoryManager.initialize(conf)
    val maxDirectorMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectorMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectorMemory).longValue()
    val memoryCounter = memoryManager.getSortMemoryCounter

    val pushListener = new MockMemoryPressureListener(TransportModuleConstants.PUSH_MODULE)
    val replicateListener =
      new MockMemoryPressureListener(TransportModuleConstants.REPLICATE_MODULE)
    memoryManager.registerMemoryListener(pushListener)
    memoryManager.registerMemoryListener(replicateListener)

    // NONE PAUSED -> PAUSE PUSH
    memoryCounter.set(pushThreshold + 1)
    // default check interval is 10ms and we need wait 30ms to make sure the listener is triggered
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(pushListener.isPause)
      assert(!replicateListener.isPause)
    }
    Thread.sleep(20)

    // PAUSE PUSH -> PAUSE PUSH AND REPLICATE
    memoryCounter.set(replicateThreshold + 1)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(pushListener.isPause)
      assert(replicateListener.isPause)
    }
    Thread.sleep(20)

    // PAUSE PUSH AND REPLICATE -> PAUSE PUSH
    memoryCounter.set(pushThreshold + 1)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(pushListener.isPause)
      assert(!replicateListener.isPause)
    }
    Thread.sleep(20)

    // PAUSE PUSH -> NONE PAUSED
    memoryCounter.set(0)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(!pushListener.isPause)
      assert(!replicateListener.isPause)
    }
    Thread.sleep(20)
    // [CELEBORN-882] Test record pause push time
    val lastPauseTime1 = memoryManager.getPausePushDataTime.longValue()
    val lastPauseReplicaTime1 = memoryManager.getPausePushDataAndReplicateTime.longValue()
    // PauseTime should count the actual waiting time
    assert(lastPauseTime1 >= 60)
    assert(lastPauseReplicaTime1 >= 20)
    logInfo(s"lastPauseTime1: $lastPauseTime1, lastPauseReplicaTime1: $lastPauseReplicaTime1")

    // NONE PAUSED -> PAUSE PUSH AND REPLICATE
    memoryCounter.set(replicateThreshold + 1)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(pushListener.isPause)
      assert(replicateListener.isPause)
    }

    Thread.sleep(20)

    // PAUSE PUSH AND REPLICATE -> NONE PAUSED
    memoryCounter.set(0)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(!pushListener.isPause)
      assert(!replicateListener.isPause)
    }

    // Wait for the check thread to update the metrics
    memoryManager.switchServingState()
    val lastPauseTime2 = memoryManager.getPausePushDataTime.longValue()
    val lastPauseReplicaTime2 = memoryManager.getPausePushDataAndReplicateTime.longValue()
    assert(lastPauseTime2 > lastPauseTime1)
    assert(lastPauseReplicaTime2 > lastPauseReplicaTime1)
    logInfo(s"lastPauseTime2: $lastPauseTime2, lastPauseReplicaTime2: $lastPauseReplicaTime2")

    // NONE PAUSED -> PAUSE PUSH
    memoryCounter.set(pushThreshold + 1)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(pushListener.isPause)
      assert(!replicateListener.isPause)
    }

    Thread.sleep(20)

    // PAUSE PUSH -> NONE PAUSED
    memoryCounter.set(0)
    eventually(timeout(30.second), interval(10.milliseconds)) {
      assert(!pushListener.isPause)
      assert(!replicateListener.isPause)
    }

    // Wait for the check thread to update the metrics
    memoryManager.switchServingState()
    val lastPauseTime3 = memoryManager.getPausePushDataTime.longValue()
    val lastPauseReplicaTime3 = memoryManager.getPausePushDataAndReplicateTime.longValue()
    assert(lastPauseTime3 > lastPauseTime2)
    assert(lastPauseReplicaTime3 == lastPauseReplicaTime2)
    logInfo(s"lastPauseTime3: $lastPauseTime3, lastPauseReplicaTime3: $lastPauseReplicaTime3")
  }

  test("[CELEBORN-1792] Test MemoryManager resume by pinned memory") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "0")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val maxDirectorMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectorMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectorMemory).longValue()

    val pushListener = new MockMemoryPressureListener(TransportModuleConstants.PUSH_MODULE)
    val replicateListener =
      new MockMemoryPressureListener(TransportModuleConstants.REPLICATE_MODULE)
    memoryManager.registerMemoryListener(pushListener)
    memoryManager.registerMemoryListener(replicateListener)

    // NONE PAUSED -> PAUSE PUSH
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    // KEEP PAUSE PUSH
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)

    // NONE PAUSED -> PAUSE PUSH AND REPLICATE
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)

    // KEEP PAUSE PUSH AND REPLICATE
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(pushListener.isPause)
    assert(replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)

    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)
    MemoryManager.reset()
  }

  test("[CELEBORN-1792] Test MemoryManager keep resume a while by pinned memory") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "1s")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val maxDirectorMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectorMemory).longValue()
    val pinnedMemoryResumeThreshold =
      (conf.workerPinnedMemoryRatioToResume * maxDirectorMemory).longValue()
    val channelsLimiter = new MockChannelsLimiter()
    memoryManager.registerMemoryListener(channelsLimiter)

    // NONE PAUSED -> PAUSE PUSH
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(channelsLimiter.isResume)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    // keep pause push, but channels keep resume when pinnedMemory still less than threshold
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(channelsLimiter.isResume)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    // exit keepResumeByPinnedMemory because pinnedMemory is greater than threshold
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(
      pinnedMemoryResumeThreshold + 1)
    memoryManager.switchServingState()
    assert(!channelsLimiter.isResume)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(channelsLimiter.isResume)
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)

  }

  test("Test MemoryManager resume replicate by pinned memory") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "0")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val maxDirectorMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectorMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectorMemory).longValue()
    val pinnedMemoryResumeThreshold =
      (conf.workerPinnedMemoryRatioToResume * maxDirectorMemory).longValue()

    val pushListener = new MockMemoryPressureListener(TransportModuleConstants.PUSH_MODULE)
    val replicateListener =
      new MockMemoryPressureListener(TransportModuleConstants.REPLICATE_MODULE)
    memoryManager.registerMemoryListener(pushListener)
    memoryManager.registerMemoryListener(replicateListener)

    // NONE_PAUSED -> PUSH_AND_REPLICATE_PAUSED, resumed by low pinned memory
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)

    // KEEP PUSH_AND_REPLICATE_PAUSED, pinned memory rises, both get paused
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(
      pinnedMemoryResumeThreshold + 1)
    memoryManager.switchServingState()
    assert(pushListener.isPause)
    assert(replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)

    // PUSH_AND_REPLICATE_PAUSED -> PUSH_PAUSED, pinned memory low again
    // replicate must be resumed regardless of tryResumeByPinnedMemory
    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(!pushListener.isPause)
    assert(!replicateListener.isPause)
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    MemoryManager.reset()
  }

  test("sortMemoryReady allows sorting in PUSH_PAUSED but blocks in PUSH_AND_REPLICATE_PAUSED") {
    val conf = new CelebornConf()
    // Disable the automatic check thread so we drive state transitions manually
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val maxDirectMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectMemory).longValue()
    val maxSortMemory =
      (conf.workerPartitionSorterDirectMemoryRatioThreshold * maxDirectMemory).longValue()
    val sortMemoryCounter = memoryManager.getSortMemoryCounter

    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)

    // NONE_PAUSED: sort is allowed
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)
    sortMemoryCounter.set(0)
    assert(memoryManager.sortMemoryReady())

    // PUSH_PAUSED: sort must be allowed so that fetch reads can proceed while push is
    // back-pressured (previously sorting was also blocked in this state).
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    sortMemoryCounter.set(0)
    assert(
      memoryManager.sortMemoryReady(),
      "sortMemoryReady must return true in PUSH_PAUSED: fetch reads of already-written " +
        "data should not be blocked by push back-pressure")

    // PUSH_PAUSED but sort budget exhausted: sort must be blocked
    sortMemoryCounter.set(maxSortMemory)
    assert(!memoryManager.sortMemoryReady())

    // PUSH_AND_REPLICATE_PAUSED: sort must be blocked regardless of sort budget
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)
    sortMemoryCounter.set(0)
    assert(
      !memoryManager.sortMemoryReady(),
      "sortMemoryReady must return false in PUSH_AND_REPLICATE_PAUSED")
    MemoryManager.reset()
  }

  test("sortMemoryReady always returns true when sort memory threshold is disabled") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_DIRECT_MEMORY_CHECK_INTERVAL.key, "300s")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(
      CelebornConf.WORKER_PARTITION_SORTER_DIRECT_MEMORY_RATIO_THRESHOLD.key,
      "0")
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val maxDirectMemory = memoryManager.maxDirectMemory
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * maxDirectMemory).longValue()
    val replicateThreshold =
      (conf.workerDirectMemoryRatioToPauseReplicate * maxDirectMemory).longValue()

    Mockito.when(memoryManager.getNettyPinnedDirectMemory).thenReturn(0L)

    // PUSH_PAUSED, threshold=0 means skip all checks
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(0L)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)
    assert(
      memoryManager.sortMemoryReady(),
      "sortMemoryReady must return true when threshold is 0 regardless of serving state")

    // PUSH_PAUSED, threshold=0 means skip all checks
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    assert(
      memoryManager.sortMemoryReady(),
      "sortMemoryReady must return true when threshold is 0 regardless of serving state")

    // PUSH_AND_REPLICATE_PAUSED, threshold=0 means skip all checks
    Mockito.when(memoryManager.getMemoryUsage).thenReturn(replicateThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_AND_REPLICATE_PAUSED)
    assert(
      memoryManager.sortMemoryReady(),
      "sortMemoryReady must return true when threshold is 0 regardless of serving state")
    MemoryManager.reset()
  }

  class MockMemoryPressureListener(
      val belongModuleName: String,
      var isPause: Boolean = false) extends MemoryPressureListener {
    override def onPause(moduleName: String): Unit = {
      if (belongModuleName == moduleName) {
        isPause = true
      }
    }

    override def onResume(moduleName: String): Unit = {
      if (belongModuleName == moduleName) {
        isPause = false
      }
    }

    override def onTrim(): Unit = {
      // do nothing
    }
  }

  class MockChannelsLimiter(var isResume: Boolean = false) extends MemoryPressureListener {
    override def onPause(moduleName: String): Unit = {
      isResume = false
    }

    override def onResume(moduleName: String): Unit = {
      isResume = true
    }

    override def onTrim(): Unit = {
      // do nothing
    }
  }

  /**
   * Records every drainIncompleteFrame invocation (module name and ratio) for assertions.
   */
  class RecordingDrainListener extends MemoryPressureListener {
    val ratios: scala.collection.mutable.ArrayBuffer[Double] =
      scala.collection.mutable.ArrayBuffer[Double]()
    val moduleNames: scala.collection.mutable.ArrayBuffer[String] =
      scala.collection.mutable.ArrayBuffer[String]()

    override def onPause(moduleName: String): Unit = {}

    override def onResume(moduleName: String): Unit = {}

    override def onTrim(): Unit = {}

    override def drainIncompleteFrame(ratio: Double, moduleName: String): Int = {
      ratios += ratio
      moduleNames += moduleName
      0
    }
  }

  test("[Trickle Resume] does not trigger trickle when disabled") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "0")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener()
    memoryManager.registerMemoryListener(listener)

    // Memory stuck in Netty (not tracked by app-layer counters) is exactly what draining
    // targets, but must stay off entirely when disabled.
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)

    assert(listener.ratios.isEmpty)
    MemoryManager.reset()
  }

  test("[Trickle Resume] does not arm until app-layer usage drops to/below the watermark") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "true")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_WATERMARK_RATIO.key, "0.05")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    // Above the watermark: draining must not arm.
    val aboveWatermarkBytes = (0.08 * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener()
    memoryManager.registerMemoryListener(listener)
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)

    memoryManager.incrementDiskBuffer(aboveWatermarkBytes.intValue())
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    assert(listener.ratios.isEmpty)

    // Repeated checks above the watermark still must not probe.
    memoryManager.switchServingState()
    assert(listener.ratios.isEmpty)
    MemoryManager.reset()
  }

  test("[Trickle Resume] arms at/below watermark, disarms above it, with fixed ratio") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "true")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_WATERMARK_RATIO.key, "0.05")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_RATIO.key, "0.02")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val watermarkBytes = (0.05 * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener()
    memoryManager.registerMemoryListener(listener)
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)

    // App-layer usage at/below the watermark: draining arms and fires at fixed ratio.
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    assert(listener.ratios.nonEmpty)
    assert(listener.ratios.last == 0.02) // 2%

    // App-layer usage above the watermark: draining disarms.
    memoryManager.incrementDiskBuffer(watermarkBytes.intValue() + 1)
    val firedBeforeDisarm = listener.ratios.size
    memoryManager.switchServingState()
    assert(listener.ratios.size == firedBeforeDisarm)

    // Drop back to/below the watermark: re-arms and probes again.
    memoryManager.releaseDiskBuffer(watermarkBytes.intValue() + 1)
    memoryManager.switchServingState()
    assert(listener.ratios.size > firedBeforeDisarm)
    assert(listener.ratios.last == 0.02)
    MemoryManager.reset()
  }

  test("[Trickle Resume] pending replicate bytes above watermark blocks PUSH_MODULE but not " +
    "REPLICATE_MODULE, since it doesn't reflect this worker's own memory footprint") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "true")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_WATERMARK_RATIO.key, "0.05")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val watermarkBytes = (0.05 * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener
    memoryManager.registerMemoryListener(listener)
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)

    // Pending replicate bytes above watermark: PUSH_MODULE stays blocked, REPLICATE_MODULE arms.
    memoryManager.incrementPendingReplicateBytes(watermarkBytes + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    assert(!listener.moduleNames.contains(TransportModuleConstants.PUSH_MODULE))
    assert(listener.moduleNames.contains(TransportModuleConstants.REPLICATE_MODULE))

    memoryManager.releasePendingReplicateBytes(watermarkBytes + 1)
    memoryManager.switchServingState()
    assert(listener.moduleNames.contains(TransportModuleConstants.PUSH_MODULE))
    MemoryManager.reset()
  }

  test("[Trickle Resume] local footprint above watermark blocks REPLICATE_MODULE regardless of " +
    "pending replicate bytes") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "true")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "0")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_WATERMARK_RATIO.key, "0.05")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val watermarkBytes = (0.05 * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener
    memoryManager.registerMemoryListener(listener)
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)

    // Local footprint above watermark blocks REPLICATE_MODULE too.
    memoryManager.incrementDiskBuffer(watermarkBytes.intValue() + 1)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.PUSH_PAUSED)
    assert(listener.moduleNames.isEmpty)

    memoryManager.releaseDiskBuffer(watermarkBytes.intValue() + 1)
    memoryManager.switchServingState()
    assert(listener.moduleNames.contains(TransportModuleConstants.REPLICATE_MODULE))
    MemoryManager.reset()
  }

  test("[Trickle Resume] resets tick bookkeeping once backpressure episode ends") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.WORKER_PINNED_MEMORY_CHECK_ENABLED.key, "false")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_ENABLED.key, "true")
    conf.set(CelebornConf.WORKER_DRAIN_INCOMPLETE_FRAME_INTERVAL.key, "10000")
    MemoryManager.reset()
    val memoryManager = MockitoSugar.spy(MemoryManager.initialize(conf))
    val pushThreshold =
      (conf.workerDirectMemoryRatioToPauseReceive * memoryManager.maxDirectMemory).longValue()
    val listener = new RecordingDrainListener()
    memoryManager.registerMemoryListener(listener)

    // Enter backpressure at the low watermark: arms and drains immediately (first tick is
    // never throttled).
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(listener.ratios.nonEmpty)
    val firedBeforeResume = listener.ratios.size

    // Checking again immediately is throttled by the (long) drainIncompleteFrame interval.
    memoryManager.switchServingState()
    assert(listener.ratios.size == firedBeforeResume)

    // Lift backpressure entirely: resets tick bookkeeping.
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(0L)
    memoryManager.switchServingState()
    assert(memoryManager.servingState == ServingState.NONE_PAUSED)

    // A fresh episode re-arms and probes immediately, unthrottled by the old interval.
    Mockito.when(memoryManager.getNettyUsedDirectMemory).thenReturn(pushThreshold + 1)
    memoryManager.switchServingState()
    assert(listener.ratios.size > firedBeforeResume)
    MemoryManager.reset()
  }

}
