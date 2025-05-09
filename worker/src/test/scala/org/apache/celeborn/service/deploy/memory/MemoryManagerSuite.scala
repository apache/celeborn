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

import java.util.concurrent.TimeUnit

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

}
