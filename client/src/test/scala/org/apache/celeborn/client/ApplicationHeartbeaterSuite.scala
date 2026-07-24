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

package org.apache.celeborn.client

import java.util.concurrent.ConcurrentHashMap

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf

class ApplicationHeartbeaterSuite extends CelebornFunSuite {

  private def makeHeartbeater(
      gcEnabled: Boolean = true,
      minIntervalMs: Long = 60000L): ApplicationHeartbeater = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.CLIENT_GC_ON_CLUSTER_OVERLOAD_ENABLED, gcEnabled)
    conf.set(CelebornConf.CLIENT_GC_ON_CLUSTER_OVERLOAD_MIN_INTERVAL, minIntervalMs)

    val registeredShuffles = ConcurrentHashMap.newKeySet[Int]()
      .asInstanceOf[ConcurrentHashMap.KeySetView[Int, java.lang.Boolean]]

    new ApplicationHeartbeater(
      "test-app",
      conf,
      null, // MasterClient not needed for handleGcSignal unit tests
      () => (0L, 0L) -> (0L, 0L, Map.empty, Map.empty),
      null, // WorkerStatusTracker not needed
      registeredShuffles,
      _ => ())
  }

  test("GC is not triggered when feature is disabled") {
    val hb = makeHeartbeater(gcEnabled = false)
    hb.handleGcSignal(shouldTriggerGc = true)
    assert(hb.lastGcTriggerTimeNs == 0L)
  }

  test("GC is not triggered when signal is false") {
    val hb = makeHeartbeater(gcEnabled = true)
    hb.handleGcSignal(shouldTriggerGc = false)
    assert(hb.lastGcTriggerTimeNs == 0L)
  }

  test("GC is triggered on first signal when enabled") {
    val hb = makeHeartbeater(gcEnabled = true, minIntervalMs = 0L)
    val before = System.nanoTime()
    hb.handleGcSignal(shouldTriggerGc = true)
    assert(hb.lastGcTriggerTimeNs >= before)
  }

  test("GC is skipped when called again within the cooldown interval") {
    val hb = makeHeartbeater(gcEnabled = true, minIntervalMs = 60000L)

    hb.handleGcSignal(shouldTriggerGc = true)
    val firstTrigger = hb.lastGcTriggerTimeNs
    assert(firstTrigger > 0L)

    // Second call immediately — well within 60s cooldown
    hb.handleGcSignal(shouldTriggerGc = true)
    assert(
      hb.lastGcTriggerTimeNs == firstTrigger,
      "lastGcTriggerTimeNs should not change on second call")
  }

  test("GC fires again after cooldown interval has elapsed") {
    // Use 0ms cooldown so any elapsed time satisfies it.
    val hb = makeHeartbeater(gcEnabled = true, minIntervalMs = 0L)

    hb.handleGcSignal(shouldTriggerGc = true)
    val firstTrigger = hb.lastGcTriggerTimeNs

    // With 0ms interval the next call should always be allowed.
    hb.handleGcSignal(shouldTriggerGc = true)
    assert(hb.lastGcTriggerTimeNs >= firstTrigger)
  }

  test("no GC when both signal is false and feature is disabled") {
    val hb = makeHeartbeater(gcEnabled = false)
    hb.handleGcSignal(shouldTriggerGc = false)
    assert(hb.lastGcTriggerTimeNs == 0L)
  }

  test("cooldown is measured from the last successful GC trigger") {
    val hb = makeHeartbeater(gcEnabled = true, minIntervalMs = 60000L)

    // First trigger — sets lastGcTriggerTimeNs
    hb.handleGcSignal(shouldTriggerGc = true)
    val firstTrigger = hb.lastGcTriggerTimeNs
    assert(firstTrigger > 0L)

    // Three more calls within cooldown — none should update lastGcTriggerTimeNs
    hb.handleGcSignal(shouldTriggerGc = true)
    hb.handleGcSignal(shouldTriggerGc = true)
    hb.handleGcSignal(shouldTriggerGc = true)
    assert(hb.lastGcTriggerTimeNs == firstTrigger)
  }
}
