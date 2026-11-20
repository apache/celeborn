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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.StatusCode

class PartitionHotnessTrackerSuite extends CelebornFunSuite {

  private val shuffleId = 1
  private val partitionId = 0

  private def makeLoc(partitionId: Int, epoch: Int, host: String): PartitionLocation =
    new PartitionLocation(partitionId, epoch, host, 9000, 9100, 9200, 9300, Mode.PRIMARY)

  private def makeTracker(
      workerAvailable: PartitionLocation => Boolean,
      maxLocations: Int = -1): PartitionHotnessTracker = {
    val conf = new CelebornConf()
    if (maxLocations > 0) {
      // Pin the cap so capping tests are independent of the product default.
      conf.set(
        CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_MAX_LOCATIONS.key,
        maxLocations.toString)
    }
    new PartitionHotnessTracker(conf, (_, _) => None, workerAvailable)
  }

  test("HARD_SPLIT with an unavailable worker only retires, never boosts") {
    val tracker = makeTracker(_ => false)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    // A HARD_SPLIT of a known-unavailable worker is not measured: desired stays 1.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 1)
  }

  test("push failure cause only retires, never boosts") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    // Seed the epoch as active via a slow soft split (no boost).
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 70000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))

    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY),
      80000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 1)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("HARD_SPLIT and SOFT_SPLIT are measured equivalently") {
    Seq(StatusCode.SOFT_SPLIT, StatusCode.HARD_SPLIT).foreach { cause =>
      val tracker = makeTracker(_ => true)
      val loc0 = makeLoc(partitionId, 0, "host1")
      tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

      // Fast fill of epoch 0 (45s, target 2): both causes boost desired to 2.
      tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(cause), 45000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

      // Slow fill of epoch 1 (70s > window): neither cause boosts.
      val loc1 = makeLoc(partitionId, 1, "host1")
      tracker.registerAllocation(shuffleId, partitionId, Set(1), 70000L)
      tracker.onEpochRetired(shuffleId, partitionId, 1, loc1, Some(cause), 140000L)
      assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)
    }
  }

  test("fillTime proportionally steps desired, capped at maxLocationsPerPartition") {
    val tracker = makeTracker(_ => true, 4)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

    // fillTime 25s: target 3.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 30000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      55000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 3)

    // fillTime 10s: target 6, capped at the configured 4.
    tracker.registerAllocation(shuffleId, partitionId, Set(2), 55000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      2,
      makeLoc(partitionId, 2, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      65000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("fillTime measured under a larger active set does not scale the target") {
    // The measured fillTime is the fill time of a single location; the target is
    // interval/fillTime regardless of how many locations were active. Multiplying by the
    // active count would close a positive feedback loop (target ∝ K while K follows desired).
    val tracker = makeTracker(_ => true, maxLocations = 64)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 2)

    tracker.registerAllocation(shuffleId, partitionId, Set(1), 30000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      40000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 6)
  }

  test("constant fillTime yields a constant target as the active set grows") {
    // Regression: a partition observed filling in ~2.2s must not boost desired further just
    // because the active set grew between measurements (36 → 949 → … → 16653 in production).
    val tracker = makeTracker(_ => true, maxLocations = -1)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 2200L)
    val firstDesired = tracker.desiredLocationCount(shuffleId, partitionId)
    assert(firstDesired == 28)

    tracker.registerAllocation(shuffleId, partitionId, (1 to 20).toSet, 2200L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      4400L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == firstDesired)
  }

  test("zero fillTime is floored at 1ms, desired capped at the mapper count") {
    // With maxLocations = -1 the cap resolves to the mapper count; zero fillTime is floored
    // at 1ms instead of computing ceil(window/0) = Infinity.
    val tracker = makeTracker(_ => true, maxLocations = -1)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 128, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 6)

    tracker.registerAllocation(shuffleId, partitionId, Set(1), 10000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 128)
  }

  test("desired is capped by min(maxLocations, numMappers)") {
    // maxLocations below the mapper count: capped at the configured 4.
    val tracker = makeTracker(_ => true, maxLocations = 4)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 128, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)

    // maxLocations above the mapper count: capped at the 128 mappers (surplus locations
    // would idle under mapId % activeCount routing).
    val tracker2 = makeTracker(_ => true, maxLocations = 200)
    tracker2.recordInitialAllocTime(2, Array(makeLoc(partitionId, 0, "host1")), 128, 0L)
    tracker2.registerAllocation(2, partitionId, Set(1), 0L)
    tracker2.onEpochRetired(
      2,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      0L)
    assert(tracker2.desiredLocationCount(2, partitionId) == 128)
  }

  test("desired never decreases on slower subsequent fills") {
    val tracker = makeTracker(_ => true, 4)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    // Very fast fill: jump straight to the cap.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 10000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)

    // Slower subsequent fill (target 3): desired stays 4.
    tracker.registerAllocation(shuffleId, partitionId, Set(1), 10000L)
    tracker.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, "host1"),
      Some(StatusCode.SOFT_SPLIT),
      40000L)
    assert(tracker.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("SOFT_SPLIT of an available worker retains the epoch in the active set") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    // Soft split: the file stays writable until it hard-splits.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 40000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("SOFT_SPLIT of an unavailable worker removes the epoch from the active set") {
    val tracker = makeTracker(_ => false)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }

  test("late SOFT_SPLIT report does not resurrect a hard-retired epoch") {
    val tracker = makeTracker(_ => true)
    val loc0 = makeLoc(partitionId, 0, "host1")
    tracker.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 0L)

    // Epoch 0 soft-splits and stays writable, then hard-splits and is removed.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 30000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId) == Set(0))
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.HARD_SPLIT), 40000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)

    // A SOFT_SPLIT report sent before the hard split arrives late.
    tracker.onEpochRetired(shuffleId, partitionId, 0, loc0, Some(StatusCode.SOFT_SPLIT), 41000L)
    assert(tracker.currentActiveEpochs(shuffleId, partitionId).isEmpty)
  }
}
