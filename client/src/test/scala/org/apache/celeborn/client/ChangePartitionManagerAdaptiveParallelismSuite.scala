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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

class ChangePartitionManagerAdaptiveParallelismSuite extends CelebornFunSuite {

  private val APP = "app-adaptive-parallelism-test"

  private case class CapturedReply(
      status: StatusCode,
      loc: Option[PartitionLocation],
      additionals: util.List[PartitionLocation])

  private class CapturingContext extends RequestLocationCallContext {
    val replies = new ConcurrentHashMap[Int, CapturedReply]()
    override def reply(
        partitionId: Int,
        status: StatusCode,
        partitionLocationOpt: Option[PartitionLocation],
        available: Boolean,
        additionalLocations: util.List[PartitionLocation]): Unit = {
      replies.put(partitionId, CapturedReply(status, partitionLocationOpt, additionalLocations))
    }
  }

  private var lifecycleManager: LifecycleManager = _

  override protected def afterEach(): Unit = {
    if (lifecycleManager != null) {
      lifecycleManager.stop()
      lifecycleManager = null
    }
    super.afterEach()
  }

  private def makeConf(): CelebornConf = {
    val conf = new CelebornConf()
    conf.set(CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_ENABLED.key, "true")
    conf.set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED.key, "false")
    conf.set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
    conf
  }

  private def makeWorker(id: Int): WorkerInfo =
    new WorkerInfo(s"host$id", 9000 + id, 9100 + id, 9200 + id, 9300 + id)

  private def makeLoc(partitionId: Int, epoch: Int, host: String): PartitionLocation =
    new PartitionLocation(partitionId, epoch, host, 9000, 9100, 9200, 9300, Mode.PRIMARY)

  /**
   * Set up a LifecycleManager holding the given workers (slot reservation stubbed out)
   * and an initial epoch-0 location of the partition on the first worker.
   */
  private def prepareLifecycleManager(
      conf: CelebornConf,
      shuffleId: Int,
      partitionId: Int,
      workers: Seq[WorkerInfo]): PartitionLocation = {
    lifecycleManager = new LifecycleManager(APP, conf) {
      override def reserveSlotsWithRetry(
          shuffleId: Int,
          candidates: util.HashSet[WorkerInfo],
          slots: WorkerResource,
          updateEpoch: Boolean,
          isSegmentGranularityVisible: Boolean): Boolean = true
    }
    val allocatedWorkers = JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
    workers.foreach(w => allocatedWorkers.put(w.toUniqueId, new ShufflePartitionLocationInfo(w)))
    val loc0 = makeLoc(partitionId, 0, workers.head.host)
    allocatedWorkers.get(workers.head.toUniqueId).addPrimaryPartitions(
      util.Collections.singletonList(loc0))
    lifecycleManager.shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
    lifecycleManager.updateLatestPartitionLocations(shuffleId, util.Arrays.asList(loc0))
    lifecycleManager.commitManager.registerShuffle(
      shuffleId,
      1,
      isSegmentGranularityVisible = false,
      1)
    loc0
  }

  private def primaryLocs(shuffleId: Int, partitionId: Int): List[PartitionLocation] =
    lifecycleManager
      .workerSnapshots(shuffleId)
      .asScala
      .values
      .flatMap(_.getPrimaryPartitions(Some(partitionId)).asScala)
      .toList

  /** A ChangePartitionManager whose clock is driven by the returned time holder. */
  private class FakeClockManager(
      conf: CelebornConf,
      lifecycleManager: LifecycleManager,
      var now: Long)
    extends ChangePartitionManager(conf, lifecycleManager) {
    nowMs = () => now
    def advance(ms: Long): Unit = {
      now += ms
    }
  }

  test("hot partition (fillTime < window) boosts desired and allocates gap locations") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Epoch 0 fills in 40s (< 60s window): the first SOFT_SPLIT report boosts desired to
    // target ceil(60/40) = 2. The soft-split epoch 0 stays writable, so only the gap of
    // 2 - 1 = 1 fresh location is allocated.
    changePartitionManager.advance(40000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1))
    assert(newLocs.map(_.getHost).distinct.size == 1)

    // The reply carries the full writable set including the soft-split epoch 0: max epoch as
    // the primary reply, the rest as additional locations.
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
  }

  test("partition filled slower than the window is not boosted") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Normal file rolling: fillTime 70s > 60s window, desired stays 1. The soft-split
    // epoch 0 stays writable, so no replacement is allocated and the reply points back
    // to epoch 0 itself.
    changePartitionManager.advance(70000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.isEmpty)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 0)
    assert(reply.additionals.isEmpty)
  }

  test("epoch with unknown allocTime (legacy data) never boosts") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    // No recordInitialAllocTime: the alloc time of epoch 0 is unknown.
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)

    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    // No boost, and the soft-split epoch 0 stays writable: nothing is allocated.
    assert(primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0).isEmpty)
  }

  test("HARD_SPLIT with an unavailable worker only retires, never boosts") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)
    // The worker of epoch 0 is known unavailable: the HARD_SPLIT is not measured.
    lifecycleManager.workerStatusTracker.excludedWorkers.put(
      loc0.getWorker,
      (StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY, 100000L))

    // Epoch 0 hard-splits 10s after allocation: even a fast fill does not boost.
    changePartitionManager.advance(10000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.HARD_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 1)
    assert(primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
      .map(_.getEpoch).toSet == Set(1))
  }

  test("repeated SOFT_SPLIT reports of the same epoch boost only once") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    changePartitionManager.advance(40000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size

    // Another executor reports SOFT_SPLIT of the same epoch 0: no second boost, no new
    // allocation, but it still gets the current active set.
    changePartitionManager.advance(5000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
  }

  test("proportional step-up: a much faster fill jumps straight to the cap, no debounce") {
    val conf = makeConf()
    // Pin the cap to 4 so the test is independent of the product default.
    conf.set(
      CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_MAX_LOCATIONS.key,
      "4")
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 0L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // t=40s: epoch 0 fills in 40s, target ceil(60/40) = 2; epoch 0 stays writable, so only
    // epoch 1 is allocated at t=40s.
    changePartitionManager.advance(40000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)

    // t=50s: epoch 1 (allocated at t=40s) fills in 10s, target ceil(60/10) = 6, capped at
    // the configured max 4. No per-window debounce: the much faster fill corrects desired
    // immediately (the cap and the per-epoch first-report dedup bound the boosts).
    changePartitionManager.advance(10000)
    val loc1 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 1).get
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      loc1,
      Some(StatusCode.SOFT_SPLIT),
      50000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("desired is capped at maxLocationsPerPartition") {
    val conf = makeConf()
    // Pin the cap to 4 so the test is independent of the product default.
    conf.set(
      CelebornConf.CLIENT_SHUFFLE_ADAPTIVE_PARTITION_WRITE_PARALLELISM_MAX_LOCATIONS.key,
      "4")
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 0L)

    // Proportional climb: fillTime 40s -> target 2, 25s -> 3, 15s -> 4, then a 10s fill
    // (target 6) is capped at the configured max 4.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      40000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 1, 70000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      1,
      makeLoc(partitionId, 1, workers.head.host),
      Some(StatusCode.SOFT_SPLIT),
      95000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 3)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 2, 140000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      2,
      makeLoc(partitionId, 2, workers.head.host),
      Some(StatusCode.SOFT_SPLIT),
      155000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 4)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 3, 210000L)
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      3,
      makeLoc(partitionId, 3, workers.head.host),
      Some(StatusCode.SOFT_SPLIT),
      220000L)
    // The configured upper bound is 4.
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 4)
  }

  test("out-of-order epoch fills are measured independently") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)

    // With K > 1, epoch 10 (written by one mapper subset) can fill up before epoch 5
    // (written by another subset). Each epoch is measured against its own alloc time.
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 5, 50000L)
    changePartitionManager.recordAllocTime(shuffleId, partitionId, 10, 0L)
    // Epoch 10 fills first: fillTime 30s < window, boost.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      10,
      makeLoc(partitionId, 10, workers.head.host),
      Some(StatusCode.SOFT_SPLIT),
      30000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    // Epoch 5 fills later at t=75s: measured against its own allocTime, fillTime 25s ->
    // target 3, unaffected by the out-of-order event.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      5,
      makeLoc(partitionId, 5, workers.head.host),
      Some(StatusCode.SOFT_SPLIT),
      75000L)
    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 3)
  }

  test("request with active set already satisfied allocates 0 but still replies full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // Boost to 2 writable locations first (fillTime 40s -> target 2): epoch 0 stays writable
    // and epoch 1 is allocated, writable epochs {0, 1}.
    changePartitionManager.advance(40000)
    changePartitionManager.handleRequestPartitionLocation(
      new CapturingContext,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size
    val loc1 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 1).get

    // A lagging executor retires epoch 1 slowly (fillTime 90s > window, no boost): the
    // writable set {0, 1} already satisfies desired=2, nothing is allocated, but the reply
    // still carries the current writable set.
    changePartitionManager.advance(90000)
    val context = new CapturingContext
    changePartitionManager.handleRequestPartitionLocation(
      context,
      shuffleId,
      partitionId,
      1,
      loc1,
      Some(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

    assert(changePartitionManager.desiredLocationCount(shuffleId, partitionId) == 2)
    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
  }

  test("concurrent revives of one partition allocate once and are replied the full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new ChangePartitionManager(conf, lifecycleManager)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 100000L)

    // The first SOFT_SPLIT report of epoch 0 (fillTime 40s, target 2) boosts desired to
    // 2; reports of the same epoch queued from other executors are deduped.
    changePartitionManager.onEpochRetired(
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT),
      140000L)
    val context1 = new CapturingContext
    val context2 = new CapturingContext
    val requestSet = new util.HashSet[ChangePartitionRequest]()
    val request1 = ChangePartitionRequest(
      context1,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT))
    val request2 = ChangePartitionRequest(
      context2,
      shuffleId,
      partitionId,
      0,
      loc0,
      Some(StatusCode.SOFT_SPLIT))
    requestSet.add(request1)
    requestSet.add(request2)
    changePartitionManager.changePartitionRequests
      .computeIfAbsent(shuffleId, changePartitionManager.rpcContextRegisterFunc)
      .put(partitionId, requestSet)

    changePartitionManager.handleRequestPartitions(shuffleId, Array(request2), false)

    // desired=2 with epoch 0 retained as writable: exactly one fresh location allocated
    // (idempotent for concurrent revives).
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1))
    assert(newLocs.map(_.getHost).distinct.size == 1)

    // Both requests are replied with the same full writable set (soft epoch 0 included).
    Seq(context1, context2).foreach { context =>
      val reply = context.replies.get(partitionId)
      assert(reply != null && reply.status == StatusCode.SUCCESS)
      assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
      assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
    }
  }
}
