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
      // numMappers high enough that the default cap (maxLocations = -1 -> mapper count)
      // never binds in this suite.
      1000,
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

  private def reviveEpoch(
      changePartitionManager: ChangePartitionManager,
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      loc: PartitionLocation): Unit =
    changePartitionManager.handlePartitionLocationRequests(
      context,
      shuffleId,
      util.Collections.singletonList(Integer.valueOf(partitionId)),
      util.Collections.singletonList(Integer.valueOf(epoch)),
      util.Collections.singletonList(loc),
      util.Collections.singletonList(StatusCode.SOFT_SPLIT),
      isSegmentGranularityVisible = false)

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
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    changePartitionManager.advance(40000)
    val context = new CapturingContext
    reviveEpoch(changePartitionManager, context, shuffleId, partitionId, 0, loc0)

    assert(changePartitionManager.hotnessTracker.desiredLocationCount(shuffleId, partitionId) == 2)
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == Set(1))
    assert(newLocs.map(_.getHost).distinct.size == 1)

    // The reply carries the full writable set including the soft-split epoch 0.
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
  }

  test("gap larger than the candidate workers stacks locations by cycling over them") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 2).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    // fillTime 6s against the 60s target boosts desired to 10; with only 2 candidate
    // workers the 9-location gap must cycle and stack on them.
    changePartitionManager.advance(6000)
    val context = new CapturingContext
    reviveEpoch(changePartitionManager, context, shuffleId, partitionId, 0, loc0)

    assert(changePartitionManager.hotnessTracker.desiredLocationCount(shuffleId, partitionId) == 10)
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0)
    assert(newLocs.map(_.getEpoch).toSet == (1 to 9).toSet)
    assert(newLocs.map(_.getHost).toSet == Set("host1", "host2"))

    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 9)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == (0 to 8).toSet)
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
    reviveEpoch(changePartitionManager, context, shuffleId, partitionId, 0, loc0)

    assert(primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 0).isEmpty)
  }

  test("repeated SOFT_SPLIT reports of the same epoch boost only once") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    changePartitionManager.advance(40000)
    reviveEpoch(changePartitionManager, new CapturingContext, shuffleId, partitionId, 0, loc0)
    assert(changePartitionManager.hotnessTracker.desiredLocationCount(shuffleId, partitionId) == 2)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size

    // No boost, no new allocation, but the reporter still gets the current active set.
    changePartitionManager.advance(5000)
    val context = new CapturingContext
    reviveEpoch(changePartitionManager, context, shuffleId, partitionId, 0, loc0)

    assert(changePartitionManager.hotnessTracker.desiredLocationCount(shuffleId, partitionId) == 2)
    assert(primaryLocs(shuffleId, partitionId).size == locCountAfterBoost)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 1)
    assert(reply.additionals.asScala.map(_.getEpoch).toSet == Set(0))
  }

  test("request with active set already satisfied allocates 0 but still replies full set") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 3).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    // Boost to 2 writable locations first: writable epochs {0, 1}.
    changePartitionManager.advance(40000)
    reviveEpoch(changePartitionManager, new CapturingContext, shuffleId, partitionId, 0, loc0)
    val locCountAfterBoost = primaryLocs(shuffleId, partitionId).size
    val loc1 = primaryLocs(shuffleId, partitionId).find(_.getEpoch == 1).get

    // Slow retire of epoch 1 (no boost): the writable set already satisfies desired=2,
    // but the reply still carries the current writable set.
    changePartitionManager.advance(90000)
    val context = new CapturingContext
    reviveEpoch(changePartitionManager, context, shuffleId, partitionId, 1, loc1)

    assert(changePartitionManager.hotnessTracker.desiredLocationCount(shuffleId, partitionId) == 2)
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
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    // Boost desired to 2 first; same-epoch reports from other executors are deduped.
    changePartitionManager.hotnessTracker.onEpochRetired(
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

    // Exactly one fresh location allocated, idempotent for concurrent revives.
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

  test("one Revive message's many same-partition entries: retire reports get bookkeeping " +
    "only, the max-epoch entry drives the request") {
    val conf = makeConf()
    val shuffleId = 1
    val partitionId = 0
    val workers = (1 to 8).map(makeWorker)
    val loc0 = prepareLifecycleManager(conf, shuffleId, partitionId, workers)
    val changePartitionManager = new FakeClockManager(conf, lifecycleManager, 100000L)
    changePartitionManager.recordInitialAllocTime(shuffleId, Array(loc0), 1000, 100000L)

    // 5 entries of the same partition in one Revive message: 4 pure retire reports
    // (epochs 0..3) plus the max-epoch request (epoch 4), all HARD_SPLIT, no boost.
    changePartitionManager.advance(70000)
    val entries = (0 to 4).map { epoch =>
      (Integer.valueOf(epoch), makeLoc(partitionId, epoch, s"host${epoch + 1}"))
    }
    val context = new CapturingContext
    changePartitionManager.handlePartitionLocationRequests(
      context,
      shuffleId,
      util.Arrays.asList(Array.fill(entries.size)(Integer.valueOf(partitionId)): _*),
      util.Arrays.asList(entries.map(_._1): _*),
      util.Arrays.asList(entries.map(_._2): _*),
      util.Arrays.asList(Array.fill(entries.size)(StatusCode.HARD_SPLIT): _*),
      isSegmentGranularityVisible = false)

    // Every retired location is registered for commit (no data loss).
    val unhandled = lifecycleManager.commitManager.committedPartitionInfo
      .get(shuffleId)
      .unhandledPartitionLocations
    assert(unhandled.asScala.map(_.getEpoch).toSet == Set(0, 1, 2, 3, 4))

    // Exactly one replacement (epoch 5) is allocated and the single reply carries it.
    val newLocs = primaryLocs(shuffleId, partitionId).filter(_.getEpoch > 4)
    assert(newLocs.map(_.getEpoch) == List(5))
    assert(
      changePartitionManager.hotnessTracker.currentActiveEpochs(shuffleId, partitionId) ==
        Set(5))
    assert(context.replies.size() == 1)
    val reply = context.replies.get(partitionId)
    assert(reply != null && reply.status == StatusCode.SUCCESS)
    assert(reply.loc.isDefined && reply.loc.get.getEpoch == 5)
    assert(reply.additionals.isEmpty)
  }
}
