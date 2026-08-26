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
import java.util.{function, Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

case class ChangePartitionRequest(
    context: RequestLocationCallContext,
    shuffleId: Int,
    partitionId: Int,
    epoch: Int,
    oldPartition: PartitionLocation,
    causes: Option[StatusCode])

class ChangePartitionManager(
    conf: CelebornConf,
    lifecycleManager: LifecycleManager) extends Logging {

  private val pushReplicateEnabled = conf.clientPushReplicateEnabled
  // shuffleId -> (partitionId -> set of ChangePartition)
  val changePartitionRequests
      : ConcurrentHashMap[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]] =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]]()

  // shuffleId -> locks
  private val locks = JavaUtils.newConcurrentHashMap[Int, Array[AnyRef]]()
  private val lockBucketSize = conf.batchHandleChangePartitionBuckets

  // shuffleId -> set of partition id
  private val inBatchPartitions =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap.KeySetView[Int, java.lang.Boolean]]()

  // shuffleId -> (partitionId -> revive times, i.e. the max epoch ever allocated,
  // as the epoch starts from 0 and increases by 1 for each successful revive)
  private val partitionReviveCounts =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, Integer]]()

  // shuffleId -> (revive cause -> times)
  private val reviveCauseCounts =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[StatusCode, java.lang.Long]]()

  private val batchHandleChangePartitionEnabled = conf.batchHandleChangePartitionEnabled
  private val batchHandleChangePartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "celeborn-client-lifecycle-manager-change-partition-executor",
    conf.batchHandleChangePartitionNumThreads)
  private val batchHandleChangePartitionRequestInterval =
    conf.batchHandleChangePartitionRequestInterval
  private val batchHandleChangePartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleChangePartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "celeborn-client-lifecycle-manager-change-partition-scheduler"))
    } else {
      None
    }

  private var batchHandleChangePartition: Option[ScheduledFuture[_]] = _

  private val testRetryRevive = conf.testRetryRevive

  private val dynamicResourceEnabled = conf.clientShuffleDynamicResourceEnabled
  private val dynamicResourceUnavailableFactor = conf.clientShuffleDynamicResourceFactor

  private val adaptivePartitionWriteParallelismEnabled =
    conf.clientShuffleAdaptivePartitionWriteParallelismEnabled
  private val adaptivePartitionWriteParallelismMaxLocations =
    conf.clientShuffleAdaptivePartitionWriteParallelismMaxLocations

  // Injectable clock for testing.
  private[client] var nowMs: () => Long = () => System.currentTimeMillis()

  // Per-partition hot state behind adaptive partition write parallelism: how many writable locations each
  // partition should have. All hot state handling is delegated to this tracker.
  private val hotnessTracker = new PartitionHotnessTracker(
    conf,
    latestEpoch,
    loc => lifecycleManager.workerStatusTracker.workerAvailableByLocation(loc))

  private[client] def onEpochRetired(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode],
      nowMs: Long): Unit =
    hotnessTracker.onEpochRetired(shuffleId, partitionId, epoch, oldPartition, cause, nowMs)

  private[client] def recordInitialAllocTime(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation],
      nowMs: Long): Unit =
    hotnessTracker.recordInitialAllocTime(shuffleId, partitionLocations, nowMs)

  private[client] def recordAllocTime(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      nowMs: Long): Unit =
    hotnessTracker.recordAllocTime(shuffleId, partitionId, epoch, nowMs)

  private[client] def desiredLocationCount(shuffleId: Int, partitionId: Int): Int =
    hotnessTracker.desiredLocationCount(shuffleId, partitionId)

  private def getOrCreateHotState(shuffleId: Int, partitionId: Int): HotState =
    hotnessTracker.getOrCreateHotState(shuffleId, partitionId)

  private def currentActiveEpochs(shuffleId: Int, partitionId: Int): Set[Int] =
    hotnessTracker.currentActiveEpochs(shuffleId, partitionId)

  def start(): Unit = {
    batchHandleChangePartition = batchHandleChangePartitionSchedulerThread.map {
      // noinspection ConvertExpressionToSAM
      _.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            try {
              changePartitionRequests.asScala.foreach { case (shuffleId, requests) =>
                batchHandleChangePartitionExecutors.submit {
                  new Runnable {
                    override def run(): Unit = {
                      val distinctPartitions = {
                        val requestSet = inBatchPartitions.get(shuffleId)
                        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
                        requests.asScala.map { case (partitionId, request) =>
                          locksForShuffle(partitionId % locksForShuffle.length).synchronized {
                            if (!requestSet.contains(partitionId) && requests.containsKey(
                                partitionId)) {
                              requestSet.add(partitionId)
                              Some(request.asScala.toArray.maxBy(_.epoch))
                            } else {
                              None
                            }
                          }
                        }.filter(_.isDefined).map(_.get).toArray
                      }
                      if (distinctPartitions.nonEmpty) {
                        handleRequestPartitions(
                          shuffleId,
                          distinctPartitions,
                          lifecycleManager.commitManager.isSegmentGranularityVisible(shuffleId))
                      }
                    }
                  }
                }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
            }
          }
        },
        0,
        batchHandleChangePartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    batchHandleChangePartition.foreach(_.cancel(true))
    batchHandleChangePartitionSchedulerThread.foreach(ThreadUtils.shutdown(_))
  }

  val rpcContextRegisterFunc
      : function.Function[Int, ConcurrentHashMap[Integer, JSet[ChangePartitionRequest]]] =
    new util.function.Function[
      Int,
      ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]] =
        JavaUtils.newConcurrentHashMap()
    }

  private val inBatchShuffleIdRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap.KeySetView[Int, java.lang.Boolean]]() {
      override def apply(s: Int): ConcurrentHashMap.KeySetView[Int, java.lang.Boolean] =
        ConcurrentHashMap.newKeySet[Int]()
    }

  private val locksRegisterFunc = new util.function.Function[Int, Array[AnyRef]] {
    override def apply(t: Int): Array[AnyRef] = {
      Array.fill(lockBucketSize)(new AnyRef())
    }
  }

  private val reviveCountsRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap[Integer, Integer]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, Integer] =
        JavaUtils.newConcurrentHashMap()
    }

  private val reviveCausesRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap[StatusCode, java.lang.Long]]() {
      override def apply(s: Int): ConcurrentHashMap[StatusCode, java.lang.Long] =
        JavaUtils.newConcurrentHashMap()
    }

  private val maxEpochMergeFunc = new util.function.BiFunction[Integer, Integer, Integer] {
    override def apply(oldEpoch: Integer, newEpoch: Integer): Integer =
      math.max(oldEpoch, newEpoch)
  }

  private val countMergeFunc =
    new util.function.BiFunction[java.lang.Long, java.lang.Long, java.lang.Long] {
      override def apply(oldCount: java.lang.Long, newCount: java.lang.Long): java.lang.Long =
        oldCount + newCount
    }

  def handleRequestPartitionLocation(
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode] = None,
      isSegmentGranularityVisible: Boolean): Unit = {

    val changePartition = ChangePartitionRequest(
      context,
      shuffleId,
      partitionId,
      oldEpoch,
      oldPartition,
      cause)
    // check if there exists request for the partition, if do just register
    val requests = changePartitionRequests.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    inBatchPartitions.computeIfAbsent(shuffleId, inBatchShuffleIdRegisterFunc)

    lifecycleManager.commitManager.registerCommitPartitionRequest(
      shuffleId,
      oldPartition,
      cause)

    // The requested epoch is retiring: update the active epoch set of the partition (soft-split
    // epochs of available workers stay writable and are retained; hard/failed ones are removed)
    // and, when the retire is measure-eligible, judge whether the partition is hot and needs
    // more locations.
    if (adaptivePartitionWriteParallelismEnabled && oldEpoch >= 0) {
      onEpochRetired(shuffleId, partitionId, oldEpoch, oldPartition, cause, nowMs())
    }

    val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
    locksForShuffle(partitionId % locksForShuffle.length).synchronized {
      if (requests.containsKey(partitionId)) {
        logDebug(s"[handleRequestPartitionLocation] For shuffle: $shuffleId, request for same " +
          s"partition: $partitionId-$oldEpoch exists, register context.")
        requests.get(partitionId).add(changePartition)
        return
      } else {
        getLatestPartition(shuffleId, partitionId, oldEpoch).foreach { latestLoc =>
          val additionalLocs =
            if (adaptivePartitionWriteParallelismEnabled) {
              currentActiveLocations(shuffleId, partitionId)
                .filter(_.getEpoch != latestLoc.getEpoch)
                .asJava
            } else {
              util.Collections.emptyList[PartitionLocation]()
            }
          context.reply(
            partitionId,
            StatusCode.SUCCESS,
            Some(latestLoc),
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(oldPartition),
            additionalLocs)
          logDebug(s"[handleRequestPartitionLocation]: For shuffle: $shuffleId," +
            s" old partition: $partitionId-$oldEpoch, new partition: $latestLoc found, return it")
          return
        }
        val set = new util.HashSet[ChangePartitionRequest]()
        set.add(changePartition)
        requests.put(partitionId, set)
      }
    }
    if (!batchHandleChangePartitionEnabled) {
      handleRequestPartitions(shuffleId, Array(changePartition), isSegmentGranularityVisible)
    }
  }

  private def latestEpoch(shuffleId: Int, partitionId: Int): Option[Int] = {
    val map = lifecycleManager.latestPartitionLocation.get(shuffleId)
    if (map == null) {
      None
    } else {
      Option(map.get(partitionId)).map(_.getEpoch)
    }
  }

  /**
   * The full set of currently active locations of a partition, looked up from worker
   * snapshots by the active epochs. Falls back to the latest location when snapshots
   * have no record (e.g. incrementally committed locations).
   */
  private def currentActiveLocations(
      shuffleId: Int,
      partitionId: Int): List[PartitionLocation] = {
    val epochs = currentActiveEpochs(shuffleId, partitionId)
    val fromSnapshots = lifecycleManager
      .workerSnapshots(shuffleId)
      .asScala
      .values
      .flatMap(_.getPrimaryPartitions(Some(partitionId)).asScala)
      .filter(loc => epochs.contains(loc.getEpoch))
      .toList
      .groupBy(_.getEpoch)
      .map(_._2.head)
      .toList
    if (fromSnapshots.nonEmpty) {
      fromSnapshots
    } else {
      val map = lifecycleManager.latestPartitionLocation.get(shuffleId)
      if (map == null) List.empty else Option(map.get(partitionId)).toList
    }
  }

  private def getLatestPartition(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int): Option[PartitionLocation] = {
    val map = lifecycleManager.latestPartitionLocation.get(shuffleId)
    if (map != null) {
      val loc = map.get(partitionId)
      if (loc != null && loc.getEpoch > epoch) {
        return Some(loc)
      }
    }
    None
  }

  def handleRequestPartitions(
      shuffleId: Int,
      changePartitions: Array[ChangePartitionRequest],
      isSegmentGranularityVisible: Boolean): Unit = {
    val requestsMap = changePartitionRequests.get(shuffleId)

    val changes = changePartitions.map { change =>
      s"${change.shuffleId}-${change.partitionId}-${change.epoch}"
    }.mkString("[", ",", "]")
    logWarning(s"Batch handle change partition for $changes")

    // Exclude all failed workers
    if (changePartitions.exists(_.causes.isDefined) && !testRetryRevive) {
      changePartitions.filter(_.causes.isDefined).foreach { changePartition =>
        lifecycleManager.workerStatusTracker.excludeWorkerFromPartition(
          shuffleId,
          changePartition.oldPartition,
          changePartition.causes.get)
      }
    }

    // remove together to reduce lock time
    def replySuccess(locations: Array[PartitionLocation]): Unit = {
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      locations.map { location =>
        locksForShuffle(location.getId % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(location.getId)
          }
          // Here one partition id can be remove more than once,
          // so need to filter null result before reply.
          location -> Option(requestsMap.remove(location.getId))
        }
      }.foreach { case (newLocation, requests) =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            StatusCode.SUCCESS,
            Option(newLocation),
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))))
      }
    }

    // remove together to reduce lock time
    def replyFailure(status: StatusCode): Unit = {
      changePartitions.map { changePartition =>
        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
        locksForShuffle(changePartition.partitionId % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(changePartition.partitionId)
          }
          Option(requestsMap.remove(changePartition.partitionId))
        }
      }.foreach { requests =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            status,
            None,
            lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))))
      }
    }

    // remove together to reduce lock time. Adaptive parallelism: allocation is decoupled from
    // replying — every request is replied with the current full active set of the partition
    // (max epoch location as the primary reply, the rest as additional locations), so all
    // executors converge to the same active set even when this round allocates nothing.
    def replySuccessFullSet(): Unit = {
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      changePartitions.map { changePartition =>
        changePartition.partitionId ->
          locksForShuffle(changePartition.partitionId % locksForShuffle.length).synchronized {
            if (batchHandleChangePartitionEnabled) {
              inBatchPartitions.get(shuffleId).remove(changePartition.partitionId)
            }
            Option(requestsMap.remove(changePartition.partitionId))
          }
      }.foreach { case (partitionId, requests) =>
        requests.foreach { requestSet =>
          val sorted = currentActiveLocations(shuffleId, partitionId).sortBy(_.getEpoch)
          val maxLoc = sorted.lastOption.orNull
          val additionalLocs = sorted.dropRight(1).asJava
          requestSet.asScala.toList.foreach(req =>
            req.context.reply(
              req.partitionId,
              StatusCode.SUCCESS,
              Option(maxLoc),
              lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition),
              additionalLocs))
        }
      }
    }

    val candidates = new util.HashSet[WorkerInfo]()
    val newlyRequestedLocations = new WorkerResource()

    val snapshotCandidates =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .asScala
        .values
        .map(_.workerInfo)
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)
        .toSet
        .asJava
    candidates.addAll(snapshotCandidates)

    if (dynamicResourceEnabled) {
      val shuffleAllocatedWorkers = lifecycleManager.workerSnapshots(shuffleId).size()
      val unavailableWorkerRatio = 1 - (snapshotCandidates.size * 1.0 / shuffleAllocatedWorkers)
      if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)
        || (unavailableWorkerRatio >= dynamicResourceUnavailableFactor)) {

        // get new available workers for the request partition ids
        val partitionIds = new util.ArrayList[Integer](
          changePartitions.map(_.partitionId).map(Integer.valueOf).toList.asJava)
        // The partition id value is not important here because we're just trying to get the workers to use
        val requestSlotsRes =
          lifecycleManager.requestMasterRequestSlotsWithRetry(shuffleId, partitionIds)

        requestSlotsRes.status match {
          case StatusCode.REQUEST_FAILED =>
            logInfo(s"ChangePartition requestSlots RPC request failed for $shuffleId!")
          case StatusCode.SLOT_NOT_AVAILABLE =>
            logInfo(s"ChangePartition requestSlots for $shuffleId failed, have no available slots.")
          case StatusCode.SUCCESS =>
            logDebug(
              s"ChangePartition requestSlots request for workers Success! shuffleId: $shuffleId availableWorkers Info: ${requestSlotsRes.workerResource.keySet()}")
          case StatusCode.WORKER_EXCLUDED =>
            logInfo(s"ChangePartition requestSlots request for workers for $shuffleId failed due to all workers be excluded!")
          case _ => // won't happen
            throw new UnsupportedOperationException()
        }

        if (requestSlotsRes.status.equals(StatusCode.SUCCESS)) {
          requestSlotsRes.workerResource.keySet().asScala.foreach { workerInfo: WorkerInfo =>
            newlyRequestedLocations.computeIfAbsent(workerInfo, lifecycleManager.newLocationFunc)
          }

          // SetupEndpoint for new Workers
          val workersRequireEndpoints = new util.HashSet[WorkerInfo](
            requestSlotsRes.workerResource.keySet()
              .asScala
              .filter(lifecycleManager.workerStatusTracker.workerAvailable)
              .asJava)

          val connectFailedWorkers = new ShuffleFailedWorkers()
          lifecycleManager.setupEndpoints(
            workersRequireEndpoints,
            shuffleId,
            connectFailedWorkers)
          workersRequireEndpoints.removeAll(connectFailedWorkers.asScala.keys.toList.asJava)
          candidates.addAll(workersRequireEndpoints)

          // Update worker status
          lifecycleManager.workerStatusTracker.recordWorkerFailure(connectFailedWorkers)
          lifecycleManager.workerStatusTracker.removeFromExcludedWorkers(candidates)
        }
      }
    }

    if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)) {
      logError("[Update partition] failed for not enough candidates for revive.")
      replyFailure(StatusCode.SLOT_NOT_AVAILABLE)
      return
    }

    // PartitionSplit all contains oldPartition
    val (newlyAllocatedLocations, parallelAllocations) =
      if (adaptivePartitionWriteParallelismEnabled) {
        allocateParallelLocations(
          shuffleId,
          changePartitions.toList,
          candidates.asScala.toList)
      } else {
        (
          reallocateChangePartitionRequestSlotsFromCandidates(
            changePartitions.toList,
            candidates.asScala.toList),
          Map.empty[Int, ParallelAllocation])
      }

    if (!lifecycleManager.reserveSlotsWithRetry(
        shuffleId,
        candidates,
        newlyAllocatedLocations,
        isSegmentGranularityVisible = isSegmentGranularityVisible)) {
      logError(s"[Update partition] failed for $shuffleId.")
      replyFailure(StatusCode.RESERVE_SLOTS_FAILED)
      return
    }

    // newlyRequestedLocations is empty if dynamicResourceEnabled is false
    newlyRequestedLocations.putAll(newlyAllocatedLocations)

    val newPrimaryLocations = newlyRequestedLocations.asScala.flatMap {
      case (workInfo, (primaryLocations, replicaLocations)) =>
        // Add all re-allocated slots to worker snapshots.
        val partitionLocationInfo = lifecycleManager.workerSnapshots(shuffleId).computeIfAbsent(
          workInfo.toUniqueId,
          new util.function.Function[String, ShufflePartitionLocationInfo] {
            override def apply(workerId: String): ShufflePartitionLocationInfo = {
              new ShufflePartitionLocationInfo(workInfo)
            }
          })
        partitionLocationInfo.addPrimaryPartitions(primaryLocations)
        partitionLocationInfo.addReplicaPartitions(replicaLocations)
        lifecycleManager.updateLatestPartitionLocations(shuffleId, primaryLocations)

        // partition location can be null when call reserveSlotsWithRetry().
        val locations = (primaryLocations.asScala ++ replicaLocations.asScala.map(_.getPeer))
          .distinct.filter(_ != null)
        // TODO: should record the new partition locations and acknowledge the new partitionLocations to downstream task,
        //  in scenario the downstream task start early before the upstream task.
        locations
    }

    if (newPrimaryLocations.nonEmpty) {
      // newPrimaryLocations may contain both the primary and the replica peer of one
      // partition, dedupe by partition id before recording stats and logging.
      val distinctPartitions = newPrimaryLocations.groupBy(_.getId).map(_._2.head)
      val requestCauses = changePartitions.map(c => c.partitionId -> c.causes).toMap
      val reviveCounts =
        partitionReviveCounts.computeIfAbsent(shuffleId, reviveCountsRegisterFunc)
      val causeCounts = reviveCauseCounts.computeIfAbsent(shuffleId, reviveCausesRegisterFunc)
      val changes = distinctPartitions.map { partition =>
        val partitionId = partition.getId
        // The epoch of the new location equals the revive times of the partition.
        reviveCounts.merge(partitionId, partition.getEpoch, maxEpochMergeFunc)
        val cause = requestCauses.get(partitionId).flatten
        cause.foreach(c => causeCounts.merge(c, 1L, countMergeFunc))
        s"(partition $partitionId epoch from ${partition.getEpoch - 1} to ${partition.getEpoch}" +
          s", cause ${cause.map(_.name()).getOrElse("NONE")})"
      }.mkString("[", ", ", "]")
      logInfo(s"[Update partition] success for " +
        s"shuffle $shuffleId, succeed partitions: " +
        s"$changes.")
    }

    // Register the hot state of revived partitions (sparse): record the allocation time of
    // every newly reserved epoch and the surviving active epoch set.
    if (adaptivePartitionWriteParallelismEnabled) {
      val allocTimeMs = nowMs()
      parallelAllocations.foreach { case (partitionId, allocation) =>
        if (allocation.newEpochs.nonEmpty) {
          val entry = getOrCreateHotState(shuffleId, partitionId)
          entry.synchronized {
            (allocation.survivingEpochs ++ allocation.newEpochs).foreach { epoch =>
              val boxed = Integer.valueOf(epoch)
              if (!entry.activeEpochs.contains(boxed)) {
                entry.activeEpochs.add(boxed)
              }
            }
            allocation.newEpochs.foreach { epoch =>
              entry.allocTimeMs.putIfAbsent(epoch, allocTimeMs)
            }
          }
        }
      }
    }

    if (adaptivePartitionWriteParallelismEnabled) {
      replySuccessFullSet()
    } else {
      replySuccess(newPrimaryLocations.toArray)
    }
  }

  private case class ParallelAllocation(
      survivingEpochs: Set[Int],
      newEpochs: Set[Int])

  /**
   * Allocate new locations for each requested partition by the gap between the desired
   * location count (judged locally from eligible split reports, capped at the configured upper
   * bound) and the current active location count. The gap can be 0 when another executor
   * has already triggered the allocation. Newly allocated locations of one partition are
   * placed on mutually different workers (best effort) with increasing epochs.
   */
  private def allocateParallelLocations(
      shuffleId: Int,
      changePartitions: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): (WorkerResource, Map[Int, ParallelAllocation]) = {
    val slots = new WorkerResource()
    val allocations = scala.collection.mutable.Map[Int, ParallelAllocation]()
    changePartitions.foreach { change =>
      val partitionId = change.partitionId
      val desired = math.min(
        desiredLocationCount(shuffleId, partitionId),
        adaptivePartitionWriteParallelismMaxLocations)
      // The active epoch set is already up to date: it was maintained when the request
      // arrived (soft-split epochs of available workers are retained as still writable;
      // hard/failed epochs were removed). No need to subtract the requested epoch here.
      val surviving = currentActiveEpochs(shuffleId, partitionId)
      val gap = math.max(0, desired - surviving.size)
      if (gap > 0) {
        val baseEpoch = math.max(
          latestEpoch(shuffleId, partitionId).getOrElse(change.epoch),
          (surviving + change.epoch).max)
        val newEpochs = allocateGapLocations(partitionId, baseEpoch, gap, candidates, slots)
        if (newEpochs.nonEmpty) {
          val hosts = slots.asScala.flatMap { case (worker, (primaries, replicas)) =>
            val hit = primaries.asScala.exists(loc =>
              loc.getId == partitionId && newEpochs.contains(loc.getEpoch)) ||
              replicas.asScala.exists(loc =>
                loc.getId == partitionId && newEpochs.contains(loc.getEpoch))
            if (hit) Some(worker.host) else None
          }.toSet
          logInfo(s"[adaptiveParallelism] Shuffle $shuffleId partition $partitionId: " +
            s"allocated ${newEpochs.size} additional location(s) (desired $desired, " +
            s"surviving ${surviving.size}), epochs ${newEpochs.toSeq.sorted.mkString(",")} " +
            s"on workers ${hosts.mkString(",")}.")
        }
        if (newEpochs.size < gap) {
          logWarning(s"[adaptiveParallelism] Shuffle $shuffleId partition $partitionId: " +
            s"wanted $gap additional location(s) but allocated ${newEpochs.size} " +
            s"(not enough candidate workers).")
        }
        allocations.put(partitionId, ParallelAllocation(surviving, newEpochs))
      } else {
        allocations.put(partitionId, ParallelAllocation(surviving, Set.empty))
      }
    }
    (slots, allocations.toMap)
  }

  private def allocateGapLocations(
      partitionId: Int,
      baseEpoch: Int,
      gap: Int,
      candidates: List[WorkerInfo],
      slots: WorkerResource): Set[Int] = {
    val minCandidates = if (pushReplicateEnabled) 2 else 1
    var remaining = candidates
    val newEpochs = scala.collection.mutable.Set[Int]()
    (1 to gap).foreach { i =>
      if (remaining.size >= minCandidates) {
        val newEpoch = baseEpoch + i
        lifecycleManager.allocateFromCandidates(partitionId, newEpoch - 1, remaining, slots)
        val chosenWorkers = slots.asScala.flatMap {
          case (worker, (primaries, replicas)) =>
            val hit = primaries.asScala.exists(loc =>
              loc.getId == partitionId && loc.getEpoch == newEpoch) ||
              replicas.asScala.exists(loc =>
                loc.getId == partitionId && loc.getEpoch == newEpoch)
            if (hit) Some(worker) else None
        }.toSet
        newEpochs += newEpoch
        remaining = remaining.filterNot(chosenWorkers.contains)
      }
    }
    newEpochs.toSet
  }

  private def reallocateChangePartitionRequestSlotsFromCandidates(
      changePartitionRequests: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    changePartitionRequests.foreach { partition =>
      lifecycleManager.allocateFromCandidates(
        partition.partitionId,
        partition.epoch,
        candidates,
        slots)
    }
    slots
  }

  def logReviveSummary(shuffleId: Int, topPartitionBytes: String = "NONE"): Unit = {
    val reviveCounts = partitionReviveCounts.get(shuffleId)
    if (reviveCounts == null || reviveCounts.isEmpty) {
      return
    }

    val totalReviveTimes = reviveCounts.values().asScala.map(_.toInt).sum
    val causeCounts = reviveCauseCounts.get(shuffleId)
    val causes =
      if (causeCounts == null || causeCounts.isEmpty) {
        "NONE"
      } else {
        causeCounts.asScala.map { case (cause, count) => s"${cause.name()}=$count" }
          .mkString(", ")
      }
    val topRevivedPartitions = reviveCounts.asScala.toSeq
      .sortBy(-_._2.toInt)
      .take(20)
      .map { case (partitionId, times) => s"(partition $partitionId, revive times $times)" }
      .mkString("[", ", ", "]")
    logInfo(s"Shuffle $shuffleId partition revive summary: total revive times " +
      s"$totalReviveTimes, revived partition num ${reviveCounts.size()}, " +
      s"causes: [$causes], top revived partitions: $topRevivedPartitions, " +
      s"top partition bytes: $topPartitionBytes.")
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    changePartitionRequests.remove(shuffleId)
    inBatchPartitions.remove(shuffleId)
    locks.remove(shuffleId)
    partitionReviveCounts.remove(shuffleId)
    reviveCauseCounts.remove(shuffleId)
    hotnessTracker.removeShuffle(shuffleId)
  }
}
