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

  private val adaptivePartitionWriteParallelismEnabled =
    conf.clientShuffleAdaptivePartitionWriteParallelismEnabled

  private[client] var nowMs: () => Long = () => System.currentTimeMillis()

  private[client] val hotnessTracker = new PartitionHotnessTracker(
    conf,
    latestEpoch,
    loc => lifecycleManager.workerStatusTracker.workerAvailableByLocation(loc))

  private[client] def recordInitialAllocTime(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation],
      numMappers: Int,
      nowMs: Long): Unit =
    hotnessTracker.recordInitialAllocTime(shuffleId, partitionLocations, numMappers, nowMs)

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
                      try {
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
                      } catch {
                        case e: InterruptedException =>
                          logError(
                            s"Batch handle change partition for shuffle $shuffleId interrupted.",
                            e)
                          throw e
                        case t: Throwable =>
                          logError(
                            s"Batch handle change partition for shuffle $shuffleId failed.",
                            t)
                      }
                    }
                  }
                }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
              case t: Throwable =>
                logError("Batch handle change partition scheduler failed.", t)
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

  def handlePartitionLocationRequests(
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionIds: util.List[Integer],
      oldEpochs: util.List[Integer],
      oldPartitions: util.List[PartitionLocation],
      causes: util.List[StatusCode],
      isSegmentGranularityVisible: Boolean): Unit = {
    (0 until partitionIds.size()).groupBy(partitionIds.get(_)).foreach {
      case (partitionId, indices) =>
        indices.foreach { idx =>
          recordEpochRetired(
            shuffleId,
            partitionId,
            oldEpochs.get(idx),
            oldPartitions.get(idx),
            Some(causes.get(idx)))
        }
        // Only the max-epoch entry can require new locations (gap allocation).
        val maxEpochIdx = indices.maxBy(idx => oldEpochs.get(idx).toInt)
        handleRequestPartitionLocation(
          context,
          shuffleId,
          partitionId,
          oldEpochs.get(maxEpochIdx),
          oldPartitions.get(maxEpochIdx),
          Some(causes.get(maxEpochIdx)),
          isSegmentGranularityVisible)
    }
  }

  private def recordEpochRetired(
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode]): Unit = {
    lifecycleManager.commitManager.registerCommitPartitionRequest(
      shuffleId,
      oldPartition,
      cause)

    hotnessTracker.onEpochRetired(shuffleId, partitionId, oldEpoch, oldPartition, cause, nowMs())
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

    if (!adaptivePartitionWriteParallelismEnabled) {
      lifecycleManager.commitManager.registerCommitPartitionRequest(
        shuffleId,
        oldPartition,
        cause)
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
              availableActiveLocations(shuffleId, partitionId)
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
   * The partition's active locations on available workers, epoch ascending. Locations on
   * unavailable workers are dropped and hard-retired in the tracker; falls back to the latest
   * location when nothing available remains.
   */
  private def availableActiveLocations(
      shuffleId: Int,
      partitionId: Int): List[PartitionLocation] = {
    val epochs = hotnessTracker.currentActiveEpochs(shuffleId, partitionId)
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
    val (available, unavailable) = fromSnapshots.partition(loc =>
      lifecycleManager.workerStatusTracker.workerAvailableByLocation(loc))
    if (unavailable.nonEmpty) {
      hotnessTracker.retireUnavailableWorkerEpochs(
        shuffleId,
        partitionId,
        unavailable.map(_.getEpoch).toSet)
    }
    if (available.nonEmpty) {
      available.sortBy(_.getEpoch)
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

    // remove together to reduce lock time
    def replySuccessActiveSet(): Unit = {
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
          val sorted = availableActiveLocations(shuffleId, partitionId)
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

    val candidates = collectCandidateWorkers(shuffleId)

    if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)) {
      logError("[Update partition] failed for not enough candidates for revive.")
      replyFailure(StatusCode.SLOT_NOT_AVAILABLE)
      return
    }

    // PartitionSplit all contains oldPartition
    val newlyAllocatedLocations =
      if (adaptivePartitionWriteParallelismEnabled) {
        allocateParallelLocations(shuffleId, changePartitions.toList, candidates.asScala.toList)
      } else {
        reallocateChangePartitionRequestSlotsFromCandidates(
          changePartitions.toList,
          candidates.asScala.toList)
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

    val newPrimaryLocations = newlyAllocatedLocations.asScala.flatMap {
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
      val changes = newPrimaryLocations.map { partition =>
        s"(partition ${partition.getId} epoch from ${partition.getEpoch - 1} to ${partition.getEpoch})"
      }.mkString("[", ", ", "]")
      logInfo(s"[Update partition] success for " +
        s"shuffle $shuffleId, succeed partitions: " +
        s"$changes.")
    }

    if (adaptivePartitionWriteParallelismEnabled) {
      registerReservedEpochs(shuffleId, newlyAllocatedLocations)
      replySuccessActiveSet()
    } else {
      replySuccess(newPrimaryLocations.toArray)
    }
  }

  private def registerReservedEpochs(
      shuffleId: Int,
      reservedLocations: WorkerResource): Unit = {
    val reservedEpochsByPartition = reservedLocations.asScala.values.flatMap {
      case (primaryLocations, replicaLocations) =>
        (primaryLocations.asScala ++ replicaLocations.asScala.map(_.getPeer))
          .filter(_ != null)
          .groupBy(_.getId)
    }.map { case (partitionId, locs) => partitionId -> locs.map(_.getEpoch).toSet }
    val allocTimeMs = nowMs()
    reservedEpochsByPartition.foreach { case (partitionId, epochs) =>
      hotnessTracker.registerAllocation(shuffleId, partitionId, epochs, allocTimeMs)
    }
  }

  /**
   * Allocate new locations for each requested partition by the gap between the desired count and
   * the current active count; new locations prefer mutually different workers (cycling over the
   * candidates when the gap exceeds their count) with increasing epochs.
   */
  private def allocateParallelLocations(
      shuffleId: Int,
      changePartitions: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    changePartitions.foreach { change =>
      val partitionId = change.partitionId
      // The tracker already caps desired at the configured maxLocations (or mapper count).
      val desired = hotnessTracker.desiredLocationCount(shuffleId, partitionId)
      // The active epoch set is already up to date: it was maintained when the request
      // arrived (soft-split epochs of available workers are retained as still writable;
      // hard/failed epochs were removed). No need to subtract the requested epoch here.
      val surviving = hotnessTracker.currentActiveEpochs(shuffleId, partitionId)
      val gap = math.max(0, desired - surviving.size)
      if (gap > 0) {
        val baseEpoch = math.max(
          latestEpoch(shuffleId, partitionId).getOrElse(change.epoch),
          (surviving + change.epoch).max)
        val newEpochs = allocateGapLocations(partitionId, baseEpoch, gap, candidates, slots)
        if (newEpochs.size < gap) {
          logWarning(s"[adaptiveParallelism] Shuffle $shuffleId partition $partitionId: " +
            s"wanted $gap additional location(s) but allocated ${newEpochs.size} " +
            s"(not enough candidate workers).")
        }
      }
    }
    slots
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
    var i = 0
    while (i < gap && candidates.size >= minCandidates) {
      // Distinct workers first; cycle over the candidates when the gap exceeds their count.
      if (remaining.size < minCandidates) {
        remaining = candidates
      }
      i += 1
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
    newEpochs.toSet
  }

  private[client] def collectCandidateWorkers(shuffleId: Int): util.HashSet[WorkerInfo] = {
    if (dynamicResourceEnabled) {
      lifecycleManager.refreshEndpointReadyWorkersFromMaster(shuffleId)
    }

    val snapshotCandidates =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .asScala
        .values
        .map(_.workerInfo)
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)
        .toSet
    val candidates = new util.HashSet[WorkerInfo](snapshotCandidates.asJava)
    if (dynamicResourceEnabled) {
      candidates.addAll(
        lifecycleManager.workerStatusTracker.endpointReadyWorkers
          .filter(lifecycleManager.workerStatusTracker.workerAvailable)
          .asJava)
    }
    candidates
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

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    changePartitionRequests.remove(shuffleId)
    inBatchPartitions.remove(shuffleId)
    locks.remove(shuffleId)
    hotnessTracker.removeShuffle(shuffleId)
  }
}
