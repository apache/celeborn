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
    clientMaxEpoch: Int,
    targetEpoch: Int,
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

  // shuffleId -> (partitionId -> maxTargetEpoch)
  // maxTargetEpoch should be java.lang.Integer because we will remove
  // element by setting the value to null which is not supported by Scala.Int
  private val inBatchPartitions =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, Integer]]()
  private val asyncSplitPartitionEnabled = conf.asyncSplitPartitionEnabled

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
                      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
                      // For each partition only need handle one request that has the largest targetEpoch
                      val distinctPartitions = requests.asScala.collect {
                        case (partitionId, requestsForPartition) =>
                          locksForShuffle(partitionId % locksForShuffle.length).synchronized {
                            requestsForPartition.asScala.maxBy(_.targetEpoch) match {
                              case request
                                  if request.targetEpoch > inBatchPartitions.get(
                                    shuffleId).getOrDefault(partitionId, -1) =>
                                inBatchPartitions.get(shuffleId).put(
                                  partitionId,
                                  request.targetEpoch)
                                Some(request)
                              case _ => None
                            }
                          }
                      }.flatten.toArray
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
    new util.function.Function[Int, ConcurrentHashMap[Integer, Integer]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, Integer] =
        JavaUtils.newConcurrentHashMap()
    }

  private val locksRegisterFunc = new util.function.Function[Int, Array[AnyRef]] {
    override def apply(t: Int): Array[AnyRef] = {
      Array.fill(lockBucketSize)(new AnyRef())
    }
  }

  def reportAndSplitPartitionIfNeeded(
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      clientMaxEpoch: Int,
      currentMaxEpochId: Int,
      cause: Option[StatusCode],
      requests: ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]]): Unit = {
    val changed =
      lifecycleManager.reportPartitionSplitOrRevived(shuffleId, partitionId, oldEpoch, cause)
    val nextReserveSlotCount =
      if (changed) {
        lifecycleManager.getNextReserveSlotCount(shuffleId, partitionId)
      } else {
        0
      }
    if (nextReserveSlotCount > 0) {
      logInfo(
        s"Reserve slot for shuffleId: $shuffleId, partitionId: $partitionId, oldEpoch: $oldEpoch, clientMaxEpoch: $clientMaxEpoch," +
          s" next reserve count: $nextReserveSlotCount, currentMaxEpochId: $currentMaxEpochId, targetEpochId: ${currentMaxEpochId + nextReserveSlotCount}")
      val preAllocatePartitionRequest = ChangePartitionRequest(
        null,
        shuffleId,
        partitionId,
        clientMaxEpoch,
        targetEpoch = currentMaxEpochId + nextReserveSlotCount,
        oldPartition,
        cause)
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      locksForShuffle(partitionId % locksForShuffle.length).synchronized {
        if (requests.containsKey(partitionId)) {
          logDebug(s"[handleRequestPartitionLocation] For shuffle: $shuffleId, request for same " +
            s"partition: $partitionId-$oldEpoch exists, register context.")
          requests.get(partitionId).add(preAllocatePartitionRequest)
          return
        } else {
          val set = new util.HashSet[ChangePartitionRequest]()
          set.add(preAllocatePartitionRequest)
          requests.put(partitionId, set)
          logInfo(s"[handleRequestPartition][PreAllocate] for shuffleId: $shuffleId, partitionId: $partitionId, " +
            s"oldEpoch: $oldEpoch, clientMaxEpoch: $clientMaxEpoch, oldHost: ${if (oldPartition == null) null
            else oldPartition.getHost}, cause: $cause")
        }
      }
    }
  }

  def handleRequestPartitionLocation(
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      clientMaxEpoch: Int,
      reportOnly: Boolean,
      cause: Option[StatusCode] = None,
      isSegmentGranularityVisible: Boolean): Unit = {

    val currentMaxEpochId = lifecycleManager.getCurrentMaxPartitionEpochId(shuffleId, partitionId)
    val changePartition = ChangePartitionRequest(
      context,
      shuffleId,
      partitionId,
      clientMaxEpoch,
      targetEpoch = currentMaxEpochId + 1,
      oldPartition,
      cause)
    // check if there exists request for the partition, if do just register
    val requests = changePartitionRequests.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    inBatchPartitions.computeIfAbsent(shuffleId, inBatchShuffleIdRegisterFunc)

    lifecycleManager.commitManager.registerCommitPartitionRequest(
      shuffleId,
      oldPartition,
      cause)
    if (asyncSplitPartitionEnabled && batchHandleChangePartitionEnabled) {
      reportAndSplitPartitionIfNeeded(
        shuffleId,
        partitionId,
        oldEpoch,
        oldPartition,
        clientMaxEpoch,
        currentMaxEpochId,
        cause,
        requests)
    }
    if (reportOnly) {
      // if reportOnly is false, we will exclude failed workers in handleRequestPartitions
      if (cause.isDefined) {
        lifecycleManager.workerStatusTracker.excludeWorkerFromPartition(
          shuffleId,
          oldPartition,
          cause.get)
      }
      val latestLocations = getLatestPartition(shuffleId, partitionId, clientMaxEpoch)
      if (latestLocations.isDefined) {
        logDebug(
          s"New partition found, old partition $partitionId-$oldEpoch-$clientMaxEpoch return it." +
            s" shuffleId: $shuffleId ${latestLocations.get}")
      }
      context.reply(
        partitionId,
        StatusCode.SUCCESS,
        latestLocations,
        lifecycleManager.workerStatusTracker.workerAvailableByLocation(oldPartition))
      return
    } else {
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      locksForShuffle(partitionId % locksForShuffle.length).synchronized {
        if (requests.containsKey(partitionId)) {
          logDebug(s"[handleRequestPartitionLocation] For shuffle: $shuffleId, request for same " +
            s"partition: $partitionId-$oldEpoch exists, register context.")
          requests.get(partitionId).add(changePartition)
          return
        } else {
          getLatestPartition(shuffleId, partitionId, oldEpoch).foreach { latestLoc =>
            context.reply(
              partitionId,
              StatusCode.SUCCESS,
              Some(latestLoc),
              lifecycleManager.workerStatusTracker.workerAvailableByLocation(oldPartition))
            logDebug(s"[handleRequestPartitionLocation]: For shuffle: $shuffleId," +
              s" old partition: $partitionId-$oldEpoch, new partition: $latestLoc found, return it")
            return
          }
          val set = new util.HashSet[ChangePartitionRequest]()
          set.add(changePartition)
          requests.put(partitionId, set)
          logInfo(
            s"[handleRequestPartition][ChangePartition] for shuffleId: $shuffleId, partitionId: $partitionId, " +
              s"oldEpoch: $oldEpoch, clientMaxEpoch: $clientMaxEpoch, oldHost: ${if (oldPartition == null) null
              else oldPartition.getHost}, cause: $cause")
        }
      }
    }
    if (!batchHandleChangePartitionEnabled) {
      handleRequestPartitions(shuffleId, Array(changePartition), isSegmentGranularityVisible)
    }
  }

  private def getLatestPartition(
      shuffleId: Int,
      partitionId: Int,
      clientMaxEpoch: Int): Option[Seq[PartitionLocation]] = {
    val map = lifecycleManager.partitionLocationMonitors.getOrDefault(shuffleId, null)
    if (map != null && map.getOrDefault(partitionId, null) != null) {
      val activeLocations = map.get(partitionId).getActiveLocations(clientMaxEpoch)
      if (activeLocations.nonEmpty) {
        return Some(activeLocations)
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
      s"${change.shuffleId}-${change.partitionId}-${change.clientMaxEpoch}-${change.targetEpoch}"
    }.mkString("[", ",", "]")
    logInfo(s"Batch handle change partition for $changes")

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
      val partitionsMap = locations.groupBy(_.getId)
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      partitionsMap.map { case (partitionId, locations) =>
        locksForShuffle(partitionId % locksForShuffle.length).synchronized {
          val largestEpochId = locations.maxBy(_.getEpoch).getEpoch
          var stillProcessing = false
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).computeIfPresent(
              partitionId,
              (_, v) =>
                if (v <= largestEpochId) {
                  null
                } else {
                  stillProcessing = true
                  v
                })
          }
          // Here one partition id can be remove more than once,
          // so need to filter null result before reply.
          if (stillProcessing) {
            None
          } else {
            Option(requestsMap.remove(partitionId))
          }
        }
      }.foreach { requests =>
        requests.map(_.asScala.toList.foreach(req =>
          if (req.context != null) { // only urgent request has context
            req.context.reply(
              req.partitionId,
              StatusCode.SUCCESS,
              getLatestPartition(shuffleId, req.partitionId, req.clientMaxEpoch),
              lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))
          }))
      }
    }

    // remove together to reduce lock time
    def replyFailure(status: StatusCode): Unit = {
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      changePartitions.groupBy(_.partitionId).map {
        case (partitionId, requests) =>
          locksForShuffle(partitionId % locksForShuffle.length).synchronized {
            val largestEpochId = requests.maxBy(_.targetEpoch).targetEpoch
            var stillProcessing = false
            if (batchHandleChangePartitionEnabled) {
              inBatchPartitions.get(shuffleId).computeIfPresent(
                partitionId,
                (_, v) =>
                  if (v <= largestEpochId) {
                    null
                  } else {
                    stillProcessing = true
                    v
                  })
            }
            if (stillProcessing) {
              None
            } else {
              Option(requestsMap.remove(partitionId))
            }
          }
      }.foreach { requests =>
        requests.map(_.asScala.toList.foreach(req =>
          if (req.context != null) { // only urgent request has context
            req.context.reply(
              req.partitionId,
              status,
              getLatestPartition(shuffleId, req.partitionId, req.clientMaxEpoch),
              lifecycleManager.workerStatusTracker.workerAvailableByLocation(req.oldPartition))
          }))
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
    val newlyAllocatedLocations =
      reallocateChangePartitionRequestSlotsFromCandidates(
        changePartitions.toList,
        candidates.asScala.toList)

    if (!lifecycleManager.reserveSlotsWithRetry(
        shuffleId,
        candidates,
        newlyAllocatedLocations,
        isSegmentGranularityVisible = isSegmentGranularityVisible)) {
      logError(s"[Update partition] failed for $shuffleId.")
      // TODO: if partial success, maybe we could reply partial success.
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
        lifecycleManager.addNewPartitionLocations(shuffleId, primaryLocations)

        // partition location can be null when call reserveSlotsWithRetry().
        val locations = (primaryLocations.asScala ++ replicaLocations.asScala.map(_.getPeer))
          .distinct.filter(_ != null)
        if (locations.nonEmpty) {
          val changes = locations.map { partition =>
            s"(partition ${partition.getId} epoch changes to ${partition.getEpoch}, new host to ${partition.getHost})"
          }.mkString("[", ", ", "]")
          logInfo(s"[Update partition] success for " +
            s"shuffle $shuffleId, succeed partitions: " +
            s"$changes.")
        }

        // TODO: should record the new partition locations and acknowledge the new partitionLocations to downstream task,
        //  in scenario the downstream task start early before the upstream task.
        locations
    }
    replySuccess(newPrimaryLocations.toArray)
  }

  private def reallocateChangePartitionRequestSlotsFromCandidates(
      changePartitionRequests: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    changePartitionRequests.foreach { request =>
      val epochIds = lifecycleManager.allocateEpochIdsAndUpdateCurrentMaxEpoch(
        request.shuffleId,
        request.partitionId,
        request.targetEpoch)
      logInfo(s"allocate for shuffleId: ${request.shuffleId}, " +
        s"partitionId ${request.partitionId}, epochIds: ${epochIds.mkString("(", ", ", ")")}")
      epochIds.foreach {
        epochId =>
          lifecycleManager.allocateFromCandidates(
            request.partitionId,
            epochId,
            candidates,
            slots)
      }
    }
    slots
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    changePartitionRequests.remove(shuffleId)
    inBatchPartitions.remove(shuffleId)
    locks.remove(shuffleId)
  }
}
