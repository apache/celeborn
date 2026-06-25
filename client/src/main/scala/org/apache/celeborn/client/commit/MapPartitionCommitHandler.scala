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

package org.apache.celeborn.client.commit

import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ThreadPoolExecutor}
import java.util.concurrent.atomic.{AtomicInteger, AtomicIntegerArray}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.client.{ShuffleCommittedInfo, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers}
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.common.write.LocationPushFailedBatches

/**
 * This commit handler is for MapPartition ShuffleType, which means that a Map Partition contains all data produced
 * by an upstream MapTask, and data in a Map Partition may be consumed by multiple ReduceTasks. If the upstream MapTask
 * has multiple outputs, each will be a Map Partition.
 *
 * @see [[org.apache.celeborn.common.protocol.PartitionType.MAP]]
 */
class MapPartitionCommitHandler(
    appId: String,
    conf: CelebornConf,
    shuffleAllocatedWorkers: ShuffleAllocatedWorkers,
    committedPartitionInfo: CommittedPartitionInfo,
    workerStatusTracker: WorkerStatusTracker,
    sharedRpcPool: ThreadPoolExecutor,
    commitRetryScheduler: ScheduledExecutorService)
  extends CommitHandler(
    appId,
    conf,
    committedPartitionInfo,
    workerStatusTracker,
    sharedRpcPool,
    commitRetryScheduler)
  with Logging {

  private val shuffleSucceedPartitionIds = JavaUtils.newConcurrentHashMap[Int, util.Set[Integer]]()

  // shuffleId -> in processing partitionId set
  private val inProcessMapPartitionEndIds = JavaUtils.newConcurrentHashMap[Int, util.Set[Integer]]()

  // shuffleId -> boolean, records whether the shuffle is visible at the segment level, facilitating future optimization of worker read and write processes
  private val shuffleIsSegmentGranularityVisible = JavaUtils.newConcurrentHashMap[Int, Boolean]

  private val shuffleIntegrityCheckEnabled = conf.clientShuffleIntegrityCheckEnabled

  // Write-side per-subpartition checksums of one finished map partition (indexed by subpartition).
  private case class MapPartitionWriteMetadata(crc32: Array[Int], bytesWritten: Array[Long]) {
    require(
      crc32.length == bytesWritten.length,
      s"crc32 length ${crc32.length} != bytesWritten length ${bytesWritten.length}")
  }

  // shuffleId -> (mapPartitionId -> write-side metadata).
  private val commitMetadataForMapPartition =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Int, MapPartitionWriteMetadata]]()

  override def getPartitionType(): PartitionType = {
    PartitionType.MAP
  }

  override def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = {
    inProcessMapPartitionEndIds.containsKey(shuffleId) && inProcessMapPartitionEndIds.get(
      shuffleId).contains(partitionId)
  }

  override def getUnhandledPartitionLocations(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo): mutable.Set[PartitionLocation] = {
    shuffleCommittedInfo.unhandledPartitionLocations.asScala.filterNot { partitionLocation =>
      shuffleCommittedInfo.handledPartitionLocations.contains(partitionLocation) ||
      isPartitionInProcess(shuffleId, partitionLocation.getId)
    }
  }

  override def incrementInFlightNum(
      shuffleCommittedInfo: ShuffleCommittedInfo,
      workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]]): Unit = {
    workerToRequests.foreach {
      case (_, partitions) =>
        partitions.groupBy(_.getId).foreach { case (id, _) =>
          val atomicInteger = shuffleCommittedInfo.partitionInFlightCommitRequestNum
            .computeIfAbsent(id, (_: Int) => new AtomicInteger(0))
          atomicInteger.incrementAndGet()
        }
    }
  }

  override def decrementInFlightNum(
      shuffleCommittedInfo: ShuffleCommittedInfo,
      workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]]): Unit = {
    workerToRequests.foreach {
      case (_, partitions) =>
        partitions.groupBy(_.getId).foreach { case (id, _) =>
          shuffleCommittedInfo.partitionInFlightCommitRequestNum.get(id).decrementAndGet()
        }
    }
  }

  override def getMapperAttempts(shuffleId: Int): Array[Int] = {
    // map partition now return empty mapper attempts array as map partition don't prevent other mapper commit file
    // even the same mapper id with another attemptId success in lifecycle manager.
    Array.empty
  }

  override def areAllMapperAttemptsFinished(shuffleId: Int): Boolean = {
    // see getMapperAttempts. !getMapperAttempts.exists(_ < -1) is always true
    true
  }

  override def removeExpiredShuffle(shuffleId: Int): Unit = {
    inProcessMapPartitionEndIds.remove(shuffleId)
    shuffleSucceedPartitionIds.remove(shuffleId)
    shuffleIsSegmentGranularityVisible.remove(shuffleId)
    commitMetadataForMapPartition.remove(shuffleId)
    super.removeExpiredShuffle(shuffleId)
  }

  private def handleFinalPartitionCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[String, ShufflePartitionLocationInfo],
      partitionId: Int): (Boolean, ShuffleFailedWorkers) = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
    // commit files
    val parallelCommitResult = parallelCommitFiles(shuffleId, allocatedWorkers, Some(partitionId))

    // check map partition inflight request complete
    waitInflightRequestComplete(shuffleCommittedInfo, partitionId)

    // check partition data lost
    val failedPrimaryPartitionUniqueIds =
      getPartitionIds(shuffleCommittedInfo.failedPrimaryPartitionIds, partitionId)
    val failedReplicaPartitionUniqueIds =
      getPartitionIds(shuffleCommittedInfo.failedReplicaPartitionIds, partitionId)
    val dataLost =
      checkDataLost(shuffleId, failedPrimaryPartitionUniqueIds, failedReplicaPartitionUniqueIds)

    // collect partition result
    if (!dataLost) {
      collectResult(
        shuffleId,
        shuffleCommittedInfo,
        getPartitionUniqueIds(shuffleCommittedInfo.committedPrimaryIds, partitionId),
        getPartitionUniqueIds(shuffleCommittedInfo.committedReplicaIds, partitionId),
        parallelCommitResult.primaryPartitionLocationMap,
        parallelCommitResult.replicaPartitionLocationMap,
        shuffleIsSegmentGranularityVisible.get(shuffleId))
    }

    (dataLost, parallelCommitResult.commitFilesFailedWorkers)
  }

  private def waitInflightRequestComplete(
      shuffleCommittedInfo: ShuffleCommittedInfo,
      partitionId: Int): Unit = {
    if (shuffleCommittedInfo.partitionInFlightCommitRequestNum.containsKey(partitionId)) {
      while (shuffleCommittedInfo.partitionInFlightCommitRequestNum.get(
          partitionId).get() > 0) {
        Thread.sleep(1000)
      }
    }
  }

  private def getPartitionIds(
      partitionIds: ConcurrentHashMap[String, WorkerInfo],
      partitionId: Int): util.Map[String, WorkerInfo] = {
    partitionIds.asScala.filter(p =>
      Utils.splitPartitionLocationUniqueId(p._1)._1 == partitionId).asJava
  }

  private def getPartitionUniqueIds(
      ids: ConcurrentHashMap[Int, util.List[String]],
      partitionId: Int): util.Iterator[String] = {
    ids.getOrDefault(partitionId, Collections.emptyList[String]).iterator()
  }

  override def finishMapperAttempt(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      partitionId: Int,
      pushFailedBatches: util.Map[String, LocationPushFailedBatches],
      recordWorkerFailure: ShuffleFailedWorkers => Unit,
      numPartitions: Int,
      crc32PerPartition: Array[Int],
      bytesWrittenPerPartition: Array[Long]): (Boolean, Boolean) = {
    val inProcessingPartitionIds =
      inProcessMapPartitionEndIds.computeIfAbsent(
        shuffleId,
        (k: Int) => ConcurrentHashMap.newKeySet[Integer]())
    inProcessingPartitionIds.add(partitionId)

    val partitionAllocatedWorkers = shuffleAllocatedWorkers.get(shuffleId).asScala.filter(p =>
      p._2.containsPartition(partitionId)).asJava

    var dataCommitSuccess = true
    if (!partitionAllocatedWorkers.isEmpty) {
      val (dataLost, commitFailedWorkers) =
        handleFinalPartitionCommitFiles(
          shuffleId,
          partitionAllocatedWorkers,
          partitionId)
      dataCommitSuccess = !dataLost
      recordWorkerFailure(commitFailedWorkers)
    }

    if (dataCommitSuccess) {
      val resultPartitions =
        shuffleSucceedPartitionIds.computeIfAbsent(
          shuffleId,
          (k: Int) => ConcurrentHashMap.newKeySet[Integer]())
      resultPartitions.add(partitionId)

      if (shuffleIntegrityCheckEnabled) {
        recordMapPartitionCommitMetadata(
          shuffleId,
          partitionId,
          numPartitions,
          crc32PerPartition,
          bytesWrittenPerPartition)
      }
    }

    (dataCommitSuccess, false)
  }

  /**
   * Records a finished mapper's per-subpartition write-side checksums, keyed by (shuffleId,
   * mapPartitionId), for later validation in [[finishPartition]]. A retried/duplicate attempt
   * overwrites the prior record (last write wins), which is intentional: the reader validates
   * against the last committed attempt. Visible for testing.
   */
  private[commit] def recordMapPartitionCommitMetadata(
      shuffleId: Int,
      mapPartitionId: Int,
      numPartitions: Int,
      crc32PerPartition: Array[Int],
      bytesWrittenPerPartition: Array[Long]): Unit = {
    if (crc32PerPartition == null || crc32PerPartition.length != numPartitions ||
      bytesWrittenPerPartition == null || bytesWrittenPerPartition.length != numPartitions) {
      logWarning(
        s"Skip recording commit metadata for shuffle $shuffleId map partition $mapPartitionId " +
          s"because reported checksum arrays do not match numPartitions $numPartitions.")
      return
    }
    commitMetadataForMapPartition
      .computeIfAbsent(
        shuffleId,
        (_: Int) => JavaUtils.newConcurrentHashMap[Int, MapPartitionWriteMetadata]())
      .put(mapPartitionId, MapPartitionWriteMetadata(crc32PerPartition, bytesWrittenPerPartition))
  }

  override def registerShuffle(
      shuffleId: Int,
      numMappers: Int,
      isSegmentGranularityVisible: Boolean,
      numPartitions: Int): Unit = {
    super.registerShuffle(shuffleId, numMappers, isSegmentGranularityVisible, numPartitions)
    shuffleIsSegmentGranularityVisible.put(shuffleId, isSegmentGranularityVisible)
    // The outer metadata map is created lazily by recordMapPartitionCommitMetadata.
  }

  override def isSegmentGranularityVisible(shuffleId: Int): Boolean = {
    shuffleIsSegmentGranularityVisible.get(shuffleId)
  }

  /**
   * Validates a map partition read over the consumed `[startSubIndex, endSubIndex]` range against
   * the order-independent combination of the write-side checksums over that range. The params
   * rename the trait's reducer-oriented signature (partitionId/startMapIndex/endMapIndex) to map
   * semantics. A reader reaches stream end only after commit, so the write-side metadata is already
   * recorded; missing metadata fails closed.
   */
  override def finishPartition(
      shuffleId: Int,
      mapPartitionId: Int,
      startSubIndex: Int,
      endSubIndex: Int,
      actualCommitMetadata: CommitMetadata): (Boolean, String) = {
    if (!shuffleIntegrityCheckEnabled) {
      return (true, "")
    }
    val perMapPartition = commitMetadataForMapPartition.get(shuffleId)
    if (perMapPartition == null) {
      return (
        false,
        s"No write-side commit metadata recorded for shuffle $shuffleId when validating " +
          s"map partition $mapPartitionId subpartitions [$startSubIndex, $endSubIndex].")
    }
    val subPartitionMetadata = perMapPartition.get(mapPartitionId)
    if (subPartitionMetadata == null) {
      return (
        false,
        s"No write-side commit metadata recorded for shuffle $shuffleId map partition " +
          s"$mapPartitionId when validating subpartitions [$startSubIndex, $endSubIndex].")
    }
    val crc32PerSubPartition = subPartitionMetadata.crc32
    val bytesPerSubPartition = subPartitionMetadata.bytesWritten
    if (startSubIndex < 0 || endSubIndex >= crc32PerSubPartition.length ||
      startSubIndex > endSubIndex) {
      return (
        false,
        s"Invalid subpartition range [$startSubIndex, $endSubIndex] for shuffle $shuffleId " +
          s"map partition $mapPartitionId with ${crc32PerSubPartition.length} subpartitions.")
    }
    val expectedCommitMetadata = new CommitMetadata()
    var subIndex = startSubIndex
    while (subIndex <= endSubIndex) {
      expectedCommitMetadata.addCommitData(
        crc32PerSubPartition(subIndex),
        bytesPerSubPartition(subIndex))
      subIndex += 1
    }
    if (CommitMetadata.checkCommitMetadata(expectedCommitMetadata, actualCommitMetadata)) {
      (true, "")
    } else {
      (
        false,
        s"Integrity check failed for shuffle $shuffleId map partition $mapPartitionId " +
          s"subpartitions [$startSubIndex, $endSubIndex], expected $expectedCommitMetadata " +
          s"but read $actualCommitMetadata.")
    }
  }

  override def handleGetReducerFileGroup(
      context: RpcCallContext,
      shuffleId: Int,
      startPartition: Int,
      endPartition: Int,
      hasPartitionRange: Boolean,
      omitMapAttempts: Boolean,
      serdeVersion: SerdeVersion): Unit = {
    // TODO: if support the downstream map task start early before the upstream reduce task, it should
    //  waiting the upstream task register shuffle, then reply these GetReducerFileGroup.
    //  Note that flink hybrid shuffle should support it in the future.

    // we need obtain the last succeed partitionIds
    val lastSucceedPartitionIds =
      shuffleSucceedPartitionIds.getOrDefault(shuffleId, new util.HashSet[Integer]())
    val allFileGroups =
      reducerFileGroupsMap.getOrDefault(shuffleId, JavaUtils.newConcurrentHashMap())
    val fileGroups = ReducerFileGroupFilter.fileGroupsForRange(
      allFileGroups,
      startPartition,
      endPartition,
      hasPartitionRange)
    val succeedPartitionIds = ReducerFileGroupFilter.partitionIdsForRange(
      lastSucceedPartitionIds,
      startPartition,
      endPartition,
      hasPartitionRange)

    context.reply(GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      fileGroups,
      if (omitMapAttempts) Array.emptyIntArray else getMapperAttempts(shuffleId),
      succeedPartitionIds,
      serdeVersion = serdeVersion,
      startPartition = startPartition,
      endPartition = endPartition,
      hasPartitionRange = hasPartitionRange))
  }

  override def releasePartitionResource(shuffleId: Int, partitionId: Int): Unit = {
    val succeedPartitionIds = shuffleSucceedPartitionIds.get(shuffleId)
    if (succeedPartitionIds != null) {
      succeedPartitionIds.remove(partitionId)
    }

    super.releasePartitionResource(shuffleId, partitionId)
  }
}
