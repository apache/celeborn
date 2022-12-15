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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers, ShuffleFileGroups}
import org.apache.celeborn.client.ShuffleCommittedInfo
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionType
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.Utils

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
    allocatedWorkers: ShuffleAllocatedWorkers,
    reducerFileGroupsMap: ShuffleFileGroups,
    committedPartitionInfo: CommittedPartitionInfo)
  extends CommitHandler(appId, conf, allocatedWorkers, reducerFileGroupsMap, committedPartitionInfo)
  with Logging {

  // shuffleId -> in processing partitionId set
  private val inProcessMapPartitionEndIds = new ConcurrentHashMap[Int, util.Set[Int]]()

  override def getPartitionType(): PartitionType = {
    PartitionType.MAP
  }

  override def setStageEnd(shuffleId: Int): Unit = {
    throw new UnsupportedOperationException(
      "Failed when do setStageEnd Operation, MapPartition shuffleType don't " +
        "support set stage end")
  }

  override def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = {
    inProcessMapPartitionEndIds.containsKey(shuffleId) && inProcessMapPartitionEndIds.get(
      shuffleId).contains(partitionId)
  }

  override def tryFinalCommit(
      shuffleId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Boolean = {
    throw new UnsupportedOperationException(
      "Failed when do final Commit Operation, MapPartition shuffleType only " +
        "support final partition Commit")
  }

  override def finalPartitionCommit(
      shuffleId: Int,
      partitionId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Boolean = {
    val inProcessingPartitionIds =
      inProcessMapPartitionEndIds.computeIfAbsent(shuffleId, (k: Int) => new util.HashSet[Int]())
    inProcessingPartitionIds.add(partitionId)

    val partitionAllocatedWorkers = allocatedWorkers.get(shuffleId).asScala.filter(p =>
      p._2.containsPartition(shuffleId.toString, partitionId)).asJava

    var dataCommitSuccess = true
    if (!partitionAllocatedWorkers.isEmpty) {
      val result =
        handleFinalPartitionCommitFiles(
          shuffleId,
          partitionAllocatedWorkers,
          partitionId)
      dataCommitSuccess = result._1
      recordWorkerFailure(result._2)
    }

    // release resources and clear related info
    partitionAllocatedWorkers.asScala.foreach { case (_, partitionLocationInfo) =>
      partitionLocationInfo.removeRelatedPartitions(shuffleId.toString, partitionId)
    }

    inProcessingPartitionIds.remove(partitionId)
    dataCommitSuccess
  }

  override def getShuffleMapperAttempts(shuffleId: Int): Array[Int] = {
    // map partition now return empty mapper attempts array as map partition don't prevent other mapper commit file
    // even the same mapper id with another attemptId success in lifecycle manager.
    Array.empty
  }

  override def removeExpiredShuffle(shuffleId: Int): Unit = {
    inProcessMapPartitionEndIds.remove(shuffleId)
  }

  private def handleFinalPartitionCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[WorkerInfo, PartitionLocationInfo],
      partitionId: Int): (Boolean, ShuffleFailedWorkers) = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
    // commit files
    val parallelCommitResult = parallelCommitFiles(shuffleId, allocatedWorkers, Some(partitionId))

    // check map partition inflight request complete
    waitInflightRequestComplete(shuffleCommittedInfo, partitionId)

    // check partition data lost
    val failedMasterPartitionUniqueIds =
      getPartitionIds(shuffleCommittedInfo.failedMasterPartitionIds, partitionId)
    val failedSlavePartitionUniqueIds =
      getPartitionIds(shuffleCommittedInfo.failedSlavePartitionIds, partitionId)
    val dataLost =
      checkDataLost(shuffleId, failedMasterPartitionUniqueIds, failedSlavePartitionUniqueIds)

    // collect partition result
    if (!dataLost) {
      collectResult(
        shuffleId,
        shuffleCommittedInfo,
        getPartitionUniqueIds(shuffleCommittedInfo.committedMasterIds, partitionId),
        getPartitionUniqueIds(shuffleCommittedInfo.committedSlaveIds, partitionId),
        parallelCommitResult.masterPartitionLocationMap,
        parallelCommitResult.slavePartitionLocationMap)
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
      Utils.splitPartitionLocationUniqueId(p._1)._1 ==
        partitionId).asJava
  }

  private def getPartitionUniqueIds(
      ids: ConcurrentHashMap[Int, util.List[String]],
      partitionId: Int): util.Iterator[String] = {
    ids.getOrDefault(partitionId, Collections.emptyList[String]).iterator()
  }
}
