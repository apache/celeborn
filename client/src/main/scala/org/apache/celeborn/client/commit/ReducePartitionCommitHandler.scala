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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers, ShuffleFileGroups, ShuffleMapperAttempts}
import org.apache.celeborn.client.ShuffleCommittedInfo
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionType

/**
 * This commit handler is for ReducePartition ShuffleType, which means that a Reduce Partition contains all data
 * produced by all upstream MapTasks, and data in a Reduce Partition would only be consumed by one ReduceTask. If the
 * ReduceTask has multiple inputs, each will be a ReducePartition
 *
 * @see [[org.apache.celeborn.common.protocol.PartitionType.REDUCE]]
 */
class ReducePartitionCommitHandler(
    appId: String,
    conf: CelebornConf,
    allocatedWorkers: ShuffleAllocatedWorkers,
    reducerFileGroupsMap: ShuffleFileGroups,
    committedPartitionInfo: CommittedPartitionInfo,
    shuffleMapperAttempts: ShuffleMapperAttempts)
  extends CommitHandler(appId, conf, allocatedWorkers, reducerFileGroupsMap, committedPartitionInfo)
  with Logging {

  private val dataLostShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val stageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val inProcessStageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()

  override def getPartitionType(): PartitionType = {
    PartitionType.REDUCE
  }

  override def isStageEnd(shuffleId: Int): Boolean = {
    stageEndShuffleSet.contains(shuffleId)
  }

  override def isStageEndOrInProcess(shuffleId: Int): Boolean = {
    if (inProcessStageEndShuffleSet.contains(shuffleId) ||
      stageEndShuffleSet.contains(shuffleId)) {
      true
    } else {
      false
    }
  }

  override def isStageDataLost(shuffleId: Int): Boolean = {
    dataLostShuffleSet.contains(shuffleId)
  }

  override def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = {
    isStageEndOrInProcess(shuffleId)
  }

  override def setStageEnd(shuffleId: Int): Unit = {
    stageEndShuffleSet.add(shuffleId)
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    dataLostShuffleSet.remove(shuffleId)
    stageEndShuffleSet.remove(shuffleId)
    inProcessStageEndShuffleSet.remove(shuffleId)
  }

  override def finalCommit(
      shuffleId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Unit = {
    if (stageEndShuffleSet.contains(shuffleId)) {
      logInfo(s"[handleStageEnd] Shuffle $shuffleId already ended!")
      return
    }
    inProcessStageEndShuffleSet.synchronized {
      if (inProcessStageEndShuffleSet.contains(shuffleId)) {
        logWarning(s"[handleStageEnd] Shuffle $shuffleId is in process!")
        return
      }
      inProcessStageEndShuffleSet.add(shuffleId)
    }
    // ask allLocations workers holding partitions to commit files
    val shuffleAllocatedWorkers = allocatedWorkers.get(shuffleId)
    val (dataLost, commitFailedWorkers) = handleFinalCommitFiles(shuffleId, shuffleAllocatedWorkers)
    recordWorkerFailure(commitFailedWorkers)
    // reply
    if (!dataLost) {
      logInfo(s"Succeed to handle stageEnd for $shuffleId.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
    } else {
      logError(s"Failed to handle stageEnd for $shuffleId, lost file!")
      dataLostShuffleSet.add(shuffleId)
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
    }
    inProcessStageEndShuffleSet.remove(shuffleId)
  }

  private def handleFinalCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[WorkerInfo, PartitionLocationInfo])
      : (Boolean, ShuffleFailedWorkers) = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)

    // commit files
    val parallelCommitResult = parallelCommitFiles(shuffleId, allocatedWorkers, None)

    // check all inflight request complete
    waitInflightRequestComplete(shuffleCommittedInfo)

    // check data lost
    val dataLost = checkDataLost(
      shuffleId,
      shuffleCommittedInfo.failedMasterPartitionIds,
      shuffleCommittedInfo.failedSlavePartitionIds)

    // collect result
    if (!dataLost) {
      collectResult(
        shuffleId,
        shuffleCommittedInfo,
        getPartitionUniqueIds(shuffleCommittedInfo.committedMasterIds),
        getPartitionUniqueIds(shuffleCommittedInfo.committedSlaveIds),
        parallelCommitResult.masterPartitionLocationMap,
        parallelCommitResult.slavePartitionLocationMap)
    }

    (dataLost, parallelCommitResult.commitFilesFailedWorkers)
  }

  override def finalPartitionCommit(
      shuffleId: Int,
      partitionId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Boolean = {
    throw new UnsupportedOperationException(
      s"Failed when do final Partition Commit Operation, Reduce Partition " +
        s"shuffleType only Support final commit for all partitions ")
  }

  override def getShuffleMapperAttempts(shuffleId: Int): Array[Int] = {
    shuffleMapperAttempts.get(shuffleId)
  }

  def waitInflightRequestComplete(shuffleCommittedInfo: ShuffleCommittedInfo): Unit = {
    while (shuffleCommittedInfo.allInFlightCommitRequestNum.get() > 0) {
      Thread.sleep(1000)
    }
  }

  def getPartitionUniqueIds(ids: ConcurrentHashMap[Int, util.List[String]])
      : util.Iterator[String] = {
    ids.asScala.flatMap(_._2.asScala).toIterator.asJava
  }
}
