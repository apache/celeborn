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
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.collection.JavaConverters._

import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFileGroups, ShuffleMapperAttempts}
import org.apache.celeborn.client.ShuffleCommittedInfo
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.{CommitFiles, CommitFilesResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

case class ParallelCommitResult(
    masterPartitionLocationMap: ConcurrentHashMap[String, PartitionLocation],
    slavePartitionLocationMap: ConcurrentHashMap[String, PartitionLocation],
    commitFilesFailedWorkers: ConcurrentHashMap[WorkerInfo, (StatusCode, Long)])

abstract class CommitHandler(
    appId: String,
    conf: CelebornConf,
    allocatedWorkers: ShuffleAllocatedWorkers,
    reducerFileGroupsMap: ShuffleFileGroups,
    committedPartitionInfo: CommittedPartitionInfo) extends Logging {

  private val pushReplicateEnabled = conf.pushReplicateEnabled
  private val testRetryCommitFiles = conf.testRetryCommitFiles
  private val commitEpoch = new AtomicLong()
  private val totalWritten = new LongAdder
  private val fileCount = new LongAdder

  def getPartitionType(): PartitionType

  def isStageEnd(shuffleId: Int): Boolean = false

  def isStageEndOrInProcess(shuffleId: Int): Boolean = false

  def isStageDataLost(shuffleId: Int): Boolean = false

  def setStageEnd(shuffleId: Int): Unit

  def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = false

  def finalCommit(
      shuffleId: Int,
      recordWorkerFailure: ConcurrentHashMap[WorkerInfo, (StatusCode, Long)] => Unit): Unit

  def finalPartitionCommit(
      shuffleId: Int,
      partitionId: Int,
      recordWorkerFailure: ConcurrentHashMap[WorkerInfo, (StatusCode, Long)] => Unit): Boolean

  def removeExpiredShuffle(shuffleId: Int): Unit

  def getShuffleMapperAttempts(shuffleId: Int): Array[Int]

  def parallelCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[WorkerInfo, PartitionLocationInfo],
      partitionIdOpt: Option[Int] = None): ParallelCommitResult = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
    val masterPartMap = new ConcurrentHashMap[String, PartitionLocation]
    val slavePartMap = new ConcurrentHashMap[String, PartitionLocation]
    val commitFilesFailedWorkers = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
    val commitFileStartTime = System.nanoTime()
    val parallelism = Math.min(allocatedWorkers.size(), conf.rpcMaxParallelism)
    ThreadUtils.parmap(
      allocatedWorkers.asScala.to,
      "CommitFiles",
      parallelism) { case (worker, partitionLocationInfo) =>
      if (partitionLocationInfo.containsShuffle(shuffleId.toString)) {
        val masterParts =
          partitionLocationInfo.getMasterLocations(shuffleId.toString, partitionIdOpt)
        val slaveParts = partitionLocationInfo.getSlaveLocations(shuffleId.toString, partitionIdOpt)
        masterParts.asScala.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setFetchPort(worker.fetchPort)
          partition.setPeer(null)
          masterPartMap.put(partition.getUniqueId, partition)
        }
        slaveParts.asScala.foreach { p =>
          val partition = new PartitionLocation(p)
          partition.setFetchPort(worker.fetchPort)
          partition.setPeer(null)
          slavePartMap.put(partition.getUniqueId, partition)
        }

        val (masterIds, slaveIds) = shuffleCommittedInfo.synchronized {
          (
            masterParts.asScala
              .filterNot(shuffleCommittedInfo.handledCommitPartitionRequests.contains)
              .map(_.getUniqueId).asJava,
            slaveParts.asScala
              .filterNot(shuffleCommittedInfo.handledCommitPartitionRequests.contains)
              .map(_.getUniqueId).asJava)
        }

        commitFiles(
          appId,
          shuffleId,
          shuffleCommittedInfo,
          worker,
          masterIds,
          slaveIds,
          commitFilesFailedWorkers)
      }
    }

    logInfo(s"Shuffle $shuffleId " +
      s"commit files complete. File count ${shuffleCommittedInfo.currentShuffleFileCount.sum()} " +
      s"using ${(System.nanoTime() - commitFileStartTime) / 1000000} ms")

    ParallelCommitResult(masterPartMap, slavePartMap, commitFilesFailedWorkers)
  }

  def commitFiles(
      applicationId: String,
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo,
      worker: WorkerInfo,
      masterIds: util.List[String],
      slaveIds: util.List[String],
      commitFilesFailedWorkers: ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]): Unit = {

    val res =
      if (!testRetryCommitFiles) {
        val commitFiles = CommitFiles(
          applicationId,
          shuffleId,
          masterIds,
          slaveIds,
          getShuffleMapperAttempts(shuffleId),
          commitEpoch.incrementAndGet())
        val res = requestCommitFilesWithRetry(worker.endpoint, commitFiles)

        res.status match {
          case StatusCode.SUCCESS => // do nothing
          case StatusCode.PARTIAL_SUCCESS | StatusCode.SHUFFLE_NOT_REGISTERED | StatusCode.FAILED =>
            logDebug(s"Request $commitFiles return ${res.status} for " +
              s"${Utils.makeShuffleKey(applicationId, shuffleId)}")
            commitFilesFailedWorkers.put(worker, (res.status, System.currentTimeMillis()))
          case _ => // won't happen
        }
        res
      } else {
        // for test
        val commitFiles1 = CommitFiles(
          applicationId,
          shuffleId,
          masterIds.subList(0, masterIds.size() / 2),
          slaveIds.subList(0, slaveIds.size() / 2),
          getShuffleMapperAttempts(shuffleId),
          commitEpoch.incrementAndGet())
        val res1 = requestCommitFilesWithRetry(worker.endpoint, commitFiles1)

        val commitFiles = CommitFiles(
          applicationId,
          shuffleId,
          masterIds.subList(masterIds.size() / 2, masterIds.size()),
          slaveIds.subList(slaveIds.size() / 2, slaveIds.size()),
          getShuffleMapperAttempts(shuffleId),
          commitEpoch.incrementAndGet())
        val res2 = requestCommitFilesWithRetry(worker.endpoint, commitFiles)

        res1.committedMasterStorageInfos.putAll(res2.committedMasterStorageInfos)
        res1.committedSlaveStorageInfos.putAll(res2.committedSlaveStorageInfos)
        res1.committedMapIdBitMap.putAll(res2.committedMapIdBitMap)
        CommitFilesResponse(
          status = if (res1.status == StatusCode.SUCCESS) res2.status else res1.status,
          (res1.committedMasterIds.asScala ++ res2.committedMasterIds.asScala).toList.asJava,
          (res1.committedSlaveIds.asScala ++ res1.committedSlaveIds.asScala).toList.asJava,
          (res1.failedMasterIds.asScala ++ res1.failedMasterIds.asScala).toList.asJava,
          (res1.failedSlaveIds.asScala ++ res2.failedSlaveIds.asScala).toList.asJava,
          res1.committedMasterStorageInfos,
          res1.committedSlaveStorageInfos,
          res1.committedMapIdBitMap,
          res1.totalWritten + res2.totalWritten,
          res1.fileCount + res2.fileCount)
      }

    shuffleCommittedInfo.synchronized {
      // record committed partitionIds
      res.committedMasterIds.asScala.foreach({
        case commitMasterId =>
          val partitionUniqueIdList = shuffleCommittedInfo.committedMasterIds.computeIfAbsent(
            Utils.splitPartitionLocationUniqueId(commitMasterId)._1,
            (k: Int) => new util.ArrayList[String]())
          partitionUniqueIdList.add(commitMasterId)
      })

      res.committedSlaveIds.asScala.foreach({
        case commitSlaveId =>
          val partitionUniqueIdList = shuffleCommittedInfo.committedSlaveIds.computeIfAbsent(
            Utils.splitPartitionLocationUniqueId(commitSlaveId)._1,
            (k: Int) => new util.ArrayList[String]())
          partitionUniqueIdList.add(commitSlaveId)
      })

      // record committed partitions storage hint and disk hint
      shuffleCommittedInfo.committedMasterStorageInfos.putAll(res.committedMasterStorageInfos)
      shuffleCommittedInfo.committedSlaveStorageInfos.putAll(res.committedSlaveStorageInfos)

      // record failed partitions
      shuffleCommittedInfo.failedMasterPartitionIds.putAll(
        res.failedMasterIds.asScala.map((_, worker)).toMap.asJava)
      shuffleCommittedInfo.failedSlavePartitionIds.putAll(
        res.failedSlaveIds.asScala.map((_, worker)).toMap.asJava)

      shuffleCommittedInfo.committedMapIdBitmap.putAll(res.committedMapIdBitMap)

      totalWritten.add(res.totalWritten)
      fileCount.add(res.fileCount)
      shuffleCommittedInfo.currentShuffleFileCount.add(res.fileCount)
    }
  }

  def collectResult(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo,
      masterPartitionUniqueIds: util.List[String],
      slavePartitionUniqueIds: util.List[String],
      masterPartMap: ConcurrentHashMap[String, PartitionLocation],
      slavePartMap: ConcurrentHashMap[String, PartitionLocation]): Unit = {
    val committedPartitions = new util.HashMap[String, PartitionLocation]
    masterPartitionUniqueIds.asScala.foreach { id =>
      if (shuffleCommittedInfo.committedMasterStorageInfos.get(id) == null) {
        logDebug(s"$appId-$shuffleId $id storage hint was not returned")
      } else {
        masterPartMap.get(id).setStorageInfo(
          shuffleCommittedInfo.committedMasterStorageInfos.get(id))
        masterPartMap.get(id).setMapIdBitMap(shuffleCommittedInfo.committedMapIdBitmap.get(id))
        committedPartitions.put(id, masterPartMap.get(id))
      }
    }

    slavePartitionUniqueIds.asScala.foreach { id =>
      val slavePartition = slavePartMap.get(id)
      if (shuffleCommittedInfo.committedSlaveStorageInfos.get(id) == null) {
        logDebug(s"$appId-$shuffleId $id storage hint was not returned")
      } else {
        slavePartition.setStorageInfo(shuffleCommittedInfo.committedSlaveStorageInfos.get(id))
        val masterPartition = committedPartitions.get(id)
        if (masterPartition ne null) {
          masterPartition.setPeer(slavePartition)
          slavePartition.setPeer(masterPartition)
        } else {
          logInfo(s"Shuffle $shuffleId partition $id: master lost, " +
            s"use slave $slavePartition.")
          slavePartition.setMapIdBitMap(shuffleCommittedInfo.committedMapIdBitmap.get(id))
          committedPartitions.put(id, slavePartition)
        }
      }
    }

    committedPartitions.values().asScala.foreach { partition =>
      val partitionLocations = reducerFileGroupsMap.get(shuffleId).computeIfAbsent(
        partition.getId,
        (k: Integer) => new util.HashSet[PartitionLocation]())
      partitionLocations.add(partition)
    }
  }

  private def requestCommitFilesWithRetry(
      endpoint: RpcEndpointRef,
      message: CommitFiles): CommitFilesResponse = {
    val maxRetries = conf.requestCommitFilesMaxRetries
    var retryTimes = 0
    while (retryTimes < maxRetries) {
      try {
        if (testRetryCommitFiles && retryTimes < maxRetries - 1) {
          endpoint.ask[CommitFilesResponse](message)
          Thread.sleep(1000)
          throw new Exception("Mock fail for CommitFiles")
        } else {
          return endpoint.askSync[CommitFilesResponse](message)
        }
      } catch {
        case e: Throwable =>
          retryTimes += 1
          logError(
            s"AskSync CommitFiles for ${message.shuffleId} failed (attempt $retryTimes/$maxRetries).",
            e)
      }
    }

    CommitFilesResponse(
      StatusCode.FAILED,
      List.empty.asJava,
      List.empty.asJava,
      message.masterIds,
      message.slaveIds)
  }

  def checkDataLost(
      shuffleId: Int,
      masterPartitionUniqueIdMap: util.Map[String, WorkerInfo],
      slavePartitionUniqueIdMap: util.Map[String, WorkerInfo]): Boolean = {
    val shuffleKey = Utils.makeShuffleKey(appId, shuffleId)
    if (!pushReplicateEnabled && masterPartitionUniqueIdMap.size() != 0) {
      val msg =
        masterPartitionUniqueIdMap.asScala.map {
          case (partitionUniqueId, workerInfo) =>
            s"Lost partition $partitionUniqueId in worker [${workerInfo.readableAddress()}]"
        }.mkString("\n")
      logError(
        s"""
           |For shuffle $shuffleKey partition data lost:
           |$msg
           |""".stripMargin)
      true
    } else {
      val failedBothPartitionIdsToWorker = masterPartitionUniqueIdMap.asScala.flatMap {
        case (partitionUniqueId, worker) =>
          if (slavePartitionUniqueIdMap.asScala.contains(partitionUniqueId)) {
            Some(partitionUniqueId -> (worker, slavePartitionUniqueIdMap.get(partitionUniqueId)))
          } else {
            None
          }
      }
      if (failedBothPartitionIdsToWorker.nonEmpty) {
        val msg = failedBothPartitionIdsToWorker.map {
          case (partitionUniqueId, (masterWorker, slaveWorker)) =>
            s"Lost partition $partitionUniqueId " +
              s"in master worker [${masterWorker.readableAddress()}] and slave worker [$slaveWorker]"
        }.mkString("\n")
        logError(
          s"""
             |For shuffle $shuffleKey partition data lost:
             |$msg
             |""".stripMargin)
        true
      } else {
        false
      }
    }
  }

  def commitMetrics(): (Long, Long) = (totalWritten.sumThenReset(), fileCount.sumThenReset())
}
