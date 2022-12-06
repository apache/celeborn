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
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, LongAdder}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.{PartitionLocation, StorageInfo}
import org.apache.celeborn.common.protocol.message.ControlMessages.{CommitFiles, CommitFilesResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

case class CommitPartitionRequest(
    applicationId: String,
    shuffleId: Int,
    partition: PartitionLocation)

case class ShuffleCommittedInfo(
    committedMasterIds: util.List[String],
    committedSlaveIds: util.List[String],
    failedMasterPartitionIds: ConcurrentHashMap[String, WorkerInfo],
    failedSlavePartitionIds: ConcurrentHashMap[String, WorkerInfo],
    committedMasterStorageInfos: ConcurrentHashMap[String, StorageInfo],
    committedSlaveStorageInfos: ConcurrentHashMap[String, StorageInfo],
    committedMapIdBitmap: ConcurrentHashMap[String, RoaringBitmap],
    currentShuffleFileCount: LongAdder,
    commitPartitionRequests: util.Set[CommitPartitionRequest],
    handledCommitPartitionRequests: util.Set[PartitionLocation],
    inFlightCommitRequest: AtomicInteger)

class CommitManager(appId: String, val conf: CelebornConf, lifecycleManager: LifecycleManager)
  extends Logging {
  // shuffle id -> ShuffleCommittedInfo
  private val committedPartitionInfo = new ConcurrentHashMap[Int, ShuffleCommittedInfo]()
  val dataLostShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  val stageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val inProcessStageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()

  private val pushReplicateEnabled = conf.pushReplicateEnabled

  private val batchHandleCommitPartitionEnabled = conf.batchHandleCommitPartitionEnabled
  private val batchHandleCommitPartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "rss-lifecycle-manager-commit-partition-executor",
    conf.batchHandleCommitPartitionNumThreads)
  private val batchHandleCommitPartitionRequestInterval =
    conf.batchHandleCommitPartitionRequestInterval
  private val batchHandleCommitPartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleCommitPartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "rss-lifecycle-manager-commit-partition-scheduler"))
    } else {
      None
    }
  private var batchHandleCommitPartition: Option[ScheduledFuture[_]] = _

  private val totalWritten = new LongAdder
  private val fileCount = new LongAdder

  private val testRetryCommitFiles = conf.testRetryCommitFiles
  private val commitEpoch = new AtomicLong()

  def start(): Unit = {
    batchHandleCommitPartition = batchHandleCommitPartitionSchedulerThread.map {
      _.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit = {
            committedPartitionInfo.asScala.foreach { case (shuffleId, shuffleCommittedInfo) =>
              batchHandleCommitPartitionExecutors.submit {
                new Runnable {
                  override def run(): Unit = {
                    val workerToRequests = shuffleCommittedInfo.synchronized {
                      // When running to here, if handleStageEnd got lock first and commitFiles,
                      // then this batch get this lock, commitPartitionRequests may contains
                      // partitions which are already committed by stageEnd process.
                      // But inProcessStageEndShuffleSet should have contain this shuffle id,
                      // can directly return.
                      if (inProcessStageEndShuffleSet.contains(shuffleId) ||
                        stageEndShuffleSet.contains(shuffleId)) {
                        logWarning(s"Shuffle $shuffleId ended or during processing stage end.")
                        shuffleCommittedInfo.commitPartitionRequests.clear()
                        Map.empty[WorkerInfo, Set[PartitionLocation]]
                      } else {
                        val batch = new util.HashSet[CommitPartitionRequest]()
                        batch.addAll(shuffleCommittedInfo.commitPartitionRequests)
                        val currentBatch = batch.asScala.filterNot { request =>
                          shuffleCommittedInfo.handledCommitPartitionRequests
                            .contains(request.partition)
                        }
                        shuffleCommittedInfo.commitPartitionRequests.clear()
                        currentBatch.foreach { commitPartitionRequest =>
                          shuffleCommittedInfo.handledCommitPartitionRequests
                            .add(commitPartitionRequest.partition)
                          if (commitPartitionRequest.partition.getPeer != null) {
                            shuffleCommittedInfo.handledCommitPartitionRequests
                              .add(commitPartitionRequest.partition.getPeer)
                          }
                        }

                        if (currentBatch.nonEmpty) {
                          logWarning(s"Commit current batch HARD_SPLIT partitions for $shuffleId: " +
                            s"${currentBatch.map(_.partition.getUniqueId).mkString("[", ",", "]")}")
                          val workerToRequests = currentBatch.flatMap { request =>
                            if (request.partition.getPeer != null) {
                              Seq(request.partition, request.partition.getPeer)
                            } else {
                              Seq(request.partition)
                            }
                          }.groupBy(_.getWorker)
                          shuffleCommittedInfo.inFlightCommitRequest.addAndGet(
                            workerToRequests.size)
                          workerToRequests
                        } else {
                          Map.empty[WorkerInfo, Set[PartitionLocation]]
                        }
                      }
                    }
                    if (workerToRequests.nonEmpty) {
                      val commitFilesFailedWorkers =
                        new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
                      val parallelism = workerToRequests.size
                      try {
                        ThreadUtils.parmap(
                          workerToRequests.to,
                          "CommitFiles",
                          parallelism) {
                          case (worker, requests) =>
                            val workerInfo =
                              lifecycleManager.shuffleAllocatedWorkers
                                .get(shuffleId)
                                .asScala
                                .find(_._1.equals(worker))
                                .get
                                ._1
                            val mastersIds =
                              requests
                                .filter(_.getMode == PartitionLocation.Mode.MASTER)
                                .map(_.getUniqueId)
                                .toList
                                .asJava
                            val slaveIds =
                              requests
                                .filter(_.getMode == PartitionLocation.Mode.SLAVE)
                                .map(_.getUniqueId)
                                .toList
                                .asJava

                            commitFiles(
                              appId,
                              shuffleId,
                              shuffleCommittedInfo,
                              workerInfo,
                              mastersIds,
                              slaveIds,
                              commitFilesFailedWorkers)
                        }
                        lifecycleManager.recordWorkerFailure(commitFilesFailedWorkers)
                      } finally {
                        shuffleCommittedInfo.inFlightCommitRequest.addAndGet(-workerToRequests.size)
                      }
                    }
                  }
                }
              }
            }
          }
        },
        0,
        batchHandleCommitPartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    batchHandleCommitPartition.foreach(_.cancel(true))
    batchHandleCommitPartitionSchedulerThread.foreach(ThreadUtils.shutdown(_, 800.millis))
  }

  def registerShuffle(shuffleId: Int): Unit = {
    committedPartitionInfo.put(
      shuffleId,
      ShuffleCommittedInfo(
        new util.ArrayList[String](),
        new util.ArrayList[String](),
        new ConcurrentHashMap[String, WorkerInfo](),
        new ConcurrentHashMap[String, WorkerInfo](),
        new ConcurrentHashMap[String, StorageInfo](),
        new ConcurrentHashMap[String, StorageInfo](),
        new ConcurrentHashMap[String, RoaringBitmap](),
        new LongAdder,
        new util.HashSet[CommitPartitionRequest](),
        new util.HashSet[PartitionLocation](),
        new AtomicInteger()))
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    committedPartitionInfo.remove(shuffleId)
    dataLostShuffleSet.remove(shuffleId)
    stageEndShuffleSet.remove(shuffleId)
  }

  def registerCommitPartition(
      applicationId: String,
      shuffleId: Int,
      partition: PartitionLocation,
      cause: Option[StatusCode]): Unit = {
    if (batchHandleCommitPartitionEnabled && cause.isDefined && cause.get == StatusCode.HARD_SPLIT) {
      val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
      shuffleCommittedInfo.synchronized {
        shuffleCommittedInfo.commitPartitionRequests
          .add(CommitPartitionRequest(applicationId, shuffleId, partition))
      }
    }
  }

  private def commitFiles(
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
          lifecycleManager.shuffleMapperAttempts.get(shuffleId),
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
          lifecycleManager.shuffleMapperAttempts.get(shuffleId),
          commitEpoch.incrementAndGet())
        val res1 = requestCommitFilesWithRetry(worker.endpoint, commitFiles1)

        val commitFiles = CommitFiles(
          applicationId,
          shuffleId,
          masterIds.subList(masterIds.size() / 2, masterIds.size()),
          slaveIds.subList(slaveIds.size() / 2, slaveIds.size()),
          lifecycleManager.shuffleMapperAttempts.get(shuffleId),
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
      shuffleCommittedInfo.committedMasterIds.addAll(res.committedMasterIds)
      shuffleCommittedInfo.committedSlaveIds.addAll(res.committedSlaveIds)

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

  def finalCommit(
      applicationId: String,
      shuffleId: Int,
      fileGroups: Array[Array[PartitionLocation]]): Unit = {
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
    val masterPartMap = new ConcurrentHashMap[String, PartitionLocation]
    val slavePartMap = new ConcurrentHashMap[String, PartitionLocation]

    val allocatedWorkers = lifecycleManager.shuffleAllocatedWorkers.get(shuffleId)
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
    val commitFilesFailedWorkers = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
    val commitFileStartTime = System.nanoTime()

    val parallelism = Math.min(allocatedWorkers.size(), conf.rpcMaxParallelism)
    ThreadUtils.parmap(
      allocatedWorkers.asScala.to,
      "CommitFiles",
      parallelism) { case (worker, partitionLocationInfo) =>
      if (partitionLocationInfo.containsShuffle(shuffleId.toString)) {
        val masterParts = partitionLocationInfo.getAllMasterLocations(shuffleId.toString)
        val slaveParts = partitionLocationInfo.getAllSlaveLocations(shuffleId.toString)
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
          applicationId,
          shuffleId,
          shuffleCommittedInfo,
          worker,
          masterIds,
          slaveIds,
          commitFilesFailedWorkers)
      }
    }

    def hasCommitFailedIds: Boolean = {
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      if (!pushReplicateEnabled && shuffleCommittedInfo.failedMasterPartitionIds.size() != 0) {
        val msg =
          shuffleCommittedInfo.failedMasterPartitionIds.asScala.map {
            case (partitionId, workerInfo) =>
              s"Lost partition $partitionId in worker [${workerInfo.readableAddress()}]"
          }.mkString("\n")
        logError(
          s"""
             |For shuffle $shuffleKey partition data lost:
             |$msg
             |""".stripMargin)
        true
      } else {
        val failedBothPartitionIdsToWorker =
          shuffleCommittedInfo.failedMasterPartitionIds.asScala.flatMap {
            case (partitionId, worker) =>
              if (shuffleCommittedInfo.failedSlavePartitionIds.contains(partitionId)) {
                Some(partitionId -> (worker, shuffleCommittedInfo.failedSlavePartitionIds.get(
                  partitionId)))
              } else {
                None
              }
          }
        if (failedBothPartitionIdsToWorker.nonEmpty) {
          val msg = failedBothPartitionIdsToWorker.map {
            case (partitionId, (masterWorker, slaveWorker)) =>
              s"Lost partition $partitionId " +
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

    while (shuffleCommittedInfo.inFlightCommitRequest.get() > 0) {
      Thread.sleep(1000)
    }

    val dataLost = hasCommitFailedIds

    if (!dataLost) {
      val committedPartitions = new util.HashMap[String, PartitionLocation]
      shuffleCommittedInfo.committedMasterIds.asScala.foreach { id =>
        if (shuffleCommittedInfo.committedMasterStorageInfos.get(id) == null) {
          logDebug(s"$applicationId-$shuffleId $id storage hint was not returned")
        } else {
          masterPartMap.get(id).setStorageInfo(
            shuffleCommittedInfo.committedMasterStorageInfos.get(id))
          masterPartMap.get(id).setMapIdBitMap(shuffleCommittedInfo.committedMapIdBitmap.get(id))
          committedPartitions.put(id, masterPartMap.get(id))
        }
      }

      shuffleCommittedInfo.committedSlaveIds.asScala.foreach { id =>
        val slavePartition = slavePartMap.get(id)
        if (shuffleCommittedInfo.committedSlaveStorageInfos.get(id) == null) {
          logDebug(s"$applicationId-$shuffleId $id storage hint was not returned")
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

      val sets = Array.fill(fileGroups.length)(new util.HashSet[PartitionLocation]())
      committedPartitions.values().asScala.foreach { partition =>
        sets(partition.getId).add(partition)
      }
      var i = 0
      while (i < fileGroups.length) {
        fileGroups(i) = sets(i).toArray(new Array[PartitionLocation](0))
        i += 1
      }

      logInfo(s"Shuffle $shuffleId " +
        s"commit files complete. File count ${shuffleCommittedInfo.currentShuffleFileCount.sum()} " +
        s"using ${(System.nanoTime() - commitFileStartTime) / 1000000} ms")
    }

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
    lifecycleManager.recordWorkerFailure(commitFilesFailedWorkers)
  }

  def removeExpiredShuffle(shuffleId: String): Unit = {
    stageEndShuffleSet.remove(shuffleId)
    dataLostShuffleSet.remove(shuffleId)
    committedPartitionInfo.remove(shuffleId)
  }

  def commitMetrics(): (Long, Long) = (totalWritten.sumThenReset(), fileCount.sumThenReset())
}
