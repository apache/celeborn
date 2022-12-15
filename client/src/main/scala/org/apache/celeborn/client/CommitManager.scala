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
import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.client.commit.{CommitHandler, MapPartitionCommitHandler, ReducePartitionCommitHandler}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType, StorageInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.ThreadUtils

case class CommitPartitionRequest(
    applicationId: String,
    shuffleId: Int,
    partition: PartitionLocation)

case class ShuffleCommittedInfo(
    committedMasterIds: ConcurrentHashMap[Int, util.List[String]],
    committedSlaveIds: ConcurrentHashMap[Int, util.List[String]],
    failedMasterPartitionIds: ConcurrentHashMap[String, WorkerInfo],
    failedSlavePartitionIds: ConcurrentHashMap[String, WorkerInfo],
    committedMasterStorageInfos: ConcurrentHashMap[String, StorageInfo],
    committedSlaveStorageInfos: ConcurrentHashMap[String, StorageInfo],
    committedMapIdBitmap: ConcurrentHashMap[String, RoaringBitmap],
    currentShuffleFileCount: LongAdder,
    commitPartitionRequests: util.Set[CommitPartitionRequest],
    handledCommitPartitionRequests: util.Set[PartitionLocation],
    allInFlightCommitRequestNum: AtomicInteger,
    partitionInFlightCommitRequestNum: ConcurrentHashMap[Int, AtomicInteger])

object CommitManager {
  type CommittedPartitionInfo = ConcurrentHashMap[Int, ShuffleCommittedInfo]
}

class CommitManager(appId: String, val conf: CelebornConf, lifecycleManager: LifecycleManager)
  extends Logging {

  // shuffle id -> ShuffleCommittedInfo
  private val committedPartitionInfo = new CommittedPartitionInfo

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
  private val commitHandlers = new ConcurrentHashMap[PartitionType, CommitHandler]()

  def start(): Unit = {
    batchHandleCommitPartition = batchHandleCommitPartitionSchedulerThread.map {
      _.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit = {
            committedPartitionInfo.asScala.foreach { case (shuffleId, shuffleCommittedInfo) =>
              batchHandleCommitPartitionExecutors.submit {
                new Runnable {
                  val partitionType = lifecycleManager.getPartitionType(shuffleId)
                  val commitHandler = getCommitHandler(shuffleId)
                  def incrementInflightNum(workerToRequests: Map[
                    WorkerInfo,
                    collection.Set[PartitionLocation]]): Unit = {
                    if (partitionType == PartitionType.MAP) {
                      workerToRequests.foreach {
                        case (_, partitions) =>
                          partitions.groupBy(_.getId).foreach { case (id, _) =>
                            val atomicInteger = shuffleCommittedInfo
                              .partitionInFlightCommitRequestNum
                              .computeIfAbsent(id, (k: Int) => new AtomicInteger(0))
                            atomicInteger.incrementAndGet()
                          }
                      }
                    }
                    shuffleCommittedInfo.allInFlightCommitRequestNum.addAndGet(
                      workerToRequests.size)
                  }

                  def decrementInflightNum(
                      workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]])
                      : Unit = {
                    if (partitionType == PartitionType.MAP) {
                      workerToRequests.foreach {
                        case (_, partitions) =>
                          partitions.groupBy(_.getId).foreach { case (id, _) =>
                            shuffleCommittedInfo.partitionInFlightCommitRequestNum.get(
                              id).decrementAndGet()
                          }
                      }
                    }
                    shuffleCommittedInfo.allInFlightCommitRequestNum.addAndGet(
                      -workerToRequests.size)
                  }

                  def getUnCommitPartitionRequests(
                      commitPartitionRequests: util.Set[CommitPartitionRequest])
                      : scala.collection.mutable.Set[CommitPartitionRequest] = {
                    if (partitionType == PartitionType.MAP) {
                      commitPartitionRequests.asScala.filterNot { request =>
                        shuffleCommittedInfo.handledCommitPartitionRequests
                          .contains(request.partition) && commitHandler.isPartitionInProcess(
                          shuffleId,
                          request.partition.getId)
                      }
                    } else {
                      commitPartitionRequests.asScala.filterNot { request =>
                        shuffleCommittedInfo.handledCommitPartitionRequests
                          .contains(request.partition)
                      }
                    }
                  }

                  override def run(): Unit = {
                    val workerToRequests = shuffleCommittedInfo.synchronized {
                      // When running to here, if handleStageEnd got lock first and commitFiles,
                      // then this batch get this lock, commitPartitionRequests may contains
                      // partitions which are already committed by stageEnd process.
                      // But inProcessStageEndShuffleSet should have contain this shuffle id,
                      // can directly return.
                      if (commitHandler.isStageEndOrInProcess(shuffleId)) {
                        logWarning(s"Shuffle $shuffleId ended or during processing stage end.")
                        shuffleCommittedInfo.commitPartitionRequests.clear()
                        Map.empty[WorkerInfo, Set[PartitionLocation]]
                      } else {
                        val currentBatch =
                          getUnCommitPartitionRequests(shuffleCommittedInfo.commitPartitionRequests)
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
                          incrementInflightNum(workerToRequests)
                          workerToRequests
                        } else {
                          Map.empty[WorkerInfo, Set[PartitionLocation]]
                        }
                      }
                    }
                    if (workerToRequests.nonEmpty) {
                      val commitFilesFailedWorkers = new ShuffleFailedWorkers()
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

                            commitHandler.commitFiles(
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
                        decrementInflightNum(workerToRequests)
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
        new ConcurrentHashMap[Int, util.List[String]](),
        new ConcurrentHashMap[Int, util.List[String]](),
        new ConcurrentHashMap[String, WorkerInfo](),
        new ConcurrentHashMap[String, WorkerInfo](),
        new ConcurrentHashMap[String, StorageInfo](),
        new ConcurrentHashMap[String, StorageInfo](),
        new ConcurrentHashMap[String, RoaringBitmap](),
        new LongAdder,
        new util.HashSet[CommitPartitionRequest](),
        new util.HashSet[PartitionLocation](),
        new AtomicInteger(),
        new ConcurrentHashMap[Int, AtomicInteger]()))
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    committedPartitionInfo.remove(shuffleId)
    getCommitHandler(shuffleId).removeExpiredShuffle(shuffleId)
  }

  def registerCommitPartitionRequest(
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

  def tryFinalCommit(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).tryFinalCommit(
      shuffleId,
      r => lifecycleManager.recordWorkerFailure(r))
  }

  def finalPartitionCommit(shuffleId: Int, partitionId: Int): Boolean = {
    getCommitHandler(shuffleId).finalPartitionCommit(
      shuffleId,
      partitionId,
      r => lifecycleManager.recordWorkerFailure(r))
  }

  def isStageEnd(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).isStageEnd(shuffleId)
  }

  def setStageEnd(shuffleId: Int): Unit = {
    getCommitHandler(shuffleId).setStageEnd(shuffleId)
  }

  def isStageDataLost(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).isStageDataLost(shuffleId)
  }

  private def getCommitHandler(shuffleId: Int): CommitHandler = {
    val partitionType = lifecycleManager.getPartitionType(shuffleId)
    if (commitHandlers.containsKey(partitionType)) {
      commitHandlers.get(partitionType)
    } else {
      commitHandlers.computeIfAbsent(
        partitionType,
        (partitionType: PartitionType) => {
          partitionType match {
            case PartitionType.REDUCE => new ReducePartitionCommitHandler(
                appId,
                conf,
                lifecycleManager.shuffleAllocatedWorkers,
                lifecycleManager.reducerFileGroupsMap,
                committedPartitionInfo,
                lifecycleManager.shuffleMapperAttempts)
            case PartitionType.MAP => new MapPartitionCommitHandler(
                appId,
                conf,
                lifecycleManager
                  .shuffleAllocatedWorkers,
                lifecycleManager.reducerFileGroupsMap,
                committedPartitionInfo)
            case _ => throw new UnsupportedOperationException(
                s"Unexpected ShufflePartitionType for CommitManager: $partitionType")
          }
        })
    }
  }

  def commitMetrics(): (Long, Long) = {
    var totalWritten = 0L
    var totalFileCount = 0L
    commitHandlers.asScala.values.foreach { commitHandler =>
      totalWritten += commitHandler.commitMetrics._1
      totalFileCount += commitHandler.commitMetrics._2
    }
    (totalWritten, totalFileCount)
  }
}
