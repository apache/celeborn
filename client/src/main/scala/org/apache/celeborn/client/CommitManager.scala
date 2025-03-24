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
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, LongAdder}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleFailedWorkers
import org.apache.celeborn.client.commit.{CommitFilesParam, CommitHandler, MapPartitionCommitHandler, ReducePartitionCommitHandler}
import org.apache.celeborn.client.listener.{WorkersStatus, WorkerStatusListener}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType, StorageInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.common.util.ThreadUtils
import org.apache.celeborn.common.write.PushFailedBatch

case class ShuffleCommittedInfo(
    // partition id -> unique partition ids
    committedPrimaryIds: ConcurrentHashMap[Int, util.List[String]],
    // partition id -> unique partition ids
    committedReplicaIds: ConcurrentHashMap[Int, util.List[String]],
    // unique partition id -> worker info
    failedPrimaryPartitionIds: ConcurrentHashMap[String, WorkerInfo],
    // unique partition id -> worker info
    failedReplicaPartitionIds: ConcurrentHashMap[String, WorkerInfo],
    // unique partition id -> storage info
    committedPrimaryStorageInfos: ConcurrentHashMap[String, StorageInfo],
    // unique partition id -> storage info
    committedReplicaStorageInfos: ConcurrentHashMap[String, StorageInfo],
    // unique partition id -> mapId bitmap
    committedMapIdBitmap: ConcurrentHashMap[String, RoaringBitmap],
    // number of partition files
    currentShuffleFileCount: LongAdder,
    unhandledPartitionLocations: util.Set[PartitionLocation],
    handledPartitionLocations: util.Set[PartitionLocation],
    // for ReducePartition, number of in flight commit requests to worker
    allInFlightCommitRequestNum: AtomicInteger,
    // for MapPartition, partition id -> number of in flight commit requests
    partitionInFlightCommitRequestNum: ConcurrentHashMap[Int, AtomicInteger])

object CommitManager {
  type CommittedPartitionInfo = ConcurrentHashMap[Int, ShuffleCommittedInfo]
}

class CommitManager(appUniqueId: String, val conf: CelebornConf, lifecycleManager: LifecycleManager)
  extends Logging {

  // exposed for test
  // shuffle id -> ShuffleCommittedInfo
  val committedPartitionInfo = new CommittedPartitionInfo
  private val batchHandleCommitPartitionEnabled = conf.batchHandleCommitPartitionEnabled
  private val batchHandleCommitPartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "celeborn-client-lifecycle-manager-commit-partition-executor",
    conf.batchHandleCommitPartitionNumThreads)
  private val batchHandleCommitPartitionRequestInterval =
    conf.batchHandleCommitPartitionRequestInterval
  private val batchHandleCommitPartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleCommitPartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "celeborn-client-lifecycle-manager-commit-partition-scheduler"))
    } else {
      None
    }
  private var batchHandleCommitPartition: Option[ScheduledFuture[_]] = _
  private val commitHandlers = JavaUtils.newConcurrentHashMap[PartitionType, CommitHandler]()

  def start(): Unit = {
    lifecycleManager.registerWorkerStatusListener(new ShutdownWorkerListener)

    batchHandleCommitPartition = batchHandleCommitPartitionSchedulerThread.map {
      _.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            committedPartitionInfo.asScala.foreach { case (shuffleId, shuffleCommittedInfo) =>
              batchHandleCommitPartitionExecutors.submit {
                new Runnable {
                  val commitHandler = getCommitHandler(shuffleId)
                  override def run(): Unit = {
                    var workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]] = null
                    shuffleCommittedInfo.synchronized {
                      workerToRequests =
                        commitHandler.batchUnhandledRequests(shuffleId, shuffleCommittedInfo)
                      // when batch commit thread starts to commit these requests, we should increment inFlightNum,
                      // then stage/partition end would be able to recognize all requests are over.
                      commitHandler.incrementInFlightNum(shuffleCommittedInfo, workerToRequests)
                    }

                    if (workerToRequests.nonEmpty) {
                      val commitFilesFailedWorkers = new ShuffleFailedWorkers()
                      try {
                        val params = new ArrayBuffer[CommitFilesParam](workerToRequests.size)
                        workerToRequests.foreach {
                          case (worker, requests) =>
                            val workerInfo =
                              lifecycleManager.shuffleAllocatedWorkers
                                .get(shuffleId)
                                .get(worker.toUniqueId)
                                .workerInfo
                            val primaryIds =
                              requests
                                .filter(_.getMode == PartitionLocation.Mode.PRIMARY)
                                .map(_.getUniqueId)
                                .toList
                                .asJava
                            val replicaIds =
                              requests
                                .filter(_.getMode == PartitionLocation.Mode.REPLICA)
                                .map(_.getUniqueId)
                                .toList
                                .asJava

                            params += CommitFilesParam(
                              workerInfo,
                              primaryIds,
                              replicaIds)
                        }

                        commitHandler.doParallelCommitFiles(
                          shuffleId,
                          shuffleCommittedInfo,
                          params,
                          commitFilesFailedWorkers)

                        lifecycleManager.workerStatusTracker.recordWorkerFailure(
                          commitFilesFailedWorkers)
                      } finally {
                        // when batch commit thread ends, we need decrementInFlightNum
                        commitHandler.decrementInFlightNum(shuffleCommittedInfo, workerToRequests)
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
    batchHandleCommitPartitionSchedulerThread.foreach(ThreadUtils.shutdown(_))
  }

  def registerShuffle(
      shuffleId: Int,
      numMappers: Int,
      isSegmentGranularityVisible: Boolean): Unit = {
    committedPartitionInfo.put(
      shuffleId,
      ShuffleCommittedInfo(
        JavaUtils.newConcurrentHashMap[Int, util.List[String]](),
        JavaUtils.newConcurrentHashMap[Int, util.List[String]](),
        JavaUtils.newConcurrentHashMap[String, WorkerInfo](),
        JavaUtils.newConcurrentHashMap[String, WorkerInfo](),
        JavaUtils.newConcurrentHashMap[String, StorageInfo](),
        JavaUtils.newConcurrentHashMap[String, StorageInfo](),
        JavaUtils.newConcurrentHashMap[String, RoaringBitmap](),
        new LongAdder,
        new util.HashSet[PartitionLocation](),
        new util.HashSet[PartitionLocation](),
        new AtomicInteger(),
        JavaUtils.newConcurrentHashMap[Int, AtomicInteger]()))

    getCommitHandler(shuffleId).registerShuffle(
      shuffleId,
      numMappers,
      isSegmentGranularityVisible);
  }

  def isSegmentGranularityVisible(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).isSegmentGranularityVisible(shuffleId);
  }

  def isMapperEnded(shuffleId: Int, mapId: Int): Boolean = {
    getCommitHandler(shuffleId).isMapperEnded(shuffleId, mapId)
  }

  def getMapperAttempts(shuffleId: Int): Array[Int] = {
    getCommitHandler(shuffleId).getMapperAttempts(shuffleId)
  }

  def finishMapperAttempt(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      partitionId: Int = -1,
      pushFailedBatches: util.Map[String, util.Set[PushFailedBatch]] = Collections.emptyMap())
      : (Boolean, Boolean) = {
    getCommitHandler(shuffleId).finishMapperAttempt(
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      partitionId,
      pushFailedBatches,
      r => lifecycleManager.workerStatusTracker.recordWorkerFailure(r))
  }

  def releasePartitionResource(shuffleId: Int, partitionId: Int): Unit = {
    getCommitHandler(shuffleId).releasePartitionResource(shuffleId, partitionId)
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    committedPartitionInfo.remove(shuffleId)
    getCommitHandler(shuffleId).removeExpiredShuffle(shuffleId)
  }

  def registerCommitPartitionRequest(
      shuffleId: Int,
      partitionLocation: PartitionLocation,
      cause: Option[StatusCode]): Unit = {
    // If a partition location is null, then the cause will be PUSH_DATA_FAIL_NON_CRITICAL_CAUSE.
    // So here is no need to check partition location is null or not.
    if (batchHandleCommitPartitionEnabled && cause.isDefined && cause.get == StatusCode.HARD_SPLIT) {
      val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
      shuffleCommittedInfo.synchronized {
        shuffleCommittedInfo.unhandledPartitionLocations.add(partitionLocation)
      }
    }
  }

  def tryFinalCommit(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).tryFinalCommit(
      shuffleId,
      r => lifecycleManager.workerStatusTracker.recordWorkerFailure(r))
  }

  def isStageEnd(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).isStageEnd(shuffleId)
  }

  def isStageEndOrInProcess(shuffleId: Int): Boolean = {
    getCommitHandler(shuffleId).isStageEndOrInProcess(shuffleId)
  }

  def setStageEnd(shuffleId: Int): Unit = {
    getCommitHandler(shuffleId).setStageEnd(shuffleId)
  }

  def waitStageEnd(shuffleId: Int): (Boolean, Long) = {
    getCommitHandler(shuffleId).waitStageEnd(shuffleId)
  }

  def handleGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit = {
    getCommitHandler(shuffleId).handleGetReducerFileGroup(context, shuffleId)
  }

  // exposed for test
  def getCommitHandler(shuffleId: Int): CommitHandler = {
    val partitionType = lifecycleManager.getPartitionType(shuffleId)
    commitHandlers.computeIfAbsent(
      partitionType,
      (partitionType: PartitionType) => {
        partitionType match {
          case PartitionType.REDUCE => new ReducePartitionCommitHandler(
              appUniqueId,
              conf,
              lifecycleManager.shuffleAllocatedWorkers,
              committedPartitionInfo,
              lifecycleManager.workerStatusTracker,
              lifecycleManager.rpcSharedThreadPool,
              lifecycleManager)
          case PartitionType.MAP => new MapPartitionCommitHandler(
              appUniqueId,
              conf,
              lifecycleManager.shuffleAllocatedWorkers,
              committedPartitionInfo,
              lifecycleManager.workerStatusTracker,
              lifecycleManager.rpcSharedThreadPool)
          case _ => throw new UnsupportedOperationException(
              s"Unexpected ShufflePartitionType for CommitManager: $partitionType")
        }
      })
  }

  def commitMetrics(): (Long, Long) = commitHandlers.asScala.values.foldLeft(0L -> 0L) {
    case ((totalWritten, totalFileCount), commitHandler) =>
      val (written, fileCount) = commitHandler.commitMetrics()
      (totalWritten + written, totalFileCount + fileCount)
  }

  class ShutdownWorkerListener extends WorkerStatusListener {

    override def notifyChangedWorkersStatus(workersStatus: WorkersStatus): Unit = {
      if (workersStatus.shutdownWorkers != null) {
        lifecycleManager.shuffleAllocatedWorkers.asScala.foreach {
          case (shuffleId, workerIdToPartitionLocationInfos) =>
            if (!isStageEndOrInProcess(shuffleId)) {
              val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
              val needCommitPartitionLocations = new util.HashSet[PartitionLocation]()

              workersStatus.shutdownWorkers.asScala.foreach { worker =>
                val partitionLocationInfos =
                  workerIdToPartitionLocationInfos.get(worker.toUniqueId)
                if (partitionLocationInfos != null) {
                  logWarning(s"Worker ${worker.toUniqueId} shutdown, " +
                    s"commit all it's partition locations for shuffle $shuffleId.")
                  needCommitPartitionLocations.addAll(partitionLocationInfos.getPrimaryPartitions())
                  needCommitPartitionLocations.addAll(partitionLocationInfos.getReplicaPartitions())
                }
              }
              shuffleCommittedInfo.synchronized {
                shuffleCommittedInfo.unhandledPartitionLocations.addAll(
                  needCommitPartitionLocations)
              }
            }
        }
      }
    }
  }
}
