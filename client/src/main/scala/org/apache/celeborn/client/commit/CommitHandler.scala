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
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.celeborn.client.{ShuffleCommittedInfo, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleFailedWorkers, ShuffleFileGroups}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.{CommitFiles, CommitFilesResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.{RpcCallContext, RpcEndpointRef}
import org.apache.celeborn.common.util.{CollectionUtils, JavaUtils, ThreadUtils, Utils}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.ThreadUtils.awaitResult

case class CommitFilesParam(
    worker: WorkerInfo,
    primaryIds: util.List[String],
    replicaIds: util.List[String])

case class FutureWithStatus(
    var future: Future[CommitFilesResponse],
    commitFilesParam: CommitFilesParam,
    var retriedTimes: Int)

case class CommitResult(
    primaryPartitionLocationMap: ConcurrentHashMap[String, PartitionLocation],
    replicaPartitionLocationMap: ConcurrentHashMap[String, PartitionLocation],
    commitFilesFailedWorkers: ShuffleFailedWorkers)

abstract class CommitHandler(
    appUniqueId: String,
    conf: CelebornConf,
    committedPartitionInfo: CommittedPartitionInfo,
    workerStatusTracker: WorkerStatusTracker,
    val sharedRpcPool: ThreadPoolExecutor) extends Logging {

  private val pushReplicateEnabled = conf.clientPushReplicateEnabled
  private val testRetryCommitFiles = conf.testRetryCommitFiles

  private val commitEpoch = new AtomicLong()
  private val totalWritten = new LongAdder
  private val fileCount = new LongAdder
  protected val reducerFileGroupsMap = new ShuffleFileGroups

  implicit val ec = ExecutionContext.fromExecutor(sharedRpcPool)

  def getPartitionType(): PartitionType

  def isStageEnd(shuffleId: Int): Boolean = false

  def isStageEndOrInProcess(shuffleId: Int): Boolean = false

  def isStageDataLost(shuffleId: Int): Boolean = false

  def setStageEnd(shuffleId: Int): Unit = {
    throw new UnsupportedOperationException(
      "Failed when do setStageEnd Operation, MapPartition shuffleType don't " +
        "support set stage end")
  }

  /**
   * return (waitStage isTimeOut, waitTime)
   */
  def waitStageEnd(shuffleId: Int): (Boolean, Long) = (true, 0)

  def isPartitionInProcess(shuffleId: Int, partitionId: Int): Boolean = false

  def batchUnhandledRequests(shuffleId: Int, shuffleCommittedInfo: ShuffleCommittedInfo)
      : Map[WorkerInfo, collection.Set[PartitionLocation]] = {
    // When running to here, if handleStageEnd got lock first and commitFiles,
    // then this batch get this lock, commitPartitionRequests may contains
    // partitions which are already committed by stageEnd process.
    // But inProcessStageEndShuffleSet should have contain this shuffle id,
    // can directly return empty.
    if (isStageEndOrInProcess(shuffleId)) {
      logDebug(s"Shuffle $shuffleId ended or during processing stage end.")
      shuffleCommittedInfo.unhandledPartitionLocations.clear()
      Map.empty[WorkerInfo, Set[PartitionLocation]]
    } else {
      val currentBatch = getUnhandledPartitionLocations(shuffleId, shuffleCommittedInfo)
      shuffleCommittedInfo.unhandledPartitionLocations.clear()
      currentBatch.foreach { partitionLocation =>
        shuffleCommittedInfo.handledPartitionLocations.add(partitionLocation)
        if (partitionLocation.hasPeer) {
          shuffleCommittedInfo.handledPartitionLocations.add(partitionLocation.getPeer)
        }
      }

      if (currentBatch.nonEmpty) {
        logDebug(s"Commit current batch HARD_SPLIT partitions for $shuffleId: " +
          s"${currentBatch.map(_.getUniqueId).mkString("[", ",", "]")}")
        val workerToRequests = currentBatch.flatMap { partitionLocation =>
          if (partitionLocation.hasPeer) {
            Seq(partitionLocation, partitionLocation.getPeer)
          } else {
            Seq(partitionLocation)
          }
        }.groupBy(_.getWorker)
        workerToRequests
      } else {
        Map.empty[WorkerInfo, Set[PartitionLocation]]
      }
    }
  }

  protected def getUnhandledPartitionLocations(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo): mutable.Set[PartitionLocation]

  def incrementInFlightNum(
      shuffleCommittedInfo: ShuffleCommittedInfo,
      workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]]): Unit = {
    shuffleCommittedInfo.allInFlightCommitRequestNum.addAndGet(workerToRequests.size)
  }

  def decrementInFlightNum(
      shuffleCommittedInfo: ShuffleCommittedInfo,
      workerToRequests: Map[WorkerInfo, collection.Set[PartitionLocation]]): Unit = {
    shuffleCommittedInfo.allInFlightCommitRequestNum.addAndGet(-workerToRequests.size)
  }

  /**
   * when someone calls tryFinalCommit, the function will return true if there is no one ever do final commit before,
   * otherwise it will return false.
   *
   * @return
   */
  def tryFinalCommit(
      shuffleId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): Boolean = {
    throw new UnsupportedOperationException(
      "Failed when do final Commit Operation, MapPartition shuffleType only " +
        "support final partition Commit")
  }

  /**
   * Only Reduce partition mode supports cache all file groups for reducer. Map partition doesn't guarantee that all
   * partitions are complete by the time the method is called, as downstream tasks may start early before all tasks
   * are completed.So map partition may need refresh reducer file group if needed.
   */
  def handleGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    reducerFileGroupsMap.remove(shuffleId)
  }

  /**
   * For reduce partition if shuffle registered and corresponding map finished, reply true.
   * For map partition would always return false, as one mapper attempt finished don't mean mapper ended.
   */
  def isMapperEnded(shuffleId: Int, mapId: Int): Boolean = false

  def getMapperAttempts(shuffleId: Int): Array[Int]

  /**
   * return (thisMapperAttemptedFinishedSuccessOrNot, allMapperFinishedOrNot)
   */
  def finishMapperAttempt(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      partitionId: Int,
      recordWorkerFailure: ShuffleFailedWorkers => Unit): (Boolean, Boolean)

  def registerShuffle(shuffleId: Int, numMappers: Int): Unit = {
    reducerFileGroupsMap.put(shuffleId, JavaUtils.newConcurrentHashMap())
  }

  def doParallelCommitFiles(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo,
      params: util.ArrayList[CommitFilesParam],
      commitFilesFailedWorkers: ShuffleFailedWorkers): Unit = {

    def processResponse(res: CommitFilesResponse, worker: WorkerInfo): Unit = {
      shuffleCommittedInfo.synchronized {
        // record committed partitionIds
        res.committedPrimaryIds.asScala.foreach {
          case commitPrimaryId =>
            val partitionUniqueIdList = shuffleCommittedInfo.committedPrimaryIds.computeIfAbsent(
              Utils.splitPartitionLocationUniqueId(commitPrimaryId)._1,
              (k: Int) => new util.ArrayList[String]())
            partitionUniqueIdList.add(commitPrimaryId)
        }

        res.committedReplicaIds.asScala.foreach {
          case commitReplicaId =>
            val partitionUniqueIdList = shuffleCommittedInfo.committedReplicaIds.computeIfAbsent(
              Utils.splitPartitionLocationUniqueId(commitReplicaId)._1,
              (k: Int) => new util.ArrayList[String]())
            partitionUniqueIdList.add(commitReplicaId)
        }

        // record committed partitions storage hint and disk hint
        shuffleCommittedInfo.committedPrimaryStorageInfos.putAll(res.committedPrimaryStorageInfos)
        shuffleCommittedInfo.committedReplicaStorageInfos.putAll(res.committedReplicaStorageInfos)

        // record failed partitions
        shuffleCommittedInfo.failedPrimaryPartitionIds.putAll(
          res.failedPrimaryIds.asScala.map((_, worker)).toMap.asJava)
        shuffleCommittedInfo.failedReplicaPartitionIds.putAll(
          res.failedReplicaIds.asScala.map((_, worker)).toMap.asJava)

        shuffleCommittedInfo.committedMapIdBitmap.putAll(res.committedMapIdBitMap)

        totalWritten.add(res.totalWritten)
        fileCount.add(res.fileCount)
        shuffleCommittedInfo.currentShuffleFileCount.add(res.fileCount)
      }
    }

    val futures = new LinkedBlockingQueue[FutureWithStatus]()

    val outFutures = params.asScala.filter(param =>
      !CollectionUtils.isEmpty(param.primaryIds) ||
        !CollectionUtils.isEmpty(param.replicaIds)) map { param =>
      Future {
        val future = commitFiles(
          appUniqueId,
          shuffleId,
          param.worker,
          param.primaryIds,
          param.replicaIds)

        futures.add(FutureWithStatus(future, param, 1))
      }
    }
    val futureSeq = Future.sequence(outFutures)
    awaitResult(futureSeq, Duration.Inf)

    val maxRetries = conf.clientRequestCommitFilesMaxRetries
    var timeout = conf.rpcAskTimeout.duration.toMillis * maxRetries
    val delta = 50
    while (timeout >= 0 && !futures.isEmpty) {
      val iter = futures.iterator()
      while (iter.hasNext) {
        val status = iter.next()
        if (status.future.isCompleted) {
          status.future.value.get match {
            case scala.util.Success(res) =>
              val worker = status.commitFilesParam.worker
              res.status match {
                case StatusCode.SUCCESS => // do nothing
                case StatusCode.PARTIAL_SUCCESS | StatusCode.SHUFFLE_NOT_REGISTERED | StatusCode.REQUEST_FAILED | StatusCode.WORKER_EXCLUDED =>
                  logInfo(s"Request commitFiles return ${res.status} for " +
                    s"${Utils.makeShuffleKey(appUniqueId, shuffleId)}")
                  if (res.status != StatusCode.WORKER_EXCLUDED) {
                    commitFilesFailedWorkers.put(worker, (res.status, System.currentTimeMillis()))
                  }
                case _ =>
                  logError(s"Should never reach here! commit files response status ${res.status}")
              }

              processResponse(res, worker)
              iter.remove()
            case scala.util.Failure(e) =>
              val worker = status.commitFilesParam.worker
              logError(
                s"Ask worker($worker) CommitFiles for $shuffleId failed" +
                  s" (attempt ${status.retriedTimes}/$maxRetries).",
                e)
              if (status.retriedTimes < maxRetries) {
                status.retriedTimes = status.retriedTimes + 1
                status.future = commitFiles(
                  appUniqueId,
                  shuffleId,
                  status.commitFilesParam.worker,
                  status.commitFilesParam.primaryIds,
                  status.commitFilesParam.replicaIds)
              } else {
                val res = CommitFilesResponse(
                  StatusCode.REQUEST_FAILED,
                  List.empty.asJava,
                  List.empty.asJava,
                  status.commitFilesParam.primaryIds,
                  status.commitFilesParam.replicaIds)
                processResponse(res, status.commitFilesParam.worker)
                iter.remove()
              }
          }
        }
      }

      if (!futures.isEmpty) {
        Thread.sleep(delta)
      }
      timeout = timeout - delta
    }

    val iter = futures.iterator()
    while (iter.hasNext) {
      val status = iter.next()
      logError(
        s"Ask worker(${status.commitFilesParam.worker}) CommitFiles for $shuffleId timed out")
      val res = CommitFilesResponse(
        StatusCode.REQUEST_FAILED,
        List.empty.asJava,
        List.empty.asJava,
        status.commitFilesParam.primaryIds,
        status.commitFilesParam.replicaIds)
      processResponse(res, status.commitFilesParam.worker)
      iter.remove()
    }
  }

  def parallelCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[WorkerInfo, ShufflePartitionLocationInfo],
      partitionIdOpt: Option[Int] = None): CommitResult = {
    val shuffleCommittedInfo = committedPartitionInfo.get(shuffleId)
    val primaryPartMap = JavaUtils.newConcurrentHashMap[String, PartitionLocation]
    val replicaPartMap = JavaUtils.newConcurrentHashMap[String, PartitionLocation]
    val commitFilesFailedWorkers = new ShuffleFailedWorkers()

    if (CollectionUtils.isEmpty(allocatedWorkers)) {
      return CommitResult(primaryPartMap, replicaPartMap, commitFilesFailedWorkers)
    }

    val commitFileStartTime = System.nanoTime()
    val workerPartitionLocations = allocatedWorkers.asScala.filter(!_._2.isEmpty)

    val params = new util.ArrayList[CommitFilesParam](workerPartitionLocations.size)
    workerPartitionLocations.foreach { case (worker, partitionLocationInfo) =>
      val primaryParts =
        partitionLocationInfo.getPrimaryPartitions(partitionIdOpt)
      val replicaParts = partitionLocationInfo.getReplicaPartitions(partitionIdOpt)
      primaryParts.asScala.foreach { p =>
        val partition = new PartitionLocation(p)
        partition.setFetchPort(worker.fetchPort)
        partition.setPeer(null)
        primaryPartMap.put(partition.getUniqueId, partition)
      }
      replicaParts.asScala.foreach { p =>
        val partition = new PartitionLocation(p)
        partition.setFetchPort(worker.fetchPort)
        partition.setPeer(null)
        replicaPartMap.put(partition.getUniqueId, partition)
      }

      val (primaryIds, replicaIds) = shuffleCommittedInfo.synchronized {
        (
          primaryParts.asScala
            .filterNot(shuffleCommittedInfo.handledPartitionLocations.contains)
            .map(_.getUniqueId).toList.asJava,
          replicaParts.asScala
            .filterNot(shuffleCommittedInfo.handledPartitionLocations.contains)
            .map(_.getUniqueId).toList.asJava)
      }

      params.add(CommitFilesParam(
        worker,
        primaryIds,
        replicaIds))
    }

    doParallelCommitFiles(shuffleId, shuffleCommittedInfo, params, commitFilesFailedWorkers)

    logInfo(s"Shuffle $shuffleId " +
      s"commit files complete. File count ${shuffleCommittedInfo.currentShuffleFileCount.sum()} " +
      s"using ${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - commitFileStartTime)} ms")

    CommitResult(primaryPartMap, replicaPartMap, commitFilesFailedWorkers)
  }

  def commitFiles(
      applicationId: String,
      shuffleId: Int,
      worker: WorkerInfo,
      primaryIds: util.List[String],
      replicaIds: util.List[String]): Future[CommitFilesResponse] = {

    if (!testRetryCommitFiles) {
      val commitFiles = CommitFiles(
        applicationId,
        shuffleId,
        primaryIds,
        replicaIds,
        getMapperAttempts(shuffleId),
        commitEpoch.incrementAndGet())

      if (conf.clientCommitFilesIgnoreExcludedWorkers &&
        workerStatusTracker.excludedWorkers.containsKey(worker)) {
        Future(CommitFilesResponse(
          StatusCode.WORKER_EXCLUDED,
          List.empty.asJava,
          List.empty.asJava,
          primaryIds,
          replicaIds))
      } else {
        worker.endpoint.ask[CommitFilesResponse](commitFiles)
      }
    } else {
      // for test
      val commitFiles1 = CommitFiles(
        applicationId,
        shuffleId,
        primaryIds.subList(0, primaryIds.size() / 2),
        replicaIds.subList(0, replicaIds.size() / 2),
        getMapperAttempts(shuffleId),
        commitEpoch.incrementAndGet())
      val res1 = requestCommitFilesWithRetryForTest(worker.endpoint, commitFiles1)

      val commitFiles = CommitFiles(
        applicationId,
        shuffleId,
        primaryIds.subList(primaryIds.size() / 2, primaryIds.size()),
        replicaIds.subList(replicaIds.size() / 2, replicaIds.size()),
        getMapperAttempts(shuffleId),
        commitEpoch.incrementAndGet())
      val res2 = requestCommitFilesWithRetryForTest(worker.endpoint, commitFiles)

      res1.committedPrimaryStorageInfos.putAll(res2.committedPrimaryStorageInfos)
      res1.committedReplicaStorageInfos.putAll(res2.committedReplicaStorageInfos)
      res1.committedMapIdBitMap.putAll(res2.committedMapIdBitMap)
      val res = CommitFilesResponse(
        status = if (res1.status == StatusCode.SUCCESS) res2.status else res1.status,
        (res1.committedPrimaryIds.asScala ++ res2.committedPrimaryIds.asScala).toList.asJava,
        (res1.committedReplicaIds.asScala ++ res2.committedReplicaIds.asScala).toList.asJava,
        (res1.failedPrimaryIds.asScala ++ res2.failedPrimaryIds.asScala).toList.asJava,
        (res1.failedReplicaIds.asScala ++ res2.failedReplicaIds.asScala).toList.asJava,
        res1.committedPrimaryStorageInfos,
        res1.committedReplicaStorageInfos,
        res1.committedMapIdBitMap,
        res1.totalWritten + res2.totalWritten,
        res1.fileCount + res2.fileCount)

      Future(res)
    }
  }

  def collectResult(
      shuffleId: Int,
      shuffleCommittedInfo: ShuffleCommittedInfo,
      primaryPartitionUniqueIds: util.Iterator[String],
      replicaPartitionUniqueIds: util.Iterator[String],
      primaryPartMap: ConcurrentHashMap[String, PartitionLocation],
      replicaPartMap: ConcurrentHashMap[String, PartitionLocation]): Unit = {
    val committedPartitions = new util.HashMap[String, PartitionLocation]
    primaryPartitionUniqueIds.asScala.foreach { id =>
      val partitionLocation = primaryPartMap.get(id)
      partitionLocation.setStorageInfo(
        shuffleCommittedInfo.committedPrimaryStorageInfos.get(id))
      partitionLocation.setMapIdBitMap(shuffleCommittedInfo.committedMapIdBitmap.get(id))
      committedPartitions.put(id, partitionLocation)
    }

    replicaPartitionUniqueIds.asScala.foreach { id =>
      val replicaPartition = replicaPartMap.get(id)
      replicaPartition.setStorageInfo(shuffleCommittedInfo.committedReplicaStorageInfos.get(id))
      val primaryPartition = committedPartitions.get(id)
      if (primaryPartition ne null) {
        primaryPartition.setPeer(replicaPartition)
        replicaPartition.setPeer(primaryPartition)
      } else {
        logInfo(s"Shuffle $shuffleId partition $id: primary lost, " +
          s"use replica $replicaPartition.")
        replicaPartition.setMapIdBitMap(shuffleCommittedInfo.committedMapIdBitmap.get(id))
        committedPartitions.put(id, replicaPartition)
      }
    }

    committedPartitions.values().asScala.foreach { partition =>
      val partitionLocations = reducerFileGroupsMap.get(shuffleId).computeIfAbsent(
        partition.getId,
        (k: Integer) => new util.HashSet[PartitionLocation]())
      partitionLocations.add(partition)
    }
  }

  private def requestCommitFilesWithRetryForTest(
      endpoint: RpcEndpointRef,
      message: CommitFiles): CommitFilesResponse = {
    val maxRetries = conf.clientRequestCommitFilesMaxRetries
    var retryTimes = 0
    while (retryTimes < maxRetries) {
      try {
        if (retryTimes < maxRetries - 1) {
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
            s"AskSync worker(${endpoint.address}) CommitFiles for ${message.shuffleId} failed" +
              s" (attempt $retryTimes/$maxRetries).",
            e)
      }
    }

    CommitFilesResponse(
      StatusCode.REQUEST_FAILED,
      List.empty.asJava,
      List.empty.asJava,
      message.primaryIds,
      message.replicaIds)
  }

  def checkDataLost(
      shuffleId: Int,
      failedPrimaries: util.Map[String, WorkerInfo],
      failedReplicas: util.Map[String, WorkerInfo]): Boolean = {
    val shuffleKey = Utils.makeShuffleKey(appUniqueId, shuffleId)
    if (!pushReplicateEnabled && failedPrimaries.size() != 0) {
      val msg =
        failedPrimaries.asScala.map {
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
      val failedBothPartitionIdsToWorker = failedPrimaries.asScala.flatMap {
        case (partitionUniqueId, worker) =>
          if (failedReplicas.asScala.contains(partitionUniqueId)) {
            Some(partitionUniqueId -> (worker, failedReplicas.get(partitionUniqueId)))
          } else {
            None
          }
      }
      if (failedBothPartitionIdsToWorker.nonEmpty) {
        val msg = failedBothPartitionIdsToWorker.map {
          case (partitionUniqueId, (primaryWorker, replicaWorker)) =>
            s"Lost partition $partitionUniqueId " +
              s"in primary worker [${primaryWorker.readableAddress()}] and replica worker [$replicaWorker]"
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

  def releasePartitionResource(shuffleId: Int, partitionId: Int): Unit = {
    val fileGroups = reducerFileGroupsMap.get(shuffleId)
    if (fileGroups != null) {
      fileGroups.remove(partitionId)
    }
  }
}
