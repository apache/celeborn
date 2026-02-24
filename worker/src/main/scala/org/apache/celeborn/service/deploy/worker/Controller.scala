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

package org.apache.celeborn.service.deploy.worker

import java.io.IOException
import java.util.{ArrayList => jArrayList, HashMap => jHashMap, List => jList, Set => jSet}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicIntegerArray, AtomicReference}
import java.util.function.BiFunction

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.netty.util.{HashedWheelTimer, Timeout, TimerTask}
import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{WorkerInfo, WorkerPartitionLocationInfo}
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.{JavaUtils, Utils}
import org.apache.celeborn.service.deploy.worker.storage.{MapPartitionMetaHandler, PartitionDataWriter, StorageManager}

private[deploy] class Controller(
    override val rpcEnv: RpcEnv,
    val conf: CelebornConf,
    val metricsSystem: MetricsSystem,
    val workerSource: WorkerSource)
  extends RpcEndpoint with Logging {

  var storageManager: StorageManager = _
  var shuffleMapperAttempts: ConcurrentHashMap[String, AtomicIntegerArray] = _
  // shuffleKey -> (epoch -> CommitInfo)
  var shuffleCommitInfos: ConcurrentHashMap[String, ConcurrentHashMap[Long, CommitInfo]] = _
  // shuffleKey -> (epoch -> (commitWaitTimestamp, RpcContext))
  var shuffleCommitTime
      : ConcurrentHashMap[String, ConcurrentHashMap[Long, (Long, RpcCallContext)]] =
    _
  var shufflePartitionType: ConcurrentHashMap[String, PartitionType] = _
  var shufflePushDataTimeout: ConcurrentHashMap[String, Long] = _
  var workerInfo: WorkerInfo = _
  var partitionLocationInfo: WorkerPartitionLocationInfo = _
  var timer: HashedWheelTimer = _
  var commitThreadPool: ThreadPoolExecutor = _
  var commitFinishedChecker: ScheduledExecutorService = _
  var asyncReplyPool: ScheduledExecutorService = _
  var createWriterThreadPool: ThreadPoolExecutor = _
  val minPartitionSizeToEstimate = conf.minPartitionSizeToEstimate
  var shutdown: AtomicBoolean = _
  val defaultPushdataTimeout = conf.pushDataTimeoutMs
  val mockCommitFilesFailure = conf.testMockCommitFilesFailure
  val shuffleCommitTimeout = conf.workerShuffleCommitTimeout
  val createWriterParallelTimeout = conf.workerCreateWriterParallelTimeout
  val workerCommitFilesCheckInterval = conf.workerCommitFilesCheckInterval
  val remoteStorageDirs = conf.remoteStorageDirs

  def init(worker: Worker): Unit = {
    storageManager = worker.storageManager
    shufflePartitionType = worker.shufflePartitionType
    shufflePushDataTimeout = worker.shufflePushDataTimeout
    shuffleMapperAttempts = worker.shuffleMapperAttempts
    shuffleCommitInfos = worker.shuffleCommitInfos
    shuffleCommitTime = worker.shuffleCommitTime
    workerInfo = worker.workerInfo
    partitionLocationInfo = worker.partitionLocationInfo
    timer = worker.timer
    commitThreadPool = worker.commitThreadPool
    asyncReplyPool = worker.asyncReplyPool
    createWriterThreadPool = worker.createWriterThreadPool
    shutdown = worker.shutdown

    commitFinishedChecker = worker.commitFinishedChecker
    commitFinishedChecker.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          checkCommitTimeout(shuffleCommitTime)
        }
      },
      0,
      workerCommitFilesCheckInterval,
      TimeUnit.MILLISECONDS)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveSlots(
          applicationId,
          shuffleId,
          primaryLocations,
          replicaLocations,
          splitThreshold,
          splitMode,
          partitionType,
          rangeReadFilter,
          userIdentifier,
          pushDataTimeout,
          partitionSplitEnabled,
          isSegmentGranularityVisible) =>
      checkAuth(context, applicationId)
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.RESERVE_SLOTS_TIME, shuffleKey) {
        logDebug(s"Received ReserveSlots request, $shuffleKey, " +
          s"primary partitions: ${primaryLocations.asScala.map(_.getUniqueId).mkString(",")}; " +
          s"replica partitions: ${replicaLocations.asScala.map(_.getUniqueId).mkString(",")}.")
        handleReserveSlots(
          context,
          applicationId,
          shuffleId,
          primaryLocations,
          replicaLocations,
          splitThreshold,
          splitMode,
          partitionType,
          rangeReadFilter,
          userIdentifier,
          pushDataTimeout,
          partitionSplitEnabled,
          isSegmentGranularityVisible)
        logDebug(s"ReserveSlots for $shuffleKey finished.")
      }

    case CommitFiles(
          applicationId,
          shuffleId,
          primaryIds,
          replicaIds,
          mapAttempts,
          epoch,
          mockFailure) =>
      checkAuth(context, applicationId)
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      logDebug(s"Received CommitFiles request, $shuffleKey, primary files" +
        s" ${primaryIds.asScala.mkString(",")}; replica files ${replicaIds.asScala.mkString(",")}.")
      val commitFilesTimeMs = Utils.timeIt({
        handleCommitFiles(
          context,
          shuffleKey,
          primaryIds,
          replicaIds,
          mapAttempts,
          epoch,
          mockFailure)
      })
      logDebug(s"Done processed CommitFiles request with shuffleKey $shuffleKey, in " +
        s"$commitFilesTimeMs ms.")

    case DestroyWorkerSlots(shuffleKey, primaryLocations, replicaLocations, mockFailure) =>
      checkAuth(context, Utils.splitShuffleKey(shuffleKey)._1)
      handleDestroy(context, shuffleKey, primaryLocations, replicaLocations, mockFailure)
  }

  private def handleReserveSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      requestPrimaryLocs: jList[PartitionLocation],
      requestReplicaLocs: jList[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier,
      pushDataTimeout: Long,
      partitionSplitEnabled: Boolean,
      isSegmentGranularityVisible: Boolean): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (shutdown.get()) {
      val msg = "Current worker is shutting down!"
      logError(s"[handleReserveSlots] $msg")
      context.reply(ReserveSlotsResponse(StatusCode.WORKER_SHUTDOWN, msg))
      return
    }

    if (storageManager.healthyWorkingDirs().size <= 0 && remoteStorageDirs.isEmpty) {
      val msg = "Local storage has no available dirs!"
      logError(s"[handleReserveSlots] $msg")
      context.reply(ReserveSlotsResponse(StatusCode.NO_AVAILABLE_WORKING_DIR, msg))
      return
    }
    val primaryLocs = createWriters(
      shuffleKey,
      applicationId,
      shuffleId,
      requestPrimaryLocs,
      splitThreshold,
      splitMode,
      partitionType,
      rangeReadFilter,
      userIdentifier,
      partitionSplitEnabled,
      isSegmentGranularityVisible,
      isPrimary = true)
    if (primaryLocs.size() < requestPrimaryLocs.size()) {
      val msg = s"Not all primary partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, will destroy writers.")
      destroyWriters(primaryLocs, shuffleKey)
      context.reply(ReserveSlotsResponse(StatusCode.RESERVE_SLOTS_FAILED, msg))
      return
    }

    val replicaLocs = createWriters(
      shuffleKey,
      applicationId,
      shuffleId,
      requestReplicaLocs,
      splitThreshold,
      splitMode,
      partitionType,
      rangeReadFilter,
      userIdentifier,
      partitionSplitEnabled,
      isSegmentGranularityVisible,
      isPrimary = false)
    if (replicaLocs.size() < requestReplicaLocs.size()) {
      val msg = s"Not all replica partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, destroy writers.")
      destroyWriters(primaryLocs, shuffleKey)
      destroyWriters(replicaLocs, shuffleKey)
      context.reply(ReserveSlotsResponse(StatusCode.RESERVE_SLOTS_FAILED, msg))
      return
    }

    // reserve success, update status
    partitionLocationInfo.addPrimaryPartitions(shuffleKey, primaryLocs)
    partitionLocationInfo.addReplicaPartitions(shuffleKey, replicaLocs)
    shufflePartitionType.put(shuffleKey, partitionType)
    shufflePushDataTimeout.put(
      shuffleKey,
      if (pushDataTimeout <= 0) defaultPushdataTimeout else pushDataTimeout)
    workerInfo.allocateSlots(
      shuffleKey,
      Utils.getSlotsPerDisk(requestPrimaryLocs, requestReplicaLocs))
    workerSource.incCounter(WorkerSource.SLOTS_ALLOCATED, primaryLocs.size() + replicaLocs.size())

    logInfo(s"Reserved ${primaryLocs.size()} primary location " +
      s"${primaryLocs.asScala.map(_.getUniqueId).mkString(",")} and ${replicaLocs.size()} replica location " +
      s"${replicaLocs.asScala.map(_.getUniqueId).mkString(",")} for $shuffleKey ")
    if (log.isDebugEnabled()) {
      logDebug(s"primary: $primaryLocs\nreplica: $replicaLocs.")
    }
    context.reply(ReserveSlotsResponse(StatusCode.SUCCESS))
  }

  private def createWriters(
      shuffleKey: String,
      applicationId: String,
      shuffleId: Int,
      requestLocs: jList[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier,
      partitionSplitEnabled: Boolean,
      isSegmentGranularityVisible: Boolean,
      isPrimary: Boolean): jList[PartitionLocation] = {
    val partitionLocations = new jArrayList[PartitionLocation]()
    try {
      def createWriter(partitionLocation: PartitionLocation): PartitionLocation = {
        createPartitionDataWriter(
          shuffleKey,
          applicationId,
          shuffleId,
          partitionLocation,
          splitThreshold,
          splitMode,
          partitionType,
          rangeReadFilter,
          userIdentifier,
          partitionSplitEnabled,
          isSegmentGranularityVisible,
          isPrimary)
      }
      if (createWriterThreadPool == null) {
        partitionLocations.addAll(requestLocs.asScala.map(createWriter).asJava)
      } else {
        partitionLocations.addAll(Utils.tryFuturesWithTimeout(
          requestLocs.asScala.map(requestLoc =>
            Utils.future(createWriter(requestLoc))(createWriterThreadPool)).toList,
          createWriterParallelTimeout,
          s"Create FileWriter for $shuffleKey timeout.").asJava)
      }
    } catch {
      case e: Exception =>
        logError(s"Create FileWriter for $shuffleKey failed.", e)
    }
    partitionLocations
  }

  private def createPartitionDataWriter(
      shuffleKey: String,
      applicationId: String,
      shuffleId: Int,
      requestLoc: PartitionLocation,
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      partitionType: PartitionType,
      rangeReadFilter: Boolean,
      userIdentifier: UserIdentifier,
      partitionSplitEnabled: Boolean,
      isSegmentGranularityVisible: Boolean,
      isPrimary: Boolean): PartitionLocation = {
    try {
      var location =
        if (isPrimary) {
          partitionLocationInfo.getPrimaryLocation(
            shuffleKey,
            requestLoc.getUniqueId)
        } else {
          partitionLocationInfo.getReplicaLocation(
            shuffleKey,
            requestLoc.getUniqueId)
        }
      if (location == null) {
        location = requestLoc
        val writer = storageManager.createPartitionDataWriter(
          applicationId,
          shuffleId,
          location,
          splitThreshold,
          splitMode,
          partitionType,
          rangeReadFilter,
          userIdentifier,
          partitionSplitEnabled,
          isSegmentGranularityVisible)
        new WorkingPartition(location, writer)
      } else {
        location
      }
    } catch {
      case e: Exception =>
        logError(s"Create FileWriter for $requestLoc $shuffleKey failed.", e)
        throw e
    }
  }

  private def destroyWriters(
      partitionLocations: jList[PartitionLocation],
      shuffleKey: String): Unit = {
    partitionLocations.asScala.foreach { partitionLocation =>
      val fileWriter = partitionLocation.asInstanceOf[WorkingPartition].getFileWriter
      fileWriter.destroy(new IOException(s"Destroy FileWriter  $fileWriter caused by " +
        s"reserving slots failed for $shuffleKey."))
    }
  }

  private def commitFiles(
      shuffleKey: String,
      uniqueIds: jList[String],
      committedIds: jSet[String],
      emptyFileIds: jSet[String],
      failedIds: jSet[String],
      committedStorageInfos: ConcurrentHashMap[String, StorageInfo],
      committedMapIdBitMap: ConcurrentHashMap[String, RoaringBitmap],
      partitionSizeList: LinkedBlockingQueue[Long],
      isPrimary: Boolean = true)
      : (CompletableFuture[Void], ArrayBuffer[CompletableFuture[Void]]) = {
    val tasks = ArrayBuffer[CompletableFuture[Void]]()
    if (uniqueIds != null) {
      uniqueIds.asScala.foreach { uniqueId =>
        val task = CompletableFuture.runAsync(
          new Runnable {
            override def run(): Unit = {
              try {
                val location =
                  if (isPrimary) {
                    partitionLocationInfo.getPrimaryLocation(shuffleKey, uniqueId)
                  } else {
                    partitionLocationInfo.getReplicaLocation(shuffleKey, uniqueId)
                  }

                if (location == null) {
                  logError(s"Get Partition Location for $shuffleKey $uniqueId but didn't exist, treat as failed.")
                  failedIds.add(uniqueId)
                  return
                }

                val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
                waitMapPartitionRegionFinished(fileWriter, shuffleCommitTimeout)
                val bytes = fileWriter.close()
                if (bytes > 0L) {
                  logDebug(
                    s"Commit file $shuffleKey $uniqueId ${fileWriter.getFilePath} success fileSize: $bytes")
                  if (fileWriter.getStorageInfo == null) {
                    // Only HDFS can be null, means that this partition location is deleted.
                    logDebug(s"Location $uniqueId is deleted.")
                  } else {
                    val storageInfo = fileWriter.getStorageInfo
                    committedStorageInfos.put(uniqueId, storageInfo)
                    if (fileWriter.getMapIdBitMap != null) {
                      committedMapIdBitMap.put(uniqueId, fileWriter.getMapIdBitMap)
                    }
                    if (bytes >= minPartitionSizeToEstimate) {
                      partitionSizeList.add(bytes)
                    }
                    committedIds.add(uniqueId)
                  }
                } else {
                  emptyFileIds.add(uniqueId)
                }
                if (mockCommitFilesFailure) {
                  Thread.sleep(10)
                }
              } catch {
                case e: IOException =>
                  logError(s"Commit file for $shuffleKey $uniqueId failed.", e)
                  workerSource.incCounter(WorkerSource.COMMIT_FILES_FAIL_COUNT)
                  failedIds.add(uniqueId)
              }
            }
          },
          commitThreadPool)
        tasks.append(task)
      }
    }
    val future: CompletableFuture[Void] =
      if (tasks.isEmpty) null else CompletableFuture.allOf(tasks.toSeq: _*)
    (future, tasks)
  }

  private def waitMapPartitionRegionFinished(
      fileWriter: PartitionDataWriter,
      waitTimeout: Long): Unit = {
    fileWriter.getMetaHandler match {
      case metaHandler: MapPartitionMetaHandler =>
        if (metaHandler.checkPartitionRegionFinished(waitTimeout)) {
          logDebug(
            s"CommitFile succeed to waitMapPartitionRegionFinished ${fileWriter.getFilePath}")
        } else {
          logWarning(
            s"CommitFile failed to waitMapPartitionRegionFinished ${fileWriter.getFilePath}")
        }
      case _ =>
    }
  }

  private def handleCommitFiles(
      context: RpcCallContext,
      shuffleKey: String,
      primaryIds: jList[String],
      replicaIds: jList[String],
      mapAttempts: Array[Int],
      epoch: Long,
      mockFailure: Boolean): Unit = {
    if (mockFailure) {
      logError(s"Mock commit files failure for Shuffle $shuffleKey!")
      context.reply(
        CommitFilesResponse(
          StatusCode.COMMIT_FILES_MOCK_FAILURE,
          List.empty.asJava,
          List.empty.asJava,
          primaryIds,
          replicaIds))
      return
    }

    def alreadyCommitted(shuffleKey: String, epoch: Long): Boolean = {
      shuffleCommitInfos.containsKey(shuffleKey) && shuffleCommitInfos.get(shuffleKey).containsKey(
        epoch)
    }

    // Reply SHUFFLE_NOT_REGISTERED if shuffleKey does not exist AND the shuffle is not committed.
    // Say the first CommitFiles-epoch request succeeds in Worker and removed from partitionLocationInfo,
    // but for some reason the client thinks it's failed, the client will trigger again, so we should
    // check whether the CommitFiles-epoch is already committed here.
    if (!partitionLocationInfo.containsShuffle(shuffleKey) && !alreadyCommitted(
        shuffleKey,
        epoch)) {
      logError(s"Shuffle $shuffleKey doesn't exist!")
      context.reply(
        CommitFilesResponse(
          StatusCode.SHUFFLE_UNREGISTERED,
          List.empty.asJava,
          List.empty.asJava,
          primaryIds,
          replicaIds))
      return
    }

    shuffleCommitInfos.putIfAbsent(
      shuffleKey,
      JavaUtils.newConcurrentHashMap[Long, CommitInfo]())
    val epochCommitMap = shuffleCommitInfos.get(shuffleKey)

    // to store the primaryIds and replicaIds
    val response = CommitFilesResponse(
      null,
      List.empty.asJava,
      List.empty.asJava,
      primaryIds,
      replicaIds)
    epochCommitMap.putIfAbsent(epoch, new CommitInfo(response, CommitInfo.COMMIT_NOTSTARTED))
    val commitInfo = epochCommitMap.get(epoch)

    commitInfo.synchronized {
      if (commitInfo.status == CommitInfo.COMMIT_FINISHED) {
        logInfo(s"$shuffleKey CommitFinished, just return the response")
        context.reply(commitInfo.response)
        return
      } else if (commitInfo.status == CommitInfo.COMMIT_INPROCESS) {
        logInfo(s"$shuffleKey CommitFiles inprogress, wait for finish")
        // Replace the ThreadPool to avoid blocking
        // Read and write security of epoch in epochWaitTimeMap is guaranteed by commitInfo's lock
        shuffleCommitTime.putIfAbsent(
          shuffleKey,
          JavaUtils.newConcurrentHashMap[Long, (Long, RpcCallContext)]())
        val epochWaitTimeMap = shuffleCommitTime.get(shuffleKey)
        val commitStartWaitTime = System.currentTimeMillis()
        epochWaitTimeMap.put(epoch, (commitStartWaitTime, context))
        return
      } else {
        logInfo(
          s"Start commitFiles for ${shuffleKey}, primaryIds : ${primaryIds.asScala.mkString(",")}, "
            + s"replicaIds : ${replicaIds.asScala.mkString(",")}")
        commitInfo.status = CommitInfo.COMMIT_INPROCESS
        workerSource.startTimer(WorkerSource.COMMIT_FILES_TIME, shuffleKey)
      }
    }

    // Update shuffleMapperAttempts
    shuffleMapperAttempts.putIfAbsent(shuffleKey, new AtomicIntegerArray(mapAttempts))
    updateShuffleMapperAttempts(mapAttempts, shuffleMapperAttempts.get(shuffleKey))

    // Use ConcurrentSet to avoid excessive lock contention.
    val committedPrimaryIds = ConcurrentHashMap.newKeySet[String]()
    val committedReplicaIds = ConcurrentHashMap.newKeySet[String]()
    val emptyFilePrimaryIds = ConcurrentHashMap.newKeySet[String]()
    val emptyFileReplicaIds = ConcurrentHashMap.newKeySet[String]()
    val failedPrimaryIds = ConcurrentHashMap.newKeySet[String]()
    val failedReplicaIds = ConcurrentHashMap.newKeySet[String]()
    val committedPrimaryStorageInfos = JavaUtils.newConcurrentHashMap[String, StorageInfo]()
    val committedReplicaStorageInfos = JavaUtils.newConcurrentHashMap[String, StorageInfo]()
    val committedMapIdBitMap = JavaUtils.newConcurrentHashMap[String, RoaringBitmap]()
    val partitionSizeList = new LinkedBlockingQueue[Long]()

    val (primaryFuture, primaryTasks) =
      commitFiles(
        shuffleKey,
        primaryIds,
        committedPrimaryIds,
        emptyFilePrimaryIds,
        failedPrimaryIds,
        committedPrimaryStorageInfos,
        committedMapIdBitMap,
        partitionSizeList)
    val (replicaFuture, replicaTasks) = commitFiles(
      shuffleKey,
      replicaIds,
      committedReplicaIds,
      emptyFileReplicaIds,
      failedReplicaIds,
      committedReplicaStorageInfos,
      committedMapIdBitMap,
      partitionSizeList,
      false)

    val future =
      if (primaryFuture != null && replicaFuture != null) {
        CompletableFuture.allOf(primaryFuture, replicaFuture)
      } else if (primaryFuture != null) {
        primaryFuture
      } else if (replicaFuture != null) {
        replicaFuture
      } else {
        null
      }

    val tasks = primaryTasks ++ replicaTasks

    def reply(): Unit = {
      // release slots before reply.
      val releasePrimaryLocations =
        partitionLocationInfo.removePrimaryPartitions(shuffleKey, primaryIds)
      val releaseReplicaLocations =
        partitionLocationInfo.removeReplicaPartitions(shuffleKey, replicaIds)
      logDebug(s"$shuffleKey remove" +
        s" slots count ${releasePrimaryLocations._2 + releaseReplicaLocations._2}")
      logDebug(s"CommitFiles result" +
        s" $committedPrimaryStorageInfos $committedReplicaStorageInfos")
      workerInfo.releaseSlots(shuffleKey, releasePrimaryLocations._1)
      workerInfo.releaseSlots(shuffleKey, releaseReplicaLocations._1)

      val committedPrimaryIdList = new jArrayList[String](committedPrimaryIds)
      val committedReplicaIdList = new jArrayList[String](committedReplicaIds)
      val failedPrimaryIdList = new jArrayList[String](failedPrimaryIds)
      val failedReplicaIdList = new jArrayList[String](failedReplicaIds)
      val committedPrimaryStorageAndDiskHintList =
        new jHashMap[String, StorageInfo](committedPrimaryStorageInfos)
      val committedReplicaStorageAndDiskHintList =
        new jHashMap[String, StorageInfo](committedReplicaStorageInfos)
      val committedMapIdBitMapList = new jHashMap[String, RoaringBitmap](committedMapIdBitMap)
      val totalSize = partitionSizeList.asScala.sum
      val fileCount = partitionSizeList.size()
      // reply
      val response =
        if (failedPrimaryIds.isEmpty && failedReplicaIds.isEmpty) {
          logInfo(
            s"CommitFiles for $shuffleKey success with " +
              s"${committedPrimaryIds.size()} committed primary partitions ${committedPrimaryIds.asScala.mkString(",")}, " +
              s"${emptyFilePrimaryIds.size()} empty primary partitions ${emptyFilePrimaryIds.asScala.mkString(",")}, " +
              s"${failedPrimaryIds.size()} failed primary partitions ${failedPrimaryIds.asScala.mkString(",")}, " +
              s"${committedReplicaIds.size()} committed replica partitions ${committedReplicaIds.asScala.mkString(",")}, " +
              s"${emptyFileReplicaIds.size()} empty replica partitions ${emptyFileReplicaIds.asScala.mkString(",")}, " +
              s"${failedReplicaIds.size()} failed replica partitions ${failedReplicaIds.asScala.mkString(",")}.")
          CommitFilesResponse(
            StatusCode.SUCCESS,
            committedPrimaryIdList,
            committedReplicaIdList,
            List.empty.asJava,
            List.empty.asJava,
            committedPrimaryStorageAndDiskHintList,
            committedReplicaStorageAndDiskHintList,
            committedMapIdBitMapList,
            totalSize,
            fileCount)
        } else {
          logWarning(
            s"CommitFiles for $shuffleKey failed with " +
              s"${committedPrimaryIds.size()} committed primary partitions ${committedPrimaryIds.asScala.mkString(",")}, " +
              s"${emptyFilePrimaryIds.size()} empty primary partitions ${emptyFilePrimaryIds.asScala.mkString(",")}, " +
              s"${failedPrimaryIds.size()} failed primary partitions, ${failedPrimaryIds.asScala.mkString(",")}, " +
              s"${committedReplicaIds.size()} committed replica partitions ${committedReplicaIds.asScala.mkString(",")}, " +
              s"${emptyFileReplicaIds.size()} empty replica partitions ${emptyFileReplicaIds.asScala.mkString(",")}, " +
              s"${failedReplicaIds.size()} failed replica partitions ${failedReplicaIds.asScala.mkString(",")}.")
          CommitFilesResponse(
            StatusCode.PARTIAL_SUCCESS,
            committedPrimaryIdList,
            committedReplicaIdList,
            failedPrimaryIdList,
            failedReplicaIdList,
            committedPrimaryStorageAndDiskHintList,
            committedReplicaStorageAndDiskHintList,
            committedMapIdBitMapList,
            totalSize,
            fileCount)
        }
      commitInfo.synchronized {
        commitInfo.response = response
        commitInfo.status = CommitInfo.COMMIT_FINISHED
      }
      context.reply(response)

      workerSource.stopTimer(WorkerSource.COMMIT_FILES_TIME, shuffleKey)
    }

    if (future != null) {
      val result = new AtomicReference[CompletableFuture[Unit]]()

      val timeout = timer.newTimeout(
        new TimerTask {
          override def run(timeout: Timeout): Unit = {
            if (result.get() != null) {
              future.cancel(true)
              tasks.foreach { task =>
                task.cancel(true)
              }
              logWarning(s"After waiting $shuffleCommitTimeout ms, cancel all commit file jobs.")
            }
          }
        },
        shuffleCommitTimeout,
        TimeUnit.MILLISECONDS)

      result.set(future.handleAsync(
        new BiFunction[Void, Throwable, Unit] {
          override def apply(v: Void, t: Throwable): Unit = {
            if (null != t) {
              val errMsg = s"Exception while handling commitFiles for shuffleId: $shuffleKey"
              t match {
                case _: CancellationException =>
                  logWarning(s"$errMsg, operation was cancelled.")
                case ee: ExecutionException =>
                  logError(s"$errMsg, ExecutionException was raised.", ee)
                case ie: InterruptedException =>
                  logWarning(s"$errMsg, operation was interrupted.")
                  Thread.currentThread().interrupt()
                  throw ie
                case _: TimeoutException =>
                  logWarning(s"$errMsg, operation timed out after $shuffleCommitTimeout ms.")
                case throwable: Throwable =>
                  logError(s"$errMsg, an unexpected exception occurred.", throwable)
              }
              commitInfo.synchronized {
                commitInfo.response = CommitFilesResponse(
                  StatusCode.COMMIT_FILE_EXCEPTION,
                  List.empty.asJava,
                  List.empty.asJava,
                  primaryIds,
                  replicaIds)

                commitInfo.status = CommitInfo.COMMIT_FINISHED
              }

              workerSource.incCounter(WorkerSource.COMMIT_FILES_FAIL_COUNT)
            } else {
              // finish, cancel timeout job first.
              timeout.cancel()
              reply()
            }
          }
        },
        asyncReplyPool
      )) // should not use commitThreadPool in case of block by commit files.
    } else {
      // If both of two futures are null, then reply directly.
      reply()
    }
  }

  private def handleDestroy(
      context: RpcCallContext,
      shuffleKey: String,
      primaryLocations: jList[String],
      replicaLocations: jList[String],
      mockDestroyFailure: Boolean): Unit = {
    if (mockDestroyFailure) {
      context.reply(
        DestroyWorkerSlotsResponse(
          StatusCode.DESTROY_SLOTS_MOCK_FAILURE,
          primaryLocations,
          replicaLocations))
      return
    }
    // check whether shuffleKey has registered
    if (!partitionLocationInfo.containsShuffle(shuffleKey)) {
      logWarning(s"Shuffle $shuffleKey not registered!")
      context.reply(
        DestroyWorkerSlotsResponse(
          StatusCode.SHUFFLE_UNREGISTERED,
          primaryLocations,
          replicaLocations))
      return
    }
    if (log.isDebugEnabled()) {
      logDebug(
        s"[handleDestroy] primaryIds: ${primaryLocations.asScala}, replicaIds: ${replicaLocations.asScala}")
    }

    val failedPrimaries = new jArrayList[String]()
    val failedReplicas = new jArrayList[String]()

    // destroy primary locations
    if (primaryLocations != null && !primaryLocations.isEmpty) {
      primaryLocations.asScala.foreach { uniqueId =>
        try {
          storageManager.cleanFile(
            shuffleKey,
            PartitionLocation.getFileName(uniqueId, PartitionLocation.Mode.PRIMARY))
        } catch {
          case e: Exception =>
            failedPrimaries.add(uniqueId)
            logDebug(s"Destroy primary file $uniqueId for $shuffleKey failed.", e)
        }
      }
      // remove primary locations from WorkerInfo
      val releasePrimaryLocations =
        partitionLocationInfo.removePrimaryPartitions(shuffleKey, primaryLocations)
      workerInfo.releaseSlots(shuffleKey, releasePrimaryLocations._1)
    }
    // destroy replica locations
    if (replicaLocations != null && !replicaLocations.isEmpty) {
      replicaLocations.asScala.foreach { uniqueId =>
        try {
          storageManager.cleanFile(
            shuffleKey,
            PartitionLocation.getFileName(uniqueId, PartitionLocation.Mode.REPLICA))
        } catch {
          case e: Exception =>
            failedReplicas.add(uniqueId)
            logDebug(s"Destroy replica file $uniqueId for $shuffleKey failed.", e)
        }
      }
      // remove replica locations from worker info
      val releaseReplicaLocations =
        partitionLocationInfo.removeReplicaPartitions(shuffleKey, replicaLocations)
      workerInfo.releaseSlots(shuffleKey, releaseReplicaLocations._1)
    }
    // reply
    if (failedPrimaries.isEmpty && failedReplicas.isEmpty) {
      logInfo(
        s"Destroy ${primaryLocations.size()} primary location and ${replicaLocations.size()}" +
          s" replica locations for $shuffleKey successfully.")
      context.reply(
        DestroyWorkerSlotsResponse(
          StatusCode.SUCCESS,
          List.empty.asJava,
          List.empty.asJava))
    } else {
      logInfo(s"Destroy ${failedPrimaries.size()}/${primaryLocations.size()} primary location and" +
        s"${failedReplicas.size()}/${replicaLocations.size()} replica location for" +
        s" $shuffleKey PartialSuccess.")
      context.reply(
        DestroyWorkerSlotsResponse(
          StatusCode.PARTIAL_SUCCESS,
          failedPrimaries,
          failedReplicas))
    }
  }

  def checkCommitTimeout(shuffleCommitTime: ConcurrentHashMap[
    String,
    ConcurrentHashMap[Long, (Long, RpcCallContext)]]): Unit = {

    val currentTime = System.currentTimeMillis()
    val commitTimeIterator = shuffleCommitTime.entrySet().iterator()
    while (commitTimeIterator.hasNext) {
      val timeMapEntry = commitTimeIterator.next()
      val shuffleKey = timeMapEntry.getKey
      val epochWaitTimeMap = timeMapEntry.getValue
      val epochIterator = epochWaitTimeMap.entrySet().iterator()

      while (epochIterator.hasNext && shuffleCommitInfos.containsKey(shuffleKey)) {
        val epochWaitTimeEntry = epochIterator.next()
        val epoch = epochWaitTimeEntry.getKey
        val (commitStartWaitTime, context) = epochWaitTimeEntry.getValue
        try {
          val commitInfo = shuffleCommitInfos.get(shuffleKey).get(epoch)
          commitInfo.synchronized {
            if (commitInfo.status == CommitInfo.COMMIT_FINISHED) {
              context.reply(commitInfo.response)
              epochIterator.remove()
            } else {
              if (currentTime - commitStartWaitTime >= shuffleCommitTimeout) {
                val replyResponse = CommitFilesResponse(
                  StatusCode.COMMIT_FILE_EXCEPTION,
                  List.empty.asJava,
                  List.empty.asJava,
                  commitInfo.response.failedPrimaryIds,
                  commitInfo.response.failedReplicaIds)
                commitInfo.status = CommitInfo.COMMIT_FINISHED
                commitInfo.response = replyResponse
                context.reply(replyResponse)
                epochIterator.remove()

                workerSource.incCounter(WorkerSource.COMMIT_FILES_FAIL_COUNT)
              }
            }
          }
        } catch {
          case error: Exception =>
            epochIterator.remove()
            logWarning(
              s"Exception occurs when checkCommitTimeout for shuffleKey-epoch:$shuffleKey-$epoch, error: $error")
        }
      }
      if (!shuffleCommitInfos.containsKey(shuffleKey)) {
        logWarning(s"Shuffle $shuffleKey commit expired when checkCommitTimeout.")
        commitTimeIterator.remove()
      }
    }
  }

  private def updateShuffleMapperAttempts(
      mapAttempts: Array[Int],
      shuffleMapperAttempts: AtomicIntegerArray): Unit = {
    var mapIdx = 0
    val mapAttemptsLen = mapAttempts.length
    while (mapIdx < mapAttemptsLen) {
      if (mapAttempts(mapIdx) != -1) {
        shuffleMapperAttempts.synchronized {
          var idx = mapIdx
          val len = shuffleMapperAttempts.length()
          while (idx < len) {
            if (mapAttempts(idx) != -1 && shuffleMapperAttempts.get(idx) == -1) {
              shuffleMapperAttempts.set(idx, mapAttempts(idx))
            }
            idx += 1
          }
        }
        return
      }
      mapIdx += 1
    }
  }
}
