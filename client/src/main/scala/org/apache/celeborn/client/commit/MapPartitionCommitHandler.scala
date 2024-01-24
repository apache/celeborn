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

import java.nio.ByteBuffer
import java.util
import java.util.Collections
import java.util.concurrent.{Callable, ConcurrentHashMap, TimeUnit}
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.base.Preconditions.checkState
import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.celeborn.client.{ShuffleCommittedInfo, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.{ShuffleAllocatedWorkers, ShuffleFailedWorkers}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionType}
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.rpc.netty.{LocalNettyRpcCallContext, RemoteNettyRpcCallContext}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.common.util.JavaUtils
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
    shuffleAllocatedWorkers: ShuffleAllocatedWorkers,
    committedPartitionInfo: CommittedPartitionInfo,
    workerStatusTracker: WorkerStatusTracker,
    sharedRpcPool: ThreadPoolExecutor)
  extends CommitHandler(appId, conf, committedPartitionInfo, workerStatusTracker, sharedRpcPool)
  with Logging {

  private val shuffleSucceedPartitionIds = JavaUtils.newConcurrentHashMap[Int, util.Set[Integer]]()

  // shuffleId -> in processing partitionId set
  private val inProcessMapPartitionEndIds = JavaUtils.newConcurrentHashMap[Int, util.Set[Integer]]()

  private val isShuffleMapperEnd = ConcurrentHashMap.newKeySet[Int]()
  private val pendingGetLocationContexts =
    JavaUtils.newConcurrentHashMap[Int, util.Set[RpcCallContext]]()

  private val rpcCacheSize = conf.clientRpcCacheSize
  private val rpcCacheConcurrencyLevel = conf.clientRpcCacheConcurrencyLevel
  private val rpcCacheExpireTime = conf.clientRpcCacheExpireTime

  private val getReducerFileGroupRpcCache: Cache[Int, ByteBuffer] = CacheBuilder.newBuilder()
    .concurrencyLevel(rpcCacheConcurrencyLevel)
    .expireAfterWrite(rpcCacheExpireTime, TimeUnit.MILLISECONDS)
    .maximumSize(rpcCacheSize)
    .build().asInstanceOf[Cache[Int, ByteBuffer]]

  private val shuffleNumMappers = JavaUtils.newConcurrentHashMap[Int, Int]
  private val shuffleHasSegments = JavaUtils.newConcurrentHashMap[Int, Boolean]

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

  override def removeExpiredShuffle(shuffleId: Int): Unit = {
    inProcessMapPartitionEndIds.remove(shuffleId)
    shuffleSucceedPartitionIds.remove(shuffleId)
    isShuffleMapperEnd.remove(shuffleId)
    pendingGetLocationContexts.remove(shuffleId)
    shuffleNumMappers.remove(shuffleId)
    shuffleHasSegments.remove(shuffleId)
    super.removeExpiredShuffle(shuffleId)
  }

  private def handleFinalPartitionCommitFiles(
      shuffleId: Int,
      allocatedWorkers: util.Map[WorkerInfo, ShufflePartitionLocationInfo],
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
        shuffleHasSegments.get(shuffleId))
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
      recordWorkerFailure: ShuffleFailedWorkers => Unit): (Boolean, Boolean) = {
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

      logDebug(s"shuffle ${shuffleId} finished successfully, " +
        s"succeed partitions size is ${resultPartitions.size}.")
      pendingGetLocationContexts synchronized {
        isShuffleMapperEnd.add(shuffleId)
      }
      val requests = pendingGetLocationContexts.remove(shuffleId)
      if (requests != null && !requests.isEmpty) {
        requests.asScala.foreach(context => handleGetReducerFileGroup(context, shuffleId))
      }
    }

    (dataCommitSuccess, false)
  }

  override def registerShuffle(shuffleId: Int, numMappers: Int, hasSegments: Boolean): Unit = {
    super.registerShuffle(shuffleId, numMappers, hasSegments)
    pendingGetLocationContexts.computeIfAbsent(shuffleId, _ => new util.HashSet[RpcCallContext])
    shuffleNumMappers.put(shuffleId, numMappers)
    shuffleHasSegments.put(shuffleId, hasSegments)
  }

  override def shuffleRegistered(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation]): Unit = {
    if (!shuffleHasSegments.get(shuffleId)) {
      return
    }

    if (reducerFileGroupsMap.get(shuffleId).isEmpty) {
      for (i <- partitionLocations.indices) {
        val partitionLocationsSet = new util.HashSet[PartitionLocation]()
        partitionLocationsSet.add(partitionLocations(i))
        reducerFileGroupsMap.get(shuffleId).put(i, partitionLocationsSet)
      }
    }
    checkState(reducerFileGroupsMap.get(shuffleId).size() == shuffleNumMappers.get(shuffleId))
    pendingGetLocationContexts.synchronized {
      if (pendingGetLocationContexts.contains(shuffleId)) {
        val requests = pendingGetLocationContexts.remove(shuffleId)
        if (requests != null && !requests.isEmpty) {
          requests.asScala.foreach(handleGetReducerFileGroup(_, shuffleId))
        }
      }
    }
  }

  override def handleGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit = {
    if (isShuffleMapperEnd.contains(shuffleId)) {
      replyGetReducerFileGroup(context, shuffleId)
    } else {
      pendingGetLocationContexts.synchronized {
        if (isShuffleMapperEnd.contains(shuffleId)) {
          replyGetReducerFileGroup(context, shuffleId)
        } else {
          logWarning("Add to pending context for " + shuffleId)
          pendingGetLocationContexts.computeIfAbsent(
            shuffleId,
            _ => new util.HashSet[RpcCallContext]).add(context)
        }
      }
    }
  }

  private def replyGetReducerFileGroup(context: RpcCallContext, shuffleId: Int): Unit = {
    // we need obtain the last succeed partitionIds
    val lastSucceedPartitionIds =
      shuffleSucceedPartitionIds.getOrDefault(shuffleId, new util.HashSet[Integer]())
    val fileGroups = reducerFileGroupsMap.getOrDefault(shuffleId, JavaUtils.newConcurrentHashMap());
    val succeedPartitionIds =
      if (shuffleHasSegments.get(shuffleId)) fileGroups.keySet()
      else new util.HashSet[Integer](lastSucceedPartitionIds)

    if (context.isInstanceOf[LocalNettyRpcCallContext]) {
      context.reply(GetReducerFileGroupResponse(
        StatusCode.SUCCESS,
        fileGroups,
        getMapperAttempts(shuffleId),
        succeedPartitionIds))
    } else {
      val cachedMsg = getReducerFileGroupRpcCache.get(
        shuffleId,
        new Callable[ByteBuffer]() {
          override def call(): ByteBuffer = {
            val returnedMsg = GetReducerFileGroupResponse(
              StatusCode.SUCCESS,
              fileGroups,
              getMapperAttempts(shuffleId),
              succeedPartitionIds)
            context.asInstanceOf[RemoteNettyRpcCallContext].nettyEnv.serialize(returnedMsg)
          }
        })
      context.asInstanceOf[RemoteNettyRpcCallContext].callback.onSuccess(cachedMsg)
    }
  }

  override def releasePartitionResource(shuffleId: Int, partitionId: Int): Unit = {
    val succeedPartitionIds = shuffleSucceedPartitionIds.get(shuffleId)
    if (succeedPartitionIds != null) {
      succeedPartitionIds.remove(partitionId)
    }

    super.releasePartitionResource(shuffleId, partitionId)
  }
}
