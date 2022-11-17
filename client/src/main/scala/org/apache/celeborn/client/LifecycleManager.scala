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

import java.nio.ByteBuffer
import java.util
import java.util.{function, List => JList, Map}
import java.util.concurrent.{Callable, ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import com.google.common.annotations.VisibleForTesting
import com.google.common.cache.{Cache, CacheBuilder}
import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.haclient.RssHARetryClient
import org.apache.celeborn.common.identity.{IdentityProvider, UserIdentifier}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{PartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.RpcNameConstants.WORKER_EP
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.rpc.netty.{LocalNettyRpcCallContext, RemoteNettyRpcCallContext}
import org.apache.celeborn.common.util.{PbSerDeUtils, ThreadUtils, Utils}

class LifecycleManager(appId: String, val conf: CelebornConf) extends RpcEndpoint with Logging {

  private val lifecycleHost = Utils.localHostName

  private val shuffleExpiredCheckIntervalMs = conf.shuffleExpiredCheckIntervalMs
  private val workerExcludedCheckIntervalMs = conf.workerExcludedCheckIntervalMs
  private val workerExcludedExpireTimeout = conf.workerExcludedExpireTimeout
  private val pushReplicateEnabled = conf.pushReplicateEnabled
  private val partitionSplitThreshold = conf.partitionSplitThreshold
  private val partitionSplitMode = conf.partitionSplitMode
  // shuffle id -> partition type
  private val shufflePartitionType = new ConcurrentHashMap[Int, PartitionType]()
  private val rangeReadFilter = conf.shuffleRangeReadFilterEnabled
  private val unregisterShuffleTime = new ConcurrentHashMap[Int, Long]()
  private val stageEndTimeout = conf.pushStageEndTimeout
  private val rpcCacheSize = conf.rpcCacheSize
  private val rpcCacheConcurrencyLevel = conf.rpcCacheConcurrencyLevel
  private val rpcCacheExpireTime = conf.rpcCacheExpireTime

  private val registeredShuffle = ConcurrentHashMap.newKeySet[Int]()
  private val shuffleMapperAttempts = new ConcurrentHashMap[Int, Array[Int]]()
  private val reducerFileGroupsMap =
    new ConcurrentHashMap[Int, Array[Array[PartitionLocation]]]()
  private val dataLostShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val stageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  private val inProcessStageEndShuffleSet = ConcurrentHashMap.newKeySet[Int]()
  // maintain each shuffle's map relation of WorkerInfo and partition location
  private val shuffleAllocatedWorkers = {
    new ConcurrentHashMap[Int, ConcurrentHashMap[WorkerInfo, PartitionLocationInfo]]()
  }
  // shuffle id -> (partitionId -> newest PartitionLocation)
  private val latestPartitionLocation =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Int, PartitionLocation]]()
  private val userIdentifier: UserIdentifier = IdentityProvider.instantiate(conf).provide()
  // noinspection UnstableApiUsage
  private val getReducerFileGroupRpcCache: Cache[Int, ByteBuffer] = CacheBuilder.newBuilder()
    .concurrencyLevel(rpcCacheConcurrencyLevel)
    .expireAfterWrite(rpcCacheExpireTime, TimeUnit.MILLISECONDS)
    .maximumSize(rpcCacheSize)
    .build().asInstanceOf[Cache[Int, ByteBuffer]]

  @VisibleForTesting
  def workerSnapshots(shuffleId: Int): util.Map[WorkerInfo, PartitionLocationInfo] =
    shuffleAllocatedWorkers.get(shuffleId)

  val newMapFunc: function.Function[Int, ConcurrentHashMap[Int, PartitionLocation]] =
    new util.function.Function[Int, ConcurrentHashMap[Int, PartitionLocation]]() {
      override def apply(s: Int): ConcurrentHashMap[Int, PartitionLocation] = {
        new ConcurrentHashMap[Int, PartitionLocation]()
      }
    }

  private def updateLatestPartitionLocations(
      shuffleId: Int,
      locations: util.List[PartitionLocation]): Unit = {
    val map = latestPartitionLocation.computeIfAbsent(shuffleId, newMapFunc)
    locations.asScala.foreach(location => map.put(location.getId, location))
  }

  case class ChangePartitionRequest(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      causes: Option[StatusCode])

  // shuffleId -> (partitionId -> set of ChangePartition)
  private val changePartitionRequests =
    new ConcurrentHashMap[Int, ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]]]()
  // shuffleId -> set of partition id
  private val inBatchPartitions = new ConcurrentHashMap[Int, util.Set[Integer]]()

  // register shuffle request waiting for response
  private val registeringShuffleRequest = new ConcurrentHashMap[Int, util.Set[RpcCallContext]]()

  // blacklist
  private val blacklist = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForShuffleRemoval: ScheduledFuture[_] = _
  private var getBlacklist: ScheduledFuture[_] = _

  // Use independent app heartbeat threads to avoid being blocked by other operations.
  private val appHeartbeatIntervalMs = conf.appHeartbeatIntervalMs
  private val appHeartbeatHandlerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("app-heartbeat")
  private var appHeartbeat: ScheduledFuture[_] = _
  private val responseCheckerThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("rss-master-resp-checker")

  private val batchHandleChangePartitionEnabled = conf.batchHandleChangePartitionEnabled
  private val batchHandleChangePartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "rss-lifecycle-manager-change-partition-executor",
    conf.batchHandleChangePartitionNumThreads)
  private val batchHandleChangePartitionRequestInterval =
    conf.batchHandleChangePartitionRequestInterval
  private val batchHandleChangePartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleChangePartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "rss-lifecycle-manager-change-partition-scheduler"))
    } else {
      None
    }

  // init driver rss meta rpc service
  override val rpcEnv: RpcEnv = RpcEnv.create(
    RpcNameConstants.RSS_METASERVICE_SYS,
    lifecycleHost,
    conf.shuffleManagerPort,
    conf)
  rpcEnv.setupEndpoint(RpcNameConstants.RSS_METASERVICE_EP, this)

  logInfo(s"Starting LifecycleManager on ${rpcEnv.address}")

  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)
  private val totalWritten = new LongAdder
  private val fileCount = new LongAdder

  // Since method `onStart` is executed when `rpcEnv.setupEndpoint` is executed, and
  // `rssHARetryClient` is initialized after `rpcEnv` is initialized, if method `onStart` contains
  // a reference to `rssHARetryClient`, there may be cases where `rssHARetryClient` is null when
  // `rssHARetryClient` is called. Therefore, it's necessary to uniformly execute the initialization
  // method at the end of the construction of the class to perform the initialization operations.
  private def initialize(): Unit = {
    // noinspection ConvertExpressionToSAM
    appHeartbeat = appHeartbeatHandlerThread.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            require(rssHARetryClient != null, "When sending a heartbeat, client shouldn't be null.")
            val tmpTotalWritten = totalWritten.sumThenReset()
            val tmpFileCount = fileCount.sumThenReset()
            logDebug(s"Send app heartbeat with $tmpTotalWritten $tmpFileCount")
            val appHeartbeat =
              HeartbeatFromApplication(appId, tmpTotalWritten, tmpFileCount, ZERO_UUID)
            rssHARetryClient.send(appHeartbeat)
            logDebug("Successfully send app heartbeat.")
          } catch {
            case it: InterruptedException =>
              logWarning("Interrupted while sending app heartbeat.")
              Thread.currentThread().interrupt()
              throw it
            case t: Throwable =>
              logError("Error while send heartbeat", t)
          }
        }
      },
      0,
      appHeartbeatIntervalMs,
      TimeUnit.MILLISECONDS)

    batchHandleChangePartitionSchedulerThread.foreach {
      // noinspection ConvertExpressionToSAM
      _.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit = {
            try {
              changePartitionRequests.asScala.foreach { case (shuffleId, requests) =>
                requests.synchronized {
                  batchHandleChangePartitionExecutors.submit {
                    new Runnable {
                      override def run(): Unit = {
                        // For each partition only need handle one request
                        val distinctPartitions = requests.asScala.filter { case (partitionId, _) =>
                          !inBatchPartitions.get(shuffleId).contains(partitionId)
                        }.map { case (partitionId, request) =>
                          inBatchPartitions.get(shuffleId).add(partitionId)
                          request.asScala.toArray.maxBy(_.epoch)
                        }.toArray
                        if (distinctPartitions.nonEmpty) {
                          batchHandleChangePartitions(
                            distinctPartitions.head.applicationId,
                            shuffleId,
                            distinctPartitions)
                        }
                      }
                    }
                  }
                }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
            }
          }
        },
        0,
        batchHandleChangePartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  override def onStart(): Unit = {
    // noinspection ConvertExpressionToSAM
    checkForShuffleRemoval = forwardMessageThread.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(RemoveExpiredShuffle)
        }
      },
      shuffleExpiredCheckIntervalMs,
      shuffleExpiredCheckIntervalMs,
      TimeUnit.MILLISECONDS)

    // noinspection ConvertExpressionToSAM
    getBlacklist = forwardMessageThread.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          self.send(GetBlacklist(blacklist.asScala.keys.toList.asJava))
        }
      },
      workerExcludedCheckIntervalMs,
      workerExcludedCheckIntervalMs,
      TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    import scala.concurrent.duration._

    checkForShuffleRemoval.cancel(true)
    getBlacklist.cancel(true)
    ThreadUtils.shutdown(forwardMessageThread, 800.millis)

    appHeartbeat.cancel(true)
    ThreadUtils.shutdown(appHeartbeatHandlerThread, 800.millis)

    ThreadUtils.shutdown(responseCheckerThread, 800.millis)

    rssHARetryClient.close()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()
    }
  }

  def getUserIdentifier: UserIdentifier = {
    userIdentifier
  }

  def getRssMetaServiceHost: String = {
    lifecycleHost
  }

  def getRssMetaServicePort: Int = {
    rpcEnv.address.port
  }

  def getPartitionType(shuffleId: Int): PartitionType = {
    shufflePartitionType.getOrDefault(shuffleId, conf.shufflePartitionType)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExpiredShuffle =>
      removeExpiredShuffle()
    case msg: GetBlacklist =>
      handleGetBlacklist(msg)
    case StageEnd(applicationId, shuffleId) =>
      logInfo(s"Received StageEnd request, ${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleStageEnd(applicationId, shuffleId)
    case pb: PbUnregisterShuffle =>
      val applicationId = pb.getAppId
      val shuffleId = pb.getShuffleId
      logDebug(s"Received UnregisterShuffle request," +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleUnregisterShuffle(applicationId, shuffleId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case pb: PbRegisterShuffle =>
      val applicationId = pb.getApplicationId
      val shuffleId = pb.getShuffleId
      val numMappers = pb.getNumMapppers
      val numPartitions = pb.getNumPartitions
      logDebug(s"Received RegisterShuffle request, " +
        s"$applicationId, $shuffleId, $numMappers, $numPartitions.")
      handleRegisterShuffle(context, applicationId, shuffleId, numMappers, numPartitions)

    case pb: PbRegisterMapPartitionTask =>
      val applicationId = pb.getApplicationId
      val shuffleId = pb.getShuffleId
      val numMappers = pb.getNumMappers
      val mapId = pb.getMapId
      val attemptId = pb.getAttemptId
      val partitionId = pb.getPartitionId
      logDebug(s"Received Register map partition task request, " +
        s"$applicationId, $shuffleId, $numMappers, $mapId, $attemptId, $partitionId.")
      handleRegisterMapPartitionTask(
        context,
        applicationId,
        shuffleId,
        numMappers,
        partitionId)

    case pb: PbRevive =>
      val applicationId = pb.getApplicationId
      val shuffleId = pb.getShuffleId
      val mapId = pb.getMapId
      val attemptId = pb.getAttemptId
      val partitionId = pb.getPartitionId
      val epoch = pb.getEpoch
      val oldPartition = PbSerDeUtils.fromPbPartitionLocation(pb.getOldPartition)
      val cause = Utils.toStatusCode(pb.getStatus)
      logTrace(s"Received Revive request, " +
        s"$applicationId, $shuffleId, $mapId, $attemptId, ,$partitionId," +
        s" $epoch, $oldPartition, $cause.")
      handleRevive(
        context,
        applicationId,
        shuffleId,
        mapId,
        attemptId,
        partitionId,
        epoch,
        oldPartition,
        cause)

    case pb: PbPartitionSplit =>
      val applicationId = pb.getApplicationId
      val shuffleId = pb.getShuffleId
      val partitionId = pb.getPartitionId
      val epoch = pb.getEpoch
      val oldPartition = PbSerDeUtils.fromPbPartitionLocation(pb.getOldPartition)
      logTrace(s"Received split request, " +
        s"$applicationId, $shuffleId, $partitionId, $epoch, $oldPartition")
      handleChangePartitionLocation(
        context,
        applicationId,
        shuffleId,
        partitionId,
        epoch,
        oldPartition)

    case MapperEnd(applicationId, shuffleId, mapId, attemptId, numMappers) =>
      logTrace(s"Received MapperEnd request, " +
        s"${Utils.makeMapKey(applicationId, shuffleId, mapId, attemptId)}.")
      handleMapperEnd(context, applicationId, shuffleId, mapId, attemptId, numMappers)

    case GetReducerFileGroup(applicationId: String, shuffleId: Int) =>
      logDebug(s"Received GetShuffleFileGroup request," +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      handleGetReducerFileGroup(context, shuffleId)
  }

  /* ========================================================== *
   |        START OF EVENT HANDLER                              |
   * ========================================================== */

  private def handleRegisterShuffle(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numReducers: Int): Unit = {
    handleOfferAndReserveSlots(context, applicationId, shuffleId, numMappers, numReducers)
  }

  private def handleRegisterMapPartitionTask(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      partitionId: Int): Unit = {
    shufflePartitionType.putIfAbsent(shuffleId, PartitionType.MAP)
    handleOfferAndReserveSlots(
      context,
      applicationId,
      shuffleId,
      numMappers,
      numMappers,
      partitionId)
  }

  private def handleOfferAndReserveSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      numMappers: Int,
      numReducers: Int,
      partitionId: Int = -1): Unit = {
    val partitionType = getPartitionType(shuffleId)
    registeringShuffleRequest.synchronized {
      if (registeringShuffleRequest.containsKey(shuffleId)) {
        // If same request already exists in the registering request list for the same shuffle,
        // just register and return.
        logDebug("[handleRegisterShuffle] request for same shuffleKey exists, just register")
        registeringShuffleRequest.get(shuffleId).add(context)
        return
      } else {
        // If shuffle is registered, reply this shuffle's partition location and return.
        // Else add this request to registeringShuffleRequest.
        if (registeredShuffle.contains(shuffleId)) {
          val initialLocs = workerSnapshots(shuffleId)
            .values()
            .asScala
            .flatMap(_.getAllMasterLocationsWithMinEpoch(shuffleId.toString).asScala)
            .filter(p =>
              (partitionType == PartitionType.REDUCE && p.getEpoch == 0) || (partitionType == PartitionType.MAP && p.getId == partitionId))
            .toArray
          context.reply(RegisterShuffleResponse(StatusCode.SUCCESS, initialLocs))
          return
        }
        logInfo(s"New shuffle request, shuffleId $shuffleId, partitionType: $partitionType " +
          s"numMappers: $numMappers, numReducers: $numReducers.")
        val set = new util.HashSet[RpcCallContext]()
        set.add(context)
        registeringShuffleRequest.put(shuffleId, set)
      }
    }

    // Reply to all RegisterShuffle request for current shuffle id.
    def reply(response: PbRegisterShuffleResponse): Unit = {
      registeringShuffleRequest.synchronized {
        registeringShuffleRequest.asScala
          .get(shuffleId)
          .foreach(_.asScala.foreach(_.reply(response)))
        registeringShuffleRequest.remove(shuffleId)
      }
    }

    // First, request to get allocated slots from Master
    val ids = new util.ArrayList[Integer]
    val numPartitions: Int = partitionType match {
      case PartitionType.REDUCE => numReducers
      case PartitionType.MAP => numMappers
    }
    (0 until numPartitions).foreach(idx => ids.add(new Integer(idx)))
    val res = requestSlotsWithRetry(applicationId, shuffleId, ids)

    res.status match {
      case StatusCode.FAILED =>
        logError(s"OfferSlots RPC request failed for $shuffleId!")
        reply(RegisterShuffleResponse(StatusCode.FAILED, Array.empty))
        return
      case StatusCode.SLOT_NOT_AVAILABLE =>
        logError(s"OfferSlots for $shuffleId failed!")
        reply(RegisterShuffleResponse(StatusCode.SLOT_NOT_AVAILABLE, Array.empty))
        return
      case StatusCode.SUCCESS =>
        logInfo(s"OfferSlots for ${Utils.makeShuffleKey(applicationId, shuffleId)} Success!")
        logDebug(s" Slots Info: ${res.workerResource}")
      case _ => // won't happen
        throw new UnsupportedOperationException()
    }

    // Reserve slots for each PartitionLocation. When response status is SUCCESS, WorkerResource
    // won't be empty since master will reply SlotNotAvailable status when reserved slots is empty.
    val slots = res.workerResource
    val candidatesWorkers = new util.HashSet(slots.keySet())
    val connectFailedWorkers = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()

    // Second, for each worker, try to initialize the endpoint.
    val parallelism = Math.min(Math.max(1, slots.size()), conf.rpcMaxParallelism)
    ThreadUtils.parmap(slots.asScala.to, "InitWorkerRef", parallelism) { case (workerInfo, _) =>
      try {
        workerInfo.endpoint =
          rpcEnv.setupEndpointRef(RpcAddress.apply(workerInfo.host, workerInfo.rpcPort), WORKER_EP)
      } catch {
        case t: Throwable =>
          logError(s"Init rpc client for $workerInfo failed", t)
          connectFailedWorkers.put(
            workerInfo,
            (StatusCode.UNKNOWN_WORKER, System.currentTimeMillis()))
      }
    }

    candidatesWorkers.removeAll(connectFailedWorkers.asScala.keys.toList.asJava)
    recordWorkerFailure(connectFailedWorkers)

    // Third, for each slot, LifecycleManager should ask Worker to reserve the slot
    // and prepare the pushing data env.
    val reserveSlotsSuccess =
      reserveSlotsWithRetry(
        applicationId,
        shuffleId,
        candidatesWorkers,
        slots,
        updateEpoch = false)

    // If reserve slots failed, clear allocated resources, reply ReserveSlotFailed and return.
    if (!reserveSlotsSuccess) {
      logError(s"reserve buffer for $shuffleId failed, reply to all.")
      reply(RegisterShuffleResponse(StatusCode.RESERVE_SLOTS_FAILED, Array.empty))
      // tell Master to release slots
      requestReleaseSlots(
        rssHARetryClient,
        ReleaseSlots(applicationId, shuffleId, List.empty.asJava, List.empty.asJava))
    } else {
      logInfo(s"ReserveSlots for ${Utils.makeShuffleKey(applicationId, shuffleId)} success!")
      logDebug(s"Allocated Slots: $slots")
      // Forth, register shuffle success, update status
      val allocatedWorkers = new ConcurrentHashMap[WorkerInfo, PartitionLocationInfo]()
      slots.asScala.foreach { case (workerInfo, (masterLocations, slaveLocations)) =>
        val partitionLocationInfo = new PartitionLocationInfo()
        partitionLocationInfo.addMasterPartitions(shuffleId.toString, masterLocations)
        updateLatestPartitionLocations(shuffleId, masterLocations)
        partitionLocationInfo.addSlavePartitions(shuffleId.toString, slaveLocations)
        allocatedWorkers.put(workerInfo, partitionLocationInfo)
      }
      shuffleAllocatedWorkers.put(shuffleId, allocatedWorkers)
      registeredShuffle.add(shuffleId)

      shuffleMapperAttempts.synchronized {
        if (!shuffleMapperAttempts.containsKey(shuffleId)) {
          val attempts = new Array[Int](numMappers)
          0 until numMappers foreach (idx => attempts(idx) = -1)
          shuffleMapperAttempts.synchronized {
            shuffleMapperAttempts.put(shuffleId, attempts)
          }
        }
      }

      reducerFileGroupsMap.put(shuffleId, new Array[Array[PartitionLocation]](numReducers))

      // Fifth, reply the allocated partition location to ShuffleClient.
      logInfo(s"Handle RegisterShuffle Success for $shuffleId.")
      val allMasterPartitionLocations = slots.asScala.flatMap(_._2._1.asScala).filter(p =>
        partitionType ==
          PartitionType.REDUCE || (partitionType == PartitionType.MAP && p.getId == partitionId)).toArray
      reply(RegisterShuffleResponse(StatusCode.SUCCESS, allMasterPartitionLocations))
    }
  }

  private def blacklistPartition(
      shuffleId: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode): Unit = {
    // only blacklist if cause is PushDataFailMain
    val failedWorker = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
    if (cause == StatusCode.PUSH_DATA_FAIL_MASTER && oldPartition != null) {
      val tmpWorker = oldPartition.getWorker
      val worker = workerSnapshots(shuffleId).keySet().asScala
        .find(_.equals(tmpWorker))
      if (worker.isDefined) {
        failedWorker.put(worker.get, (StatusCode.PUSH_DATA_FAIL_MASTER, System.currentTimeMillis()))
      }
    }
    if (!failedWorker.isEmpty) {
      recordWorkerFailure(failedWorker)
    }
  }

  private def handleRevive(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: StatusCode): Unit = {
    // If shuffle not registered, reply ShuffleNotRegistered and return
    if (!registeredShuffle.contains(shuffleId)) {
      logError(s"[handleRevive] shuffle $shuffleId not registered!")
      context.reply(ChangeLocationResponse(StatusCode.SHUFFLE_NOT_REGISTERED, None))
      return
    }

    // If shuffle registered and corresponding map finished, reply MapEnd and return.
    if (shuffleMapperAttempts.containsKey(shuffleId)
      && shuffleMapperAttempts.get(shuffleId)(mapId) != -1) {
      logWarning(s"[handleRevive] Mapper ended, mapId $mapId, current attemptId $attemptId, " +
        s"ended attemptId ${shuffleMapperAttempts.get(shuffleId)(mapId)}, shuffleId $shuffleId.")
      context.reply(ChangeLocationResponse(StatusCode.MAP_ENDED, None))
      return
    }

    logWarning(s"Do Revive for shuffle ${Utils.makeShuffleKey(applicationId, shuffleId)}, " +
      s"oldPartition: $oldPartition, cause: $cause")

    handleChangePartitionLocation(
      context,
      applicationId,
      shuffleId,
      partitionId,
      oldEpoch,
      oldPartition,
      Some(cause))
  }

  private val rpcContextRegisterFunc =
    new util.function.Function[
      Int,
      ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]]]() {
      override def apply(s: Int): ConcurrentHashMap[Integer, util.Set[ChangePartitionRequest]] =
        new ConcurrentHashMap()
    }

  private val inBatchShuffleIdRegisterFunc = new util.function.Function[Int, util.Set[Integer]]() {
    override def apply(s: Int): util.Set[Integer] = new util.HashSet[Integer]()
  }

  private def handleChangePartitionLocation(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode] = None): Unit = {

    val changePartition = ChangePartitionRequest(
      context,
      applicationId,
      shuffleId,
      partitionId,
      oldEpoch,
      oldPartition,
      cause)
    // check if there exists request for the partition, if do just register
    val requests = changePartitionRequests.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    inBatchPartitions.computeIfAbsent(shuffleId, inBatchShuffleIdRegisterFunc)
    requests.synchronized {
      if (requests.containsKey(partitionId)) {
        requests.get(partitionId).add(changePartition)
        logTrace(s"[handleChangePartitionLocation] For $shuffleId, request for same partition" +
          s"$partitionId-$oldEpoch exists, register context.")
        return
      } else {
        // If new slot for the partition has been allocated, reply and return.
        // Else register and allocate for it.
        getLatestPartition(shuffleId, partitionId, oldEpoch).foreach { latestLoc =>
          context.reply(ChangeLocationResponse(StatusCode.SUCCESS, Some(latestLoc)))
          logDebug(s"New partition found, old partition $partitionId-$oldEpoch return it." +
            s" shuffleId: $shuffleId $latestLoc")
          return
        }
        val set = new util.HashSet[ChangePartitionRequest]()
        set.add(changePartition)
        requests.put(partitionId, set)
      }
    }
    if (!batchHandleChangePartitionEnabled) {
      batchHandleChangePartitions(applicationId, shuffleId, Array(changePartition))
    }
  }

  def batchHandleChangePartitions(
      applicationId: String,
      shuffleId: Int,
      changePartitions: Array[ChangePartitionRequest]): Unit = {
    val requestsMap = changePartitionRequests.get(shuffleId)

    val changes = changePartitions.map { change =>
      s"${change.shuffleId}-${change.partitionId}-${change.epoch}"
    }.mkString("[", ",", "]")
    logWarning(s"Batch handle change partition for $applicationId of $changes")

    // Blacklist all failed workers
    if (changePartitions.exists(_.causes.isDefined)) {
      changePartitions.filter(_.causes.isDefined).foreach { changePartition =>
        blacklistPartition(shuffleId, changePartition.oldPartition, changePartition.causes.get)
      }
    }

    // remove together to reduce lock time
    def replySuccess(locations: Array[PartitionLocation]): Unit = {
      requestsMap.synchronized {
        locations.map { location =>
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(location.getId)
          }
          // Here one partition id can be remove more than once,
          // so need to filter null result before reply.
          location -> Option(requestsMap.remove(location.getId))
        }
      }.foreach { case (newLocation, requests) =>
        val response = ChangeLocationResponse(StatusCode.SUCCESS, Option(newLocation))
        requests.map(_.asScala.toList.foreach(_.context.reply(response)))
      }
    }

    // remove together to reduce lock time
    def replyFailure(response: PbChangeLocationResponse): Unit = {
      requestsMap.synchronized {
        changePartitions.map { changePartition =>
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(changePartition.partitionId)
          }
          Option(requestsMap.remove(changePartition.partitionId))
        }
      }.foreach { requests =>
        requests.map(_.asScala.toList.foreach(_.context.reply(response)))
      }
    }

    val candidates = workersNotBlacklisted(shuffleId)
    if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)) {
      logError("[Update partition] failed for not enough candidates for revive.")
      replyFailure(ChangeLocationResponse(StatusCode.SLOT_NOT_AVAILABLE, None))
      return
    }

    // PartitionSplit all contains oldPartition
    val newlyAllocatedLocations =
      reallocateChangePartitionRequestSlotsFromCandidates(changePartitions.toList, candidates)

    if (!registeredShuffle.contains(shuffleId)) {
      logError(s"[handleChangePartition] shuffle $shuffleId not registered!")
      replyFailure(ChangeLocationResponse(StatusCode.SHUFFLE_NOT_REGISTERED, None))
      return
    }

    if (stageEndShuffleSet.contains(shuffleId)) {
      logError(s"[handleChangePartition] shuffle $shuffleId already ended!")
      replyFailure(ChangeLocationResponse(StatusCode.STAGE_ENDED, None))
      return
    }

    if (!reserveSlotsWithRetry(
        applicationId,
        shuffleId,
        new util.HashSet(candidates.toSet.asJava),
        newlyAllocatedLocations)) {
      logError(s"[Update partition] failed for $shuffleId.")
      replyFailure(ChangeLocationResponse(StatusCode.RESERVE_SLOTS_FAILED, None))
      return
    }

    val newMasterLocations =
      newlyAllocatedLocations.asScala.flatMap {
        case (workInfo, (masterLocations, slaveLocations)) =>
          // Add all re-allocated slots to worker snapshots.
          workerSnapshots(shuffleId).asScala.get(workInfo).foreach { partitionLocationInfo =>
            partitionLocationInfo.addMasterPartitions(shuffleId.toString, masterLocations)
            updateLatestPartitionLocations(shuffleId, masterLocations)
            partitionLocationInfo.addSlavePartitions(shuffleId.toString, slaveLocations)
          }
          // partition location can be null when call reserveSlotsWithRetry().
          val locations = (masterLocations.asScala ++ slaveLocations.asScala.map(_.getPeer))
            .distinct.filter(_ != null)
          if (locations.nonEmpty) {
            val changes = locations.map { partition =>
              s"(partition ${partition.getId} epoch from ${partition.getEpoch - 1} to ${partition.getEpoch})"
            }.mkString("[", ", ", "]")
            logDebug(s"[Update partition] success for " +
              s"shuffle ${Utils.makeShuffleKey(applicationId, shuffleId)}, succeed partitions: " +
              s"$changes.")
          }
          locations
      }
    replySuccess(newMasterLocations.toArray)
  }

  private def getLatestPartition(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int): Option[PartitionLocation] = {
    val map = latestPartitionLocation.get(shuffleId)
    if (map != null) {
      val loc = map.get(partitionId)
      if (loc != null && loc.getEpoch > epoch) {
        return Some(loc)
      }
    }
    None
  }

  private def handleMapperEnd(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int): Unit = {
    var askStageEnd: Boolean = false
    // update max attemptId
    shuffleMapperAttempts.synchronized {
      var attempts = shuffleMapperAttempts.get(shuffleId)
      // it would happen when task with no shuffle data called MapperEnd first
      if (attempts == null) {
        logDebug(s"[handleMapperEnd] $shuffleId not registered, create one.")
        attempts = new Array[Int](numMappers)
        0 until numMappers foreach (idx => attempts(idx) = -1)
        shuffleMapperAttempts.put(shuffleId, attempts)
      }

      if (attempts(mapId) < 0) {
        attempts(mapId) = attemptId
      } else {
        // Mapper with another attemptId called, skip this request
        context.reply(MapperEndResponse(StatusCode.SUCCESS))
        return
      }

      if (!attempts.exists(_ < 0)) {
        askStageEnd = true
      }
    }

    if (askStageEnd) {
      // last mapper finished. call mapper end
      logInfo(s"Last MapperEnd, call StageEnd with shuffleKey:" +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}.")
      self.send(StageEnd(applicationId, shuffleId))
    }

    // reply success
    context.reply(MapperEndResponse(StatusCode.SUCCESS))
  }

  private def handleGetReducerFileGroup(
      context: RpcCallContext,
      shuffleId: Int): Unit = {
    var timeout = stageEndTimeout
    val delta = 100
    while (!stageEndShuffleSet.contains(shuffleId)) {
      Thread.sleep(delta)
      if (timeout <= 0) {
        logError(s"[handleGetReducerFileGroup] Wait for handleStageEnd Timeout! $shuffleId.")
        context.reply(
          GetReducerFileGroupResponse(StatusCode.STAGE_END_TIME_OUT, Array.empty, Array.empty))
        return
      }
      timeout = timeout - delta
    }
    logDebug("[handleGetReducerFileGroup] Wait for handleStageEnd complete cost" +
      s" ${stageEndTimeout - timeout}ms")

    if (dataLostShuffleSet.contains(shuffleId)) {
      context.reply(
        GetReducerFileGroupResponse(StatusCode.SHUFFLE_DATA_LOST, Array.empty, Array.empty))
    } else {
      if (context.isInstanceOf[LocalNettyRpcCallContext]) {
        // This branch is for the UTs
        context.reply(GetReducerFileGroupResponse(
          StatusCode.SUCCESS,
          reducerFileGroupsMap.getOrDefault(shuffleId, Array.empty),
          shuffleMapperAttempts.getOrDefault(shuffleId, Array.empty)))
      } else {
        val cachedMsg = getReducerFileGroupRpcCache.get(
          shuffleId,
          new Callable[ByteBuffer]() {
            override def call(): ByteBuffer = {
              val returnedMsg = GetReducerFileGroupResponse(
                StatusCode.SUCCESS,
                reducerFileGroupsMap.getOrDefault(shuffleId, Array.empty),
                shuffleMapperAttempts.getOrDefault(shuffleId, Array.empty))
              context.asInstanceOf[RemoteNettyRpcCallContext].nettyEnv.serialize(returnedMsg)
            }
          })
        context.asInstanceOf[RemoteNettyRpcCallContext].callback.onSuccess(cachedMsg)
      }
    }
  }

  private def handleStageEnd(applicationId: String, shuffleId: Int): Unit = {
    // check whether shuffle has registered
    if (!registeredShuffle.contains(shuffleId)) {
      logInfo(s"[handleStageEnd]" +
        s"$shuffleId not registered, maybe no shuffle data within this stage.")
      // record in stageEndShuffleSet
      stageEndShuffleSet.add(shuffleId)
      return
    }
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
    val committedMasterIds = ConcurrentHashMap.newKeySet[String]()
    val committedSlaveIds = ConcurrentHashMap.newKeySet[String]()
    val committedMasterStorageInfos = new ConcurrentHashMap[String, StorageInfo]()
    val committedSlaveStorageInfos = new ConcurrentHashMap[String, StorageInfo]()
    val committedMapIdBitmap = new ConcurrentHashMap[String, RoaringBitmap]()
    val failedMasterPartitionIds = new ConcurrentHashMap[String, WorkerInfo]()
    val failedSlavePartitionIds = new ConcurrentHashMap[String, WorkerInfo]()

    val allocatedWorkers = shuffleAllocatedWorkers.get(shuffleId)
    val commitFilesFailedWorkers = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()

    val currentShuffleFileCount = new LongAdder
    val commitFileStartTime = System.nanoTime()

    val parallelism = Math.min(workerSnapshots(shuffleId).size(), conf.rpcMaxParallelism)
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

        val masterIds = masterParts.asScala.map(_.getUniqueId).asJava
        val slaveIds = slaveParts.asScala.map(_.getUniqueId).asJava

        val commitFiles = CommitFiles(
          applicationId,
          shuffleId,
          masterIds,
          slaveIds,
          shuffleMapperAttempts.get(shuffleId))
        val res = requestCommitFilesWithRetry(worker.endpoint, commitFiles)

        res.status match {
          case StatusCode.SUCCESS => // do nothing
          case StatusCode.PARTIAL_SUCCESS | StatusCode.SHUFFLE_NOT_REGISTERED | StatusCode.FAILED =>
            logDebug(s"Request $commitFiles return ${res.status} for " +
              s"${Utils.makeShuffleKey(applicationId, shuffleId)}")
            commitFilesFailedWorkers.put(worker, (res.status, System.currentTimeMillis()))
          case _ => // won't happen
        }

        // record committed partitionIds
        committedMasterIds.addAll(res.committedMasterIds)
        committedSlaveIds.addAll(res.committedSlaveIds)

        // record committed partitions storage hint and disk hint
        committedMasterStorageInfos.putAll(res.committedMasterStorageInfos)
        committedSlaveStorageInfos.putAll(res.committedSlaveStorageInfos)

        // record failed partitions
        failedMasterPartitionIds.putAll(res.failedMasterIds.asScala.map((_, worker)).toMap.asJava)
        failedSlavePartitionIds.putAll(res.failedSlaveIds.asScala.map((_, worker)).toMap.asJava)

        if (!res.committedMapIdBitMap.isEmpty) {
          committedMapIdBitmap.putAll(res.committedMapIdBitMap)
        }

        totalWritten.add(res.totalWritten)
        fileCount.add(res.fileCount)
        currentShuffleFileCount.add(res.fileCount)
      }
    }

    def hasCommitFailedIds: Boolean = {
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      if (!pushReplicateEnabled && failedMasterPartitionIds.size() != 0) {
        val msg = failedMasterPartitionIds.asScala.map { case (partitionId, workerInfo) =>
          s"Lost partition $partitionId in worker [${workerInfo.readableAddress()}]"
        }.mkString("\n")
        logError(
          s"""
             |For shuffle $shuffleKey partition data lost:
             |$msg
             |""".stripMargin)
        true
      } else {
        val failedBothPartitionIdsToWorker = failedMasterPartitionIds.asScala.flatMap {
          case (partitionId, worker) =>
            if (failedSlavePartitionIds.contains(partitionId)) {
              Some(partitionId -> (worker, failedSlavePartitionIds.get(partitionId)))
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

    val dataLost = hasCommitFailedIds

    if (!dataLost) {
      val committedPartitions = new util.HashMap[String, PartitionLocation]
      committedMasterIds.asScala.foreach { id =>
        if (committedMasterStorageInfos.get(id) == null) {
          logDebug(s"$applicationId-$shuffleId $id storage hint was not returned")
        } else {
          masterPartMap.get(id).setStorageInfo(committedMasterStorageInfos.get(id))
          masterPartMap.get(id).setMapIdBitMap(committedMapIdBitmap.get(id))
          committedPartitions.put(id, masterPartMap.get(id))
        }
      }

      committedSlaveIds.asScala.foreach { id =>
        val slavePartition = slavePartMap.get(id)
        if (committedSlaveStorageInfos.get(id) == null) {
          logDebug(s"$applicationId-$shuffleId $id storage hint was not returned")
        } else {
          slavePartition.setStorageInfo(committedSlaveStorageInfos.get(id))
          val masterPartition = committedPartitions.get(id)
          if (masterPartition ne null) {
            masterPartition.setPeer(slavePartition)
            slavePartition.setPeer(masterPartition)
          } else {
            logInfo(s"Shuffle $shuffleId partition $id: master lost, " +
              s"use slave $slavePartition.")
            slavePartition.setMapIdBitMap(committedMapIdBitmap.get(id))
            committedPartitions.put(id, slavePartition)
          }
        }
      }

      val fileGroups = reducerFileGroupsMap.get(shuffleId)
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
        s"commit files complete. File count ${currentShuffleFileCount.sum()} " +
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
    recordWorkerFailure(commitFilesFailedWorkers)
    // release resources and clear worker info
    workerSnapshots(shuffleId).asScala.foreach { case (_, partitionLocationInfo) =>
      partitionLocationInfo.removeMasterPartitions(shuffleId.toString)
      partitionLocationInfo.removeSlavePartitions(shuffleId.toString)
    }
    requestReleaseSlots(
      rssHARetryClient,
      ReleaseSlots(applicationId, shuffleId, List.empty.asJava, List.empty.asJava))
  }

  private def handleUnregisterShuffle(
      appId: String,
      shuffleId: Int): Unit = {
    // if StageEnd has not been handled, trigger StageEnd
    if (!stageEndShuffleSet.contains(shuffleId)) {
      logInfo(s"Call StageEnd before Unregister Shuffle $shuffleId.")
      handleStageEnd(appId, shuffleId)
      var timeout = stageEndTimeout
      val delta = 100
      while (!stageEndShuffleSet.contains(shuffleId) && timeout > 0) {
        Thread.sleep(delta)
        timeout = timeout - delta
      }
      if (timeout <= 0) {
        logError(s"StageEnd Timeout! $shuffleId.")
      } else {
        logInfo("[handleUnregisterShuffle] Wait for handleStageEnd complete cost" +
          s" ${stageEndTimeout - timeout}ms")
      }
    }

    if (partitionExists(shuffleId)) {
      logWarning(s"Partition exists for shuffle $shuffleId, " +
        "maybe caused by task rerun or speculative.")
      workerSnapshots(shuffleId).asScala.foreach { case (_, partitionLocationInfo) =>
        partitionLocationInfo.removeMasterPartitions(shuffleId.toString)
        partitionLocationInfo.removeSlavePartitions(shuffleId.toString)
      }
      requestReleaseSlots(
        rssHARetryClient,
        ReleaseSlots(appId, shuffleId, List.empty.asJava, List.empty.asJava))
    }

    // add shuffleKey to delay shuffle removal set
    unregisterShuffleTime.put(shuffleId, System.currentTimeMillis())

    logInfo(s"Unregister for $shuffleId success.")
  }

  /* ========================================================== *
   |        END OF EVENT HANDLER                                |
   * ========================================================== */

  /**
   * After getting WorkerResource, LifecycleManger needs to ask each Worker to
   * reserve corresponding slot and prepare push data env in Worker side.
   *
   * @param applicationId Application ID
   * @param shuffleId     Application shuffle id
   * @param slots         WorkerResource to reserve slots
   * @return List of reserving slot failed workers
   */
  private def reserveSlots(
      applicationId: String,
      shuffleId: Int,
      slots: WorkerResource): util.List[WorkerInfo] = {
    val reserveSlotFailedWorkers = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
    val parallelism = Math.min(Math.max(1, slots.size()), conf.rpcMaxParallelism)
    ThreadUtils.parmap(slots.asScala.to, "ReserveSlot", parallelism) {
      case (workerInfo, (masterLocations, slaveLocations)) =>
        val res = requestReserveSlots(
          workerInfo.endpoint,
          ReserveSlots(
            applicationId,
            shuffleId,
            masterLocations,
            slaveLocations,
            partitionSplitThreshold,
            partitionSplitMode,
            getPartitionType(shuffleId),
            rangeReadFilter,
            userIdentifier))
        if (res.status.equals(StatusCode.SUCCESS)) {
          logDebug(s"Successfully allocated " +
            s"partitions buffer for ${Utils.makeShuffleKey(applicationId, shuffleId)}" +
            s" from worker ${workerInfo.readableAddress()}.")
        } else {
          logError(s"[reserveSlots] Failed to" +
            s" reserve buffers for ${Utils.makeShuffleKey(applicationId, shuffleId)}" +
            s" from worker ${workerInfo.readableAddress()}. Reason: ${res.reason}")
          reserveSlotFailedWorkers.put(workerInfo, (res.status, System.currentTimeMillis()))
        }
    }

    recordWorkerFailure(reserveSlotFailedWorkers)
    new util.ArrayList[WorkerInfo](reserveSlotFailedWorkers.asScala.keys.toList.asJava)
  }

  /**
   * When enabling replicate, if one of the partition location reserve slots failed,
   * LifecycleManager also needs to release another corresponding partition location.
   * To release the corresponding partition location, LifecycleManager should:
   *   1. Remove the peer partition location of failed partition location from slots.
   *   2. Request the Worker to destroy the slot's FileWriter.
   *   3. Request the Master to release the worker slots status.
   *
   * @param applicationId            application id
   * @param shuffleId                shuffle id
   * @param slots                    allocated WorkerResource
   * @param failedPartitionLocations reserve slot failed partition location
   */
  private def releasePeerPartitionLocation(
      applicationId: String,
      shuffleId: Int,
      slots: WorkerResource,
      failedPartitionLocations: mutable.HashMap[Int, PartitionLocation]): Unit = {
    val destroyResource = new WorkerResource
    failedPartitionLocations.values
      .flatMap { partition => Option(partition.getPeer) }
      .foreach { partition =>
        var destroyWorkerInfo = partition.getWorker
        val workerInfoWithRpcRef = slots.keySet().asScala.find(_.equals(destroyWorkerInfo))
          .getOrElse {
            logWarning(s"Cannot find workInfo from previous success workResource:" +
              s" ${destroyWorkerInfo.readableAddress()}, init according to partition info")
            try {
              destroyWorkerInfo.endpoint = rpcEnv.setupEndpointRef(
                RpcAddress.apply(destroyWorkerInfo.host, destroyWorkerInfo.rpcPort),
                WORKER_EP)
            } catch {
              case t: Throwable =>
                logError(s"Init rpc client failed for ${destroyWorkerInfo.readableAddress()}", t)
                destroyWorkerInfo = null
            }
            destroyWorkerInfo
          }
        if (slots.containsKey(workerInfoWithRpcRef)) {
          val (masterPartitionLocations, slavePartitionLocations) = slots.get(workerInfoWithRpcRef)
          partition.getMode match {
            case PartitionLocation.Mode.MASTER =>
              masterPartitionLocations.remove(partition)
              destroyResource.computeIfAbsent(workerInfoWithRpcRef, newLocationFunc)
                ._1.add(partition)
            case PartitionLocation.Mode.SLAVE =>
              slavePartitionLocations.remove(partition)
              destroyResource.computeIfAbsent(workerInfoWithRpcRef, newLocationFunc)
                ._2.add(partition)
          }
          if (masterPartitionLocations.isEmpty && slavePartitionLocations.isEmpty) {
            slots.remove(workerInfoWithRpcRef)
          }
        }
      }
    if (!destroyResource.isEmpty) {
      destroySlotsWithRetry(applicationId, shuffleId, destroyResource)
      logInfo(s"Destroyed peer partitions for reserve buffer failed workers " +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}, $destroyResource")

      val workerIds = new util.ArrayList[String]()
      val workerSlotsPerDisk = new util.ArrayList[util.Map[String, Integer]]()
      Utils.getSlotsPerDisk(destroyResource).asScala.foreach {
        case (workerInfo, slotsPerDisk) =>
          workerIds.add(workerInfo.toUniqueId())
          workerSlotsPerDisk.add(slotsPerDisk)
      }
      val msg = ReleaseSlots(applicationId, shuffleId, workerIds, workerSlotsPerDisk)
      requestReleaseSlots(rssHARetryClient, msg)
      logInfo(s"Released slots for reserve buffer failed workers " +
        s"${workerIds.asScala.mkString(",")}" + s"${slots.asScala.mkString(",")}" +
        s"${Utils.makeShuffleKey(applicationId, shuffleId)}, ")
    }
  }

  /**
   * Collect all allocated partition locations on reserving slot failed workers
   * and remove failed worker's partition locations from total slots.
   * For each reduce id, we only need to maintain one of the pair locations
   * even if enabling replicate. If RSS wants to release the failed partition location,
   * the corresponding peers will be handled in [[releasePeerPartitionLocation]]
   *
   * @param reserveFailedWorkers reserve slot failed WorkerInfo list of slots
   * @param slots                the slots tried to reserve a slot
   * @return reserving slot failed partition locations
   */
  def getFailedPartitionLocations(
      reserveFailedWorkers: util.List[WorkerInfo],
      slots: WorkerResource): mutable.HashMap[Int, PartitionLocation] = {
    val failedPartitionLocations = new mutable.HashMap[Int, PartitionLocation]()
    reserveFailedWorkers.asScala.foreach { workerInfo =>
      val (failedMasterLocations, failedSlaveLocations) = slots.remove(workerInfo)
      if (null != failedMasterLocations) {
        failedMasterLocations.asScala.foreach { failedMasterLocation =>
          failedPartitionLocations += (failedMasterLocation.getId -> failedMasterLocation)
        }
      }
      if (null != failedSlaveLocations) {
        failedSlaveLocations.asScala.foreach { failedSlaveLocation =>
          val partitionId = failedSlaveLocation.getId
          if (!failedPartitionLocations.contains(partitionId)) {
            failedPartitionLocations += (partitionId -> failedSlaveLocation)
          }
        }
      }
    }
    failedPartitionLocations
  }

  /**
   * Reserve buffers with retry, retry on another node will cause slots to be inconsistent.
   *
   * @param applicationId application id
   * @param shuffleId     shuffle id
   * @param candidates    working worker list
   * @param slots         the total allocated worker resources that need to be applied for the slot
   * @return If reserve all slots success
   */
  private def reserveSlotsWithRetry(
      applicationId: String,
      shuffleId: Int,
      candidates: util.HashSet[WorkerInfo],
      slots: WorkerResource,
      updateEpoch: Boolean = true): Boolean = {
    var requestSlots = slots
    val reserveSlotsMaxRetries = conf.reserveSlotsMaxRetries
    val reserveSlotsRetryWait = conf.reserveSlotsRetryWait
    var retryTimes = 1
    var noAvailableSlots = false
    var success = false
    while (retryTimes <= reserveSlotsMaxRetries && !success && !noAvailableSlots) {
      if (retryTimes > 1) {
        Thread.sleep(reserveSlotsRetryWait)
      }
      // reserve buffers
      logInfo(s"Try reserve slots for ${Utils.makeShuffleKey(applicationId, shuffleId)} " +
        s"for $retryTimes times.")
      val reserveFailedWorkers = reserveSlots(applicationId, shuffleId, requestSlots)
      if (reserveFailedWorkers.isEmpty) {
        success = true
      } else {
        // Should remove failed workers from candidates during retry to avoid reallocate in failed workers.
        candidates.removeAll(reserveFailedWorkers)
        // Find out all failed partition locations and remove failed worker's partition location
        // from slots.
        val failedPartitionLocations = getFailedPartitionLocations(reserveFailedWorkers, slots)
        // When enable replicate, if one of the partition location reserve slots failed, we also
        // need to release another corresponding partition location and remove it from slots.
        if (pushReplicateEnabled && failedPartitionLocations.nonEmpty && !slots.isEmpty) {
          releasePeerPartitionLocation(applicationId, shuffleId, slots, failedPartitionLocations)
        }
        if (retryTimes < reserveSlotsMaxRetries) {
          // get retryCandidates resource and retry reserve buffer
          val retryCandidates = new util.HashSet(slots.keySet())
          // add candidates to avoid revive action passed in slots only 2 worker
          retryCandidates.addAll(candidates)
          // remove blacklist from retryCandidates
          retryCandidates.removeAll(blacklist.keys().asScala.toList.asJava)
          if (retryCandidates.size < 1 || (pushReplicateEnabled && retryCandidates.size < 2)) {
            logError("Retry reserve slots failed caused by not enough slots.")
            noAvailableSlots = true
          } else {
            // Only when the LifecycleManager needs to retry reserve slots again, re-allocate slots
            // and put the new allocated slots to the total slots, the re-allocated slots won't be
            // duplicated with existing partition locations.
            requestSlots = reallocateSlotsFromCandidates(
              failedPartitionLocations.values.toList,
              retryCandidates.asScala.toList,
              updateEpoch)
            requestSlots.asScala.foreach { case (workerInfo, (retryMasterLocs, retrySlaveLocs)) =>
              val (masterPartitionLocations, slavePartitionLocations) =
                slots.computeIfAbsent(workerInfo, newLocationFunc)
              masterPartitionLocations.addAll(retryMasterLocs)
              slavePartitionLocations.addAll(retrySlaveLocs)
            }
          }
        } else {
          logError(s"Try reserve slots failed after $reserveSlotsMaxRetries retry.")
        }
      }
      retryTimes += 1
    }
    // if failed after retry, destroy all allocated buffers
    if (!success) {
      // Reserve slot failed workers' partition location and corresponding peer partition location
      // has been removed from slots by call [[getFailedPartitionLocations]] and
      // [[releasePeerPartitionLocation]]. Now in the slots are all the successful partition
      // locations.
      logWarning(s"Reserve buffers $shuffleId still fail after retrying, clear buffers.")
      destroySlotsWithRetry(applicationId, shuffleId, slots)
    } else {
      logInfo(s"Reserve buffer success for ${Utils.makeShuffleKey(applicationId, shuffleId)}")
    }
    success
  }

  private val newLocationFunc =
    new util.function.Function[WorkerInfo, (JList[PartitionLocation], JList[PartitionLocation])] {
      override def apply(w: WorkerInfo): (JList[PartitionLocation], JList[PartitionLocation]) =
        (new util.LinkedList[PartitionLocation](), new util.LinkedList[PartitionLocation]())
    }

  /**
   * Allocate a new master/slave PartitionLocation pair from the current WorkerInfo list.
   *
   * @param oldEpochId Current partition reduce location last epoch id
   * @param candidates WorkerInfo list can be used to offer worker slots
   * @param slots      Current WorkerResource
   */
  private def allocateFromCandidates(
      id: Int,
      oldEpochId: Int,
      candidates: List[WorkerInfo],
      slots: WorkerResource,
      updateEpoch: Boolean = true): Unit = {
    val masterIndex = Random.nextInt(candidates.size)
    val masterLocation = new PartitionLocation(
      id,
      if (updateEpoch) oldEpochId + 1 else oldEpochId,
      candidates(masterIndex).host,
      candidates(masterIndex).rpcPort,
      candidates(masterIndex).pushPort,
      candidates(masterIndex).fetchPort,
      candidates(masterIndex).replicatePort,
      PartitionLocation.Mode.MASTER)

    if (pushReplicateEnabled) {
      val slaveIndex = (masterIndex + 1) % candidates.size
      val slaveLocation = new PartitionLocation(
        id,
        if (updateEpoch) oldEpochId + 1 else oldEpochId,
        candidates(slaveIndex).host,
        candidates(slaveIndex).rpcPort,
        candidates(slaveIndex).pushPort,
        candidates(slaveIndex).fetchPort,
        candidates(slaveIndex).replicatePort,
        PartitionLocation.Mode.SLAVE,
        masterLocation)
      masterLocation.setPeer(slaveLocation)
      val masterAndSlavePairs = slots.computeIfAbsent(candidates(slaveIndex), newLocationFunc)
      masterAndSlavePairs._2.add(slaveLocation)
    }

    val masterAndSlavePairs = slots.computeIfAbsent(candidates(masterIndex), newLocationFunc)
    masterAndSlavePairs._1.add(masterLocation)
  }

  private def reallocateChangePartitionRequestSlotsFromCandidates(
      changePartitionRequests: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    changePartitionRequests.foreach { partition =>
      allocateFromCandidates(partition.partitionId, partition.epoch, candidates, slots)
    }
    slots
  }

  private def reallocateSlotsFromCandidates(
      oldPartitions: List[PartitionLocation],
      candidates: List[WorkerInfo],
      updateEpoch: Boolean = true): WorkerResource = {
    val slots = new WorkerResource()
    oldPartitions.foreach { partition =>
      allocateFromCandidates(partition.getId, partition.getEpoch, candidates, slots, updateEpoch)
    }
    slots
  }

  /**
   * For the slots that need to be destroyed, LifecycleManager will ask the corresponding worker
   * to destroy related FileWriter.
   *
   * @param applicationId  application id
   * @param shuffleId      shuffle id
   * @param slotsToDestroy worker resource to be destroyed
   * @return destroy failed master and slave location unique id
   */
  private def destroySlotsWithRetry(
      applicationId: String,
      shuffleId: Int,
      slotsToDestroy: WorkerResource): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    slotsToDestroy.asScala.foreach { case (workerInfo, (masterLocations, slaveLocations)) =>
      val destroy = Destroy(
        shuffleKey,
        masterLocations.asScala.map(_.getUniqueId).asJava,
        slaveLocations.asScala.map(_.getUniqueId).asJava)
      var res = requestDestroy(workerInfo.endpoint, destroy)
      if (res.status != StatusCode.SUCCESS) {
        logDebug(s"Request $destroy return ${res.status} for " +
          s"${Utils.makeShuffleKey(applicationId, shuffleId)}")
        res = requestDestroy(
          workerInfo.endpoint,
          Destroy(shuffleKey, res.failedMasters, res.failedSlaves))
      }
    }
  }

  private def removeExpiredShuffle(): Unit = {
    val currentTime = System.currentTimeMillis()
    unregisterShuffleTime.keys().asScala.foreach { shuffleId =>
      if (unregisterShuffleTime.get(shuffleId) < currentTime - shuffleExpiredCheckIntervalMs) {
        logInfo(s"Clear shuffle $shuffleId.")
        // clear for the shuffle
        registeredShuffle.remove(shuffleId)
        registeringShuffleRequest.remove(shuffleId)
        reducerFileGroupsMap.remove(shuffleId)
        dataLostShuffleSet.remove(shuffleId)
        shuffleMapperAttempts.remove(shuffleId)
        stageEndShuffleSet.remove(shuffleId)
        changePartitionRequests.remove(shuffleId)
        inBatchPartitions.remove(shuffleId)
        unregisterShuffleTime.remove(shuffleId)
        shuffleAllocatedWorkers.remove(shuffleId)
        latestPartitionLocation.remove(shuffleId)

        requestUnregisterShuffle(
          rssHARetryClient,
          UnregisterShuffle(appId, shuffleId, RssHARetryClient.genRequestId()))
      }
    }
  }

  private def handleGetBlacklist(msg: GetBlacklist): Unit = {
    val res = requestGetBlacklist(rssHARetryClient, msg)
    if (res.statusCode == StatusCode.SUCCESS) {
      logInfo(s"Received Blacklist from Master, blacklist: ${res.blacklist} " +
        s"unknown workers: ${res.unknownWorkers}")
      val current = System.currentTimeMillis()
      val reserved = blacklist.asScala
        .filter { case (_, entry) =>
          val (statusCode, registerTime) = entry
          statusCode match {
            case StatusCode.WORKER_SHUTDOWN | StatusCode.NO_AVAILABLE_WORKING_DIR | StatusCode.RESERVE_SLOTS_FAILED
                if current - registerTime < workerExcludedExpireTimeout =>
              true
            case StatusCode.UNKNOWN_WORKER => true
            case _ => false
          }
        }.asJava
      val reservedBlackList = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)]()
      reservedBlackList.putAll(reserved)
      blacklist.clear()
      blacklist.putAll(
        res.blacklist.asScala.map(_ -> (StatusCode.WORKER_IN_BLACKLIST -> current)).toMap.asJava)
      blacklist.putAll(
        res.unknownWorkers.asScala.map(_ -> (StatusCode.UNKNOWN_WORKER -> current)).toMap.asJava)
      // put reserved blacklist at last to cover blacklist's local status.
      blacklist.putAll(reservedBlackList)
    }
  }

  private def requestSlotsWithRetry(
      applicationId: String,
      shuffleId: Int,
      ids: util.ArrayList[Integer]): RequestSlotsResponse = {
    val req =
      RequestSlots(
        applicationId,
        shuffleId,
        ids,
        lifecycleHost,
        pushReplicateEnabled,
        userIdentifier)
    val res = requestRequestSlots(rssHARetryClient, req)
    if (res.status != StatusCode.SUCCESS) {
      requestRequestSlots(rssHARetryClient, req)
    } else {
      res
    }
  }

  private def requestRequestSlots(
      rssHARetryClient: RssHARetryClient,
      message: RequestSlots): RequestSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      rssHARetryClient.askSync[RequestSlotsResponse](message, classOf[RequestSlotsResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync RegisterShuffle for $shuffleKey failed.", e)
        RequestSlotsResponse(StatusCode.FAILED, new WorkerResource())
    }
  }

  private def requestReserveSlots(
      endpoint: RpcEndpointRef,
      message: ReserveSlots): ReserveSlotsResponse = {
    val shuffleKey = Utils.makeShuffleKey(message.applicationId, message.shuffleId)
    try {
      endpoint.askSync[ReserveSlotsResponse](message)
    } catch {
      case e: Exception =>
        val msg = s"Exception when askSync ReserveSlots for $shuffleKey " +
          s"on worker $endpoint."
        logError(msg, e)
        ReserveSlotsResponse(StatusCode.FAILED, msg + s" ${e.getMessage}")
    }
  }

  private def requestDestroy(endpoint: RpcEndpointRef, message: Destroy): DestroyResponse = {
    try {
      endpoint.askSync[DestroyResponse](message)
    } catch {
      case e: Exception =>
        logError(s"AskSync Destroy for ${message.shuffleKey} failed.", e)
        DestroyResponse(StatusCode.FAILED, message.masterLocations, message.slaveLocations)
    }
  }

  private def requestCommitFilesWithRetry(
      endpoint: RpcEndpointRef,
      message: CommitFiles): CommitFilesResponse = {
    val maxRetries = conf.requestCommitFilesMaxRetries
    var retryTimes = 0
    while (retryTimes < maxRetries) {
      try {
        return endpoint.askSync[CommitFilesResponse](message)
      } catch {
        case e: Exception =>
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

  private def requestReleaseSlots(
      rssHARetryClient: RssHARetryClient,
      message: ReleaseSlots): ReleaseSlotsResponse = {
    try {
      rssHARetryClient.askSync[ReleaseSlotsResponse](message, classOf[ReleaseSlotsResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync ReleaseSlots for ${message.shuffleId} failed.", e)
        ReleaseSlotsResponse(StatusCode.FAILED)
    }
  }

  private def requestUnregisterShuffle(
      rssHARetryClient: RssHARetryClient,
      message: PbUnregisterShuffle): PbUnregisterShuffleResponse = {
    try {
      rssHARetryClient.askSync[PbUnregisterShuffleResponse](
        message,
        classOf[PbUnregisterShuffleResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync UnregisterShuffle for ${message.getShuffleId} failed.", e)
        UnregisterShuffleResponse(StatusCode.FAILED)
    }
  }

  private def requestGetBlacklist(
      rssHARetryClient: RssHARetryClient,
      message: GetBlacklist): GetBlacklistResponse = {
    try {
      rssHARetryClient.askSync[GetBlacklistResponse](message, classOf[GetBlacklistResponse])
    } catch {
      case e: Exception =>
        logError(s"AskSync GetBlacklist failed.", e)
        GetBlacklistResponse(StatusCode.FAILED, List.empty.asJava, List.empty.asJava)
    }
  }

  private def recordWorkerFailure(failures: ConcurrentHashMap[WorkerInfo, (StatusCode, Long)])
      : Unit = {
    val failedWorker = new ConcurrentHashMap[WorkerInfo, (StatusCode, Long)](failures)
    logInfo(s"Report Worker Failure: ${failedWorker.asScala}, current blacklist $blacklist")
    blacklist.putAll(failedWorker)
  }

  def checkQuota(): Boolean = {
    try {
      rssHARetryClient.askSync[CheckQuotaResponse](
        CheckQuota(userIdentifier),
        classOf[CheckQuotaResponse]).isAvailable
    } catch {
      case e: Exception =>
        logError(s"AskSync Cluster check quota for $userIdentifier failed.", e)
        false
    }
  }

  private def partitionExists(shuffleId: Int): Boolean = {
    val workers = workerSnapshots(shuffleId)
    if (workers == null || workers.isEmpty) {
      false
    } else {
      workers.values().asScala.exists(_.containsShuffle(shuffleId.toString))
    }
  }

  private def workersNotBlacklisted(shuffleId: Int): List[WorkerInfo] = {
    workerSnapshots(shuffleId)
      .keySet()
      .asScala
      .filter(w => !blacklist.keySet().contains(w))
      .toList
  }

  // Initialize at the end of LifecycleManager construction.
  initialize()
}
