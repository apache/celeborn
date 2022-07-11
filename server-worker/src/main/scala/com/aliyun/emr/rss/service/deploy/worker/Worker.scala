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

package com.aliyun.emr.rss.service.deploy.worker

import java.io.IOException
import java.nio.ByteBuffer
import java.util.{ArrayList => jArrayList, HashSet => jHashSet, List => jList}
import java.util.concurrent.{CancellationException, CompletableFuture, ConcurrentHashMap, ExecutionException, LinkedBlockingQueue, ScheduledFuture, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.function.BiFunction

import scala.collection.JavaConverters._

import io.netty.buffer.ByteBuf
import io.netty.util.{HashedWheelTimer, Timeout, TimerTask}
import io.netty.util.internal.ConcurrentSet

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.{memoryTrimActionThreshold, partitionSortMaxMemoryRatio, partitionSortTimeout, workerDirectMemoryPressureCheckIntervalMs, workerDirectMemoryReportIntervalSecond, workerPausePushDataRatio, workerPauseRepcaliteRatio, workerResumeRatio}
import com.aliyun.emr.rss.common.exception.{AlreadyClosedException, RssException}
import com.aliyun.emr.rss.common.haclient.RssHARetryClient
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.{PartitionLocationInfo, WorkerInfo}
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.common.metrics.source.{JVMCPUSource, JVMSource, NetWorkSource}
import com.aliyun.emr.rss.common.network.TransportContext
import com.aliyun.emr.rss.common.network.buffer.NettyManagedBuffer
import com.aliyun.emr.rss.common.network.client.{RpcResponseCallback, TransportClientBootstrap}
import com.aliyun.emr.rss.common.network.protocol.{PushData, PushMergedData}
import com.aliyun.emr.rss.common.network.server.{ChannelsLimiter, FileInfo, MemoryTracker, TransportServerBootstrap}
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, PartitionSplitMode, RpcNameConstants, TransportModuleConstants}
import com.aliyun.emr.rss.common.protocol.PartitionLocation.StorageHint
import com.aliyun.emr.rss.common.protocol.message.ControlMessages._
import com.aliyun.emr.rss.common.protocol.message.StatusCode
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.unsafe.Platform
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.service.deploy.worker.http.HttpRequestHandler

private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    val conf: RssConf,
    val metricsSystem: MetricsSystem)
  extends RpcEndpoint with PushDataHandler with OpenStreamHandler with Registerable with Logging {

  private val workerSource = {
    val source = new WorkerSource(conf)
    metricsSystem.registerSource(source)
    metricsSystem.registerSource(new NetWorkSource(conf, MetricsSystem.ROLE_WOKRER))
    metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_WOKRER))
    metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_WOKRER))
    source
  }

  val memoryTracker = MemoryTracker.initialize(
    workerPausePushDataRatio(conf),
    workerPauseRepcaliteRatio(conf),
    workerResumeRatio(conf),
    partitionSortMaxMemoryRatio(conf),
    workerDirectMemoryPressureCheckIntervalMs(conf),
    workerDirectMemoryReportIntervalSecond(conf),
    memoryTrimActionThreshold(conf))

  private val localStorageManager = new LocalStorageManager(conf, workerSource, this)
  memoryTracker.registerMemoryListener(localStorageManager)

  private val partitionsSorter = new PartitionFilesSorter(memoryTracker,
    partitionSortTimeout(conf),
    RssConf.workerFetchChunkSize(conf),
    RssConf.memoryReservedForSingleSort(conf),
    workerSource)

  private val (pushServer, pushClientFactory) = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.push.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.PUSH_MODULE, numThreads)
    val rpcHandler = new PushDataRpcHandler(transportConf, this)
    val pushServerLimiter = new ChannelsLimiter(TransportModuleConstants.PUSH_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, closeIdleConnections, workerSource,
        pushServerLimiter)
    val serverBootstraps = new jArrayList[TransportServerBootstrap]()
    val clientBootstraps = new jArrayList[TransportClientBootstrap]()
    (transportContext.createServer(RssConf.pushServerPort(conf), serverBootstraps),
      transportContext.createClientFactory(clientBootstraps))
  }

  private val replicateServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.replicate.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.REPLICATE_MODULE,
      numThreads)
    val rpcHandler = new PushDataRpcHandler(transportConf, this)
    val replicateLimiter = new ChannelsLimiter(TransportModuleConstants.REPLICATE_MODULE)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, closeIdleConnections, workerSource,
        replicateLimiter)
    val serverBootstraps = new jArrayList[TransportServerBootstrap]()
    transportContext.createServer(RssConf.replicateServerPort(conf), serverBootstraps)
  }

  private val fetchServer = {
    val closeIdleConnections = RssConf.closeIdleConnections(conf)
    val numThreads = conf.getInt("rss.fetch.io.threads", localStorageManager.numDisks * 2)
    val transportConf = Utils.fromRssConf(conf, TransportModuleConstants.FETCH_MODULE, numThreads)
    val rpcHandler = new ChunkFetchHandler(transportConf, workerSource, this)
    val transportContext: TransportContext =
      new TransportContext(transportConf, rpcHandler, closeIdleConnections, workerSource)
    val serverBootstraps = new jArrayList[TransportServerBootstrap]()
    transportContext.createServer(RssConf.fetchServerPort(conf), serverBootstraps)
  }

  private val host = rpcEnv.address.host
  private val rpcPort = rpcEnv.address.port
  private val pushPort = pushServer.getPort
  private val fetchPort = fetchServer.getPort
  private val replicatePort = replicateServer.getPort

  Utils.checkHost(host)
  assert(pushPort > 0)
  assert(fetchPort > 0)
  assert(replicatePort > 0)

  // whether this Worker registered to Master succesfully
  private val registered = new AtomicBoolean(false)

  private val shuffleMapperAttempts = new ConcurrentHashMap[String, Array[Int]]()

  private val rssHARetryClient = new RssHARetryClient(rpcEnv, conf)

  // worker info
  private val workerInfo = new WorkerInfo(host, rpcPort, pushPort, fetchPort, replicatePort, self)

  private val partitionLocationInfo = new PartitionLocationInfo

  private val replicateFastfailDuration = RssConf.replicateFastFailDurationMs(conf)
  // (workerInfo -> last connect timeout timestamp)
  private val unavailablePeers = new ConcurrentHashMap[WorkerInfo, Long]()

  workerSource.addGauge(
    WorkerSource.RegisteredShuffleCount, _ => partitionLocationInfo.shuffleKeySet.size())
  workerSource.addGauge(WorkerSource.SlotsUsed, _ => workerInfo.usedSlots())
  workerSource.addGauge(WorkerSource.SortMemory, _ => memoryTracker.getSortMemoryCounter.get())
  workerSource.addGauge(WorkerSource.SortingFiles, _ => partitionsSorter.getSortingCount)
  workerSource.addGauge(WorkerSource.DiskBuffer, _ => memoryTracker.getDiskBufferCounter.get())
  workerSource.addGauge(WorkerSource.NettyMemory, _ => memoryTracker.getNettyMemoryCounter.get())
  workerSource.addGauge(WorkerSource.PausePushDataCount, _ => memoryTracker.getPausePushDataCounter)
  workerSource.addGauge(WorkerSource.PausePushDataAndReplicateCount,
    _ => memoryTracker.getPausePushDataAndReplicateCounter)

  // Threads
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private val replicateThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-replicate-data", RssConf.workerReplicateNumThreads(conf))
  private val timer = new HashedWheelTimer()

  // Configs
  private val HEARTBEAT_MILLIS = RssConf.workerTimeoutMs(conf) / 4

  // shared ExecutorService for flush
  private val commitThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "Worker-CommitFiles", RssConf.workerAsyncCommitFileThreads(conf))
  private val asyncReplyPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("async-reply")

  private var logAvailableFlushBuffersTask: ScheduledFuture[_] = _
  private var sendHeartbeatTask: ScheduledFuture[_] = _
  private var checkFastfailTask: ScheduledFuture[_] = _

  def heartBeatToMaster(): Unit = {
    val shuffleKeys = new jHashSet[String]
    shuffleKeys.addAll(partitionLocationInfo.shuffleKeySet)
    shuffleKeys.addAll(localStorageManager.shuffleKeySet())
    val response = rssHARetryClient.askSync[HeartbeatResponse](
      HeartbeatFromWorker(host, rpcPort, pushPort, fetchPort, replicatePort,
        localStorageManager.getDiskSnapshot().asJava, shuffleKeys), classOf[HeartbeatResponse])
    if (response.registered) {
      cleanTaskQueue.put(response.expiredShuffleKeys)
    } else {
      logError("Worker not registered in master, clean all shuffle data and register again.")
      // Clean all shuffle related metadata and data
      cleanup(shuffleKeys)
      try {
        registerWithMaster()
      } catch {
        case e: Throwable =>
          logError("Re-register worker failed after worker lost.", e)
          // Register failed then stop server
          stop()
      }
    }
  }

  override def onStart(): Unit = {
    logInfo(s"Starting Worker $host:$pushPort:$fetchPort:$replicatePort" +
      s" with ${workerInfo.disks}.")
    registerWithMaster()

    // start heartbeat
    sendHeartbeatTask = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        heartBeatToMaster()
      }
    }, HEARTBEAT_MILLIS, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)

    checkFastfailTask = forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        unavailablePeers.entrySet().asScala.foreach(entry => {
          if (System.currentTimeMillis() - entry.getValue >
            replicateFastfailDuration) {
            unavailablePeers.remove(entry.getKey)
          }
        })
      }
    }, 0, replicateFastfailDuration, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    logInfo("Stopping RSS Worker.")

    if (sendHeartbeatTask != null) {
      sendHeartbeatTask.cancel(true)
      sendHeartbeatTask = null
    }
    if (logAvailableFlushBuffersTask != null) {
      logAvailableFlushBuffersTask.cancel(true)
      logAvailableFlushBuffersTask = null
    }
    if (checkFastfailTask != null) {
      checkFastfailTask.cancel(true)
      checkFastfailTask = null
    }

    forwardMessageScheduler.shutdownNow()
    replicateThreadPool.shutdownNow()
    commitThreadPool.shutdownNow()
    asyncReplyPool.shutdownNow()
    partitionsSorter.close()

    if (null != localStorageManager) {
      localStorageManager.close()
    }

    rssHARetryClient.close()
    replicateServer.close()
    fetchServer.close()
    logInfo("RSS Worker is stopped.")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ReserveSlots(applicationId, shuffleId, masterLocations, slaveLocations, splitThreashold,
    splitMode, storageHint) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.ReserveSlotsTime, shuffleKey) {
        logDebug(s"Received ReserveSlots request, $shuffleKey, " +
          s"master partitions: ${masterLocations.asScala.map(_.getUniqueId).mkString(",")}; " +
          s"slave partitions: ${slaveLocations.asScala.map(_.getUniqueId).mkString(",")}.")
        handleReserveSlots(context, applicationId, shuffleId, masterLocations,
          slaveLocations, splitThreashold, splitMode, storageHint)
        logDebug(s"ReserveSlots for $shuffleKey succeed.")
      }

    case CommitFiles(applicationId, shuffleId, masterIds, slaveIds, mapAttempts) =>
      val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
      workerSource.sample(WorkerSource.CommitFilesTime, shuffleKey) {
        logDebug(s"Received CommitFiles request, $shuffleKey, master files" +
          s" ${masterIds.asScala.mkString(",")}; slave files ${slaveIds.asScala.mkString(",")}.")
        val commitFilesTimeMs = Utils.timeIt({
          handleCommitFiles(context, shuffleKey, masterIds, slaveIds, mapAttempts)
        })
        logDebug(s"Done processed CommitFiles request with shuffleKey $shuffleKey, in " +
          s"${commitFilesTimeMs}ms.")
      }

    case GetWorkerInfos =>
      handleGetWorkerInfos(context)

    case ThreadDump =>
      handleThreadDump(context)

    case Destroy(shuffleKey, masterLocations, slaveLocations) =>
      handleDestroy(context, shuffleKey, masterLocations, slaveLocations)
  }

  private def handleReserveSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      masterLocations: jList[PartitionLocation],
      slaveLocations: jList[PartitionLocation],
      splitThreshold: Long,
      splitMode: PartitionSplitMode,
      storageHint: StorageHint): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    if (!localStorageManager.hasAvailableWorkingDirs) {
      val msg = "Local storage has no available dirs!"
      logError(s"[handleReserveSlots] $msg")
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }
    val masterPartitions = new jArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until masterLocations.size()) {
        val location = masterLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId, location,
          splitThreshold, splitMode)
        masterPartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (masterPartitions.size() < masterLocations.size()) {
      val msg = s"Not all master partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, will destroy writers.")
      masterPartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }

    val slavePartitions = new jArrayList[PartitionLocation]()
    try {
      for (ind <- 0 until slaveLocations.size()) {
        val location = slaveLocations.get(ind)
        val writer = localStorageManager.createWriter(applicationId, shuffleId,
          location, splitThreshold, splitMode)
        slavePartitions.add(new WorkingPartition(location, writer))
      }
    } catch {
      case e: Exception =>
        logError(s"CreateWriter for $shuffleKey failed.", e)
    }
    if (slavePartitions.size() < slaveLocations.size()) {
      val msg = s"Not all slave partition satisfied for $shuffleKey"
      logWarning(s"[handleReserveSlots] $msg, destroy writers.")
      masterPartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      slavePartitions.asScala.foreach(_.asInstanceOf[WorkingPartition].getFileWriter.destroy())
      context.reply(ReserveSlotsResponse(StatusCode.ReserveSlotFailed, msg))
      return
    }

    // reserve success, update status
    partitionLocationInfo.addMasterPartitions(shuffleKey, masterPartitions)
    partitionLocationInfo.addSlavePartitions(shuffleKey, slavePartitions)
    val diskAllocationMap = (masterPartitions.asScala ++ slavePartitions.asScala)
      .groupBy(_.getDiskHint).map(i => i._1 -> new Integer(i._2.size))
    workerInfo.allocateSlots(shuffleKey, diskAllocationMap.asJava)
    logInfo(s"Reserved ${masterPartitions.size()} master location and ${slavePartitions.size()}" +
      s" slave location for $shuffleKey master: ${masterPartitions}\nslave: ${slavePartitions}.")
    context.reply(ReserveSlotsResponse(StatusCode.Success))
  }

  private def commitFiles(
      shuffleKey: String,
      uniqueIds: jList[String],
      committedIds: ConcurrentSet[String],
      failedIds: ConcurrentSet[String],
      writtenList: LinkedBlockingQueue[Long],
      master: Boolean = true
      ): CompletableFuture[Void] = {
    var future: CompletableFuture[Void] = null

    if (uniqueIds != null) {
      uniqueIds.asScala.foreach { uniqueId =>
        val task = CompletableFuture.runAsync(new Runnable {
          override def run(): Unit = {
            try {
              val location = if (master) {
                partitionLocationInfo.getMasterLocation(shuffleKey, uniqueId)
              } else {
                partitionLocationInfo.getSlaveLocation(shuffleKey, uniqueId)
              }

              if (location == null) {
                logWarning(s"Get Partition Location for $shuffleKey $uniqueId but didn't exist.")
                return
              }

              val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
              val writtenBytes = fileWriter.close()
              if (writtenBytes > 0L) {
                logDebug(s"FileName ${fileWriter.getFile.getAbsoluteFile}, size $writtenBytes")
                writtenList.add(writtenBytes)
              val bytes = fileWriter.close()
              if (bytes > 0L) {
                committedIds.add(uniqueId)
              }
            } catch {
              case e: IOException =>
                logError(s"Commit file for $shuffleKey $uniqueId failed.", e)
                failedIds.add(uniqueId)
            }
          }
        }, commitThreadPool)

        if (future == null) {
          future = task
        } else {
          future = CompletableFuture.allOf(future, task)
        }
      }
    }

    future
  }

  private def handleCommitFiles(
      context: RpcCallContext,
      shuffleKey: String,
      masterIds: jList[String],
      slaveIds: jList[String],
      mapAttempts: Array[Int]): Unit = {
    // return null if shuffleKey does not exist
    if (!partitionLocationInfo.containsShuffle(shuffleKey)) {
      logError(s"Shuffle $shuffleKey doesn't exist!")
      context.reply(CommitFilesResponse(
        StatusCode.ShuffleNotRegistered, List.empty.asJava, List.empty.asJava,
        masterIds, slaveIds, 0, 0))
      return
    }

    // close and flush files.
    shuffleMapperAttempts.putIfAbsent(shuffleKey, mapAttempts)

    // Use ConcurrentSet to avoid excessive lock contention.
    val committedMasterIds = new ConcurrentSet[String]()
    val committedSlaveIds = new ConcurrentSet[String]()
    val failedMasterIds = new ConcurrentSet[String]()
    val failedSlaveIds = new ConcurrentSet[String]()

    val committedWrittenSize = new LinkedBlockingQueue[Long]()

    val masterFuture = commitFiles(shuffleKey, masterIds, committedMasterIds, failedMasterIds, committedWrittenSize)
    val slaveFuture = commitFiles(shuffleKey, slaveIds, committedSlaveIds, failedSlaveIds, committedWrittenSize, false)

    val future = if (masterFuture != null && slaveFuture != null) {
      CompletableFuture.allOf(masterFuture, slaveFuture)
    } else if (masterFuture != null) {
      masterFuture
    } else if (slaveFuture != null) {
      slaveFuture
    } else {
      null
    }

    def reply(): Unit = {
      // release slots before reply.
      val numSlotsReleased =
        partitionLocationInfo.removeMasterPartitions(shuffleKey, masterIds) +
        partitionLocationInfo.removeSlavePartitions(shuffleKey, slaveIds)

      val committedMasterIdList = new jArrayList[String](committedMasterIds)
      val committedSlaveIdList = new jArrayList[String](committedSlaveIds)
      val failedMasterIdList = new jArrayList[String](failedMasterIds)
      val failedSlaveIdList = new jArrayList[String](failedSlaveIds)
      // reply
      val totalWritten = committedWrittenSize.asScala.sum
      val fileCount = committedWrittenSize.size()
      committedWrittenSize.clear()
      if (failedMasterIds.isEmpty && failedSlaveIds.isEmpty) {
        logInfo(s"CommitFiles for $shuffleKey success with ${committedMasterIds.size()}" +
          s" master partitions and ${committedSlaveIds.size()} slave partitions!")
        context.reply(CommitFilesResponse(
          StatusCode.Success, committedMasterIdList, committedSlaveIdList,
          List.empty.asJava, List.empty.asJava, totalWritten, fileCount))
      } else {
        logWarning(s"CommitFiles for $shuffleKey failed with ${failedMasterIds.size()} master" +
          s" partitions and ${failedSlaveIds.size()} slave partitions!")
        context.reply(CommitFilesResponse(StatusCode.PartialSuccess, committedMasterIdList,
          committedSlaveIdList, failedMasterIdList, failedSlaveIdList, totalWritten, fileCount))
      }
    }

    if (future != null) {
      val result = new AtomicReference[CompletableFuture[Unit]]()
      val flushTimeout = RssConf.flushTimeout(conf)

      val timeout = timer.newTimeout(new TimerTask {
        override def run(timeout: Timeout): Unit = {
          if (result.get() != null) {
            result.get().cancel(true)
            logWarning(s"After waiting $flushTimeout s, cancel all commit file jobs.")
          }
        }
      }, flushTimeout, TimeUnit.SECONDS)

      result.set(future.handleAsync(new BiFunction[Void, Throwable, Unit] {
        override def apply(v: Void, t: Throwable): Unit = {
          if (null != t) {
            t match {
              case _: CancellationException =>
                logWarning("While handling commitFiles, canceled.")
              case ee: ExecutionException =>
                logError("While handling commitFiles, ExecutionException raised.", ee)
              case ie: InterruptedException =>
                logWarning("While handling commitFiles, interrupted.")
                Thread.currentThread().interrupt()
                throw ie
              case _: TimeoutException =>
                logWarning(s"While handling commitFiles, timeout after $flushTimeout s.")
              case throwable: Throwable =>
                logError("While handling commitFiles, exception occurs.", throwable)
            }
          } else {
            // finish, cancel timeout job first.
            timeout.cancel()
            reply()
          }
        }
      }, asyncReplyPool)) // should not use commitThreadPool in case of block by commit files.
    } else {
      // If both of two futures are null, then reply directly.
      reply()
    }
  }

  private def handleDestroy(
      context: RpcCallContext,
      shuffleKey: String,
      masterLocations: jList[String],
      slaveLocations: jList[String]): Unit = {
    // check whether shuffleKey has registered
    if (!partitionLocationInfo.containsShuffle(shuffleKey)) {
      logWarning(s"Shuffle $shuffleKey not registered!")
      context.reply(DestroyResponse(
        StatusCode.ShuffleNotRegistered, masterLocations, slaveLocations))
      return
    }

    val failedMasters = new jArrayList[String]()
    val failedSlaves = new jArrayList[String]()

    // destroy master locations
    if (masterLocations != null && !masterLocations.isEmpty) {
      masterLocations.asScala.foreach { loc =>
        val allocatedLoc = partitionLocationInfo.getMasterLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedMasters.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove master locations from WorkerInfo
      partitionLocationInfo.removeMasterPartitions(shuffleKey, masterLocations)
    }
    // destroy slave locations
    if (slaveLocations != null && !slaveLocations.isEmpty) {
      slaveLocations.asScala.foreach { loc =>
        val allocatedLoc = partitionLocationInfo.getSlaveLocation(shuffleKey, loc)
        if (allocatedLoc == null) {
          failedSlaves.add(loc)
        } else {
          allocatedLoc.asInstanceOf[WorkingPartition].getFileWriter.destroy()
        }
      }
      // remove slave locations from worker info
      partitionLocationInfo.removeSlavePartitions(shuffleKey, slaveLocations)
    }
    // reply
    if (failedMasters.isEmpty && failedSlaves.isEmpty) {
      logInfo(s"Destroy ${masterLocations.size()} master location and ${slaveLocations.size()}" +
        s" slave locations for $shuffleKey successfully.")
      context.reply(DestroyResponse(StatusCode.Success, List.empty.asJava, List.empty.asJava))
    } else {
      logInfo(s"Destroy ${failedMasters.size()}/${masterLocations.size()} master location and" +
        s"${failedSlaves.size()}/${slaveLocations.size()} slave location for" +
          s" $shuffleKey PartialSuccess.")
      context.reply(DestroyResponse(StatusCode.PartialSuccess, failedMasters, failedSlaves))
    }
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    val list = new jArrayList[WorkerInfo]()
    list.add(workerInfo)
    context.reply(GetWorkerInfosResponse(StatusCode.Success, list.asScala.toList: _*))
  }

  private def handleThreadDump(context: RpcCallContext): Unit = {
    val threadDump = Utils.getThreadDump()
    context.reply(ThreadDumpResponse(threadDump))
  }

  private def getMapAttempt(body: ByteBuf): (Int, Int) = {
    // header: mapId attemptId batchId compressedTotalSize
    val header = new Array[Byte](8)
    body.getBytes(body.readerIndex(), header)
    val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
    val attemptId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET + 4)
    (mapId, attemptId)
  }

  override def handlePushData(pushData: PushData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushData.shuffleKey
    val mode = PartitionLocation.getMode(pushData.mode)
    val body = pushData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master
    val bodySize = pushData.body().size()

    val key = s"${pushData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    // find FileWriter responsible for the data
    val location = if (isMaster) {
      partitionLocationInfo.getMasterLocation(shuffleKey, pushData.partitionUniqueId)
    } else {
      partitionLocationInfo.getSlaveLocation(shuffleKey, pushData.partitionUniqueId)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        logError(s"[handlePushData.onFailure] partitionLocation: $location")
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage(), e))
      }
    }

    if (location == null) {
      val (mapId, attemptId) = getMapAttempt(body)
      if (shuffleMapperAttempts.containsKey(shuffleKey) &&
          -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
        // partition data has already been committed
        logInfo(s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId, " +
          s" attempt $attemptId), but this mapper has already been ended.")
        wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
      } else {
        val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId, " +
          s"attempt $attemptId, uniqueId ${pushData.partitionUniqueId})."
        logWarning(s"[handlePushData] $msg")
        callback.onFailure(new Exception(StatusCode.PushDataFailPartitionNotFound.getMessage()))
      }
      return
    }
    val fileWriter = location.asInstanceOf[WorkingPartition].getFileWriter
    val exception = fileWriter.getException
    if (exception != null) {
      logWarning(s"[handlePushData] fileWriter $fileWriter has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage()
      } else {
        StatusCode.PushDataFailSlave.getMessage()
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    if (isMaster && fileWriter.getFileLength > fileWriter.getSplitThreshold()) {
      fileWriter.setSplitFlag()
      if (fileWriter.getSplitMode == PartitionSplitMode.soft) {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.SoftSplit.getValue)))
      } else {
        callback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.HardSplit.getValue)))
        return
      }
    }
    fileWriter.incrementPendingWrites()

    // for master, send data to slave
    if (location.getPeer != null && isMaster) {
      pushData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(peer.getHost, peer.getRpcPort, peer.getPushPort,
            peer.getFetchPort, peer.getReplicatePort)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(peer.getHost, peer.getReplicatePort,
              location.getReduceId)
            val newPushData = new PushData(
              PartitionLocation.Mode.Slave.mode(),
              shuffleKey,
              pushData.partitionUniqueId,
              pushData.body)
            client.pushData(newPushData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    try {
      fileWriter.write(body)
    } catch {
      case e: AlreadyClosedException =>
        fileWriter.decrementPendingWrites()
        val (mapId, attemptId) = getMapAttempt(body)
        val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
          shuffleMapperAttempts.get(shuffleKey)(mapId)
        } else -1
        logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
          s" $attemptId), caused by ${e.getMessage}")
      case e: Exception =>
        logError("Exception encountered when write.", e)
    }
  }

  override def handlePushMergedData(
      pushMergedData: PushMergedData, callback: RpcResponseCallback): Unit = {
    val shuffleKey = pushMergedData.shuffleKey
    val mode = PartitionLocation.getMode(pushMergedData.mode)
    val batchOffsets = pushMergedData.batchOffsets
    val body = pushMergedData.body.asInstanceOf[NettyManagedBuffer].getBuf
    val isMaster = mode == PartitionLocation.Mode.Master
    val bodySize = pushMergedData.body().size()

    val key = s"${pushMergedData.requestId}"
    if (isMaster) {
      workerSource.startTimer(WorkerSource.MasterPushDataTime, key)
    } else {
      workerSource.startTimer(WorkerSource.SlavePushDataTime, key)
    }

    val wrappedCallback = new RpcResponseCallback() {
      override def onSuccess(response: ByteBuffer): Unit = {
        if (isMaster) {
          workerSource.stopTimer(WorkerSource.MasterPushDataTime, key)
          if (response.remaining() > 0) {
            val resp = ByteBuffer.allocate(response.remaining())
            resp.put(response)
            resp.flip()
            callback.onSuccess(resp)
          } else {
            callback.onSuccess(response)
          }
        } else {
          workerSource.stopTimer(WorkerSource.SlavePushDataTime, key)
          callback.onSuccess(response)
        }
      }

      override def onFailure(e: Throwable): Unit = {
        workerSource.incCounter(WorkerSource.PushDataFailCount)
        callback.onFailure(new Exception(StatusCode.PushDataFailSlave.getMessage, e))
      }
    }

    // find FileWriters responsible for the data
    val locations = pushMergedData.partitionUniqueIds.map { id =>
      val loc = if (isMaster) {
        partitionLocationInfo.getMasterLocation(shuffleKey, id)
      } else {
        partitionLocationInfo.getSlaveLocation(shuffleKey, id)
      }
      if (loc == null) {
        val (mapId, attemptId) = getMapAttempt(body)
        if (shuffleMapperAttempts.containsKey(shuffleKey)
            && -1 != shuffleMapperAttempts.get(shuffleKey)(mapId)) {
          val msg = s"Receive push data from speculative task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId), but this mapper has already been ended."
          logInfo(msg)
          wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte](StatusCode.StageEnded.getValue)))
        } else {
          val msg = s"Partition location wasn't found for task(shuffle $shuffleKey, map $mapId," +
            s" attempt $attemptId, uniqueId $id)."
          logWarning(s"[handlePushMergedData] $msg")
          wrappedCallback.onFailure(new Exception(msg))
        }
        return
      }
      loc
    }

    val fileWriters = locations.map(_.asInstanceOf[WorkingPartition].getFileWriter)
    val fileWriterWithException = fileWriters.find(_.getException != null)
    if (fileWriterWithException.nonEmpty) {
      val exception = fileWriterWithException.get.getException
      logDebug(s"[handlePushMergedData] fileWriter ${fileWriterWithException}" +
          s" has Exception $exception")
      val message = if (isMaster) {
        StatusCode.PushDataFailMain.getMessage()
      } else {
        StatusCode.PushDataFailSlave.getMessage()
      }
      callback.onFailure(new Exception(message, exception))
      return
    }
    fileWriters.foreach(_.incrementPendingWrites())

    // for master, send data to slave
    if (locations.head.getPeer != null && isMaster) {
      pushMergedData.body().retain()
      replicateThreadPool.submit(new Runnable {
        override def run(): Unit = {
          val location = locations.head
          val peer = location.getPeer
          val peerWorker = new WorkerInfo(peer.getHost,
            peer.getRpcPort, peer.getPushPort, peer.getFetchPort, peer.getReplicatePort, null)
          if (unavailablePeers.containsKey(peerWorker)) {
            pushMergedData.body().release()
            wrappedCallback.onFailure(new Exception(s"Peer $peerWorker unavailable!"))
            return
          }
          try {
            val client = pushClientFactory.createClient(
              peer.getHost, peer.getReplicatePort, location.getReduceId)
            val newPushMergedData = new PushMergedData(
              PartitionLocation.Mode.Slave.mode(),
              shuffleKey,
              pushMergedData.partitionUniqueIds,
              batchOffsets,
              pushMergedData.body)
            client.pushMergedData(newPushMergedData, wrappedCallback)
          } catch {
            case e: Exception =>
              pushMergedData.body().release()
              unavailablePeers.put(peerWorker, System.currentTimeMillis())
              wrappedCallback.onFailure(e)
          }
        }
      })
    } else {
      wrappedCallback.onSuccess(ByteBuffer.wrap(Array[Byte]()))
    }

    var index = 0
    var fileWriter: FileWriter = null
    var alreadyClosed = false
    while (index < fileWriters.length) {
      fileWriter = fileWriters(index)
      val offset = body.readerIndex() + batchOffsets(index)
      val length = if (index == fileWriters.length - 1) {
        body.readableBytes() - batchOffsets(index)
      } else {
        batchOffsets(index + 1) - batchOffsets(index)
      }
      val batchBody = body.slice(offset, length)

      try {
        if (!alreadyClosed) {
          fileWriter.write(batchBody)
        } else {
          fileWriter.decrementPendingWrites()
        }
      } catch {
        case e: AlreadyClosedException =>
          fileWriter.decrementPendingWrites()
          alreadyClosed = true
          val (mapId, attemptId) = getMapAttempt(body)
          val endedAttempt = if (shuffleMapperAttempts.containsKey(shuffleKey)) {
            shuffleMapperAttempts.get(shuffleKey)(mapId)
          } else -1
          logWarning(s"Append data failed for task(shuffle $shuffleKey, map $mapId, attempt" +
            s" $attemptId), caused by ${e.getMessage}")
        case e: Exception =>
          logError("Exception encountered when write.", e)
      }
      index += 1
    }
  }

  override def handleOpenStream(shuffleKey: String, fileName: String, startMapIndex: Int,
    endMapIndex: Int): FileInfo = {
    // find FileWriter responsible for the data
    val fileWriter = localStorageManager.getWriter(shuffleKey, fileName)
    if (fileWriter eq null) {
      logWarning(s"File $fileName for $shuffleKey was not found!")
      return null
    }
    partitionsSorter.openStream(shuffleKey, fileName, fileWriter, startMapIndex, endMapIndex);
  }

  private def registerWithMaster() {
    var registerTimeout = RssConf.registerWorkerTimeoutMs(conf)
    val delta = 2000
    while (registerTimeout > 0) {
      val rsp = try {
        rssHARetryClient.askSync[RegisterWorkerResponse](
          RegisterWorker(host, rpcPort, pushPort, fetchPort, replicatePort, workerInfo.disks),
          classOf[RegisterWorkerResponse]
        )
      } catch {
        case throwable: Throwable =>
          logWarning(s"Register worker to master failed, will retry after 2s, exception: ",
            throwable)
          null
      }
      // Register successfully
      if (null != rsp && rsp.success) {
        registered.set(true)
        logInfo("Register worker successfully.")
        return
      }
      // Register failed, sleep and retry
      Thread.sleep(delta)
      registerTimeout = registerTimeout - delta
    }
    // If worker register still failed after retry, throw exception to stop worker process
    throw new RssException("Register worker failed.")
  }

  private val cleanTaskQueue = new LinkedBlockingQueue[jHashSet[String]]
  private val cleaner = new Thread("Cleaner") {
    override def run(): Unit = {
      while (true) {
        val expiredShuffleKeys = cleanTaskQueue.take()
        try {
          cleanup(expiredShuffleKeys)
        } catch {
          case e: Throwable =>
            logError("Cleanup failed", e)
        }
      }
    }
  }
  cleaner.setDaemon(true)
  cleaner.start()

  private def cleanup(expiredShuffleKeys: jHashSet[String]): Unit = synchronized {
    expiredShuffleKeys.asScala.foreach { shuffleKey =>
      partitionLocationInfo.getAllMasterLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.getAllSlaveLocations(shuffleKey).asScala.foreach { partition =>
        partition.asInstanceOf[WorkingPartition].getFileWriter.destroy()
      }
      partitionLocationInfo.removeMasterPartitions(shuffleKey)
      partitionLocationInfo.removeSlavePartitions(shuffleKey)
      shuffleMapperAttempts.remove(shuffleKey)
      logInfo(s"Cleaned up expired shuffle $shuffleKey")
    }
    partitionsSorter.cleanup(expiredShuffleKeys)
    localStorageManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
  }

  def isRegistered(): Boolean = {
    registered.get()
  }
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RssConf
    val workerArgs = new WorkerArguments(args, conf)
    // There are many entries for setting the master address, and we should unify the entries as
    // much as possible. Therefore, if the user manually specifies the address of the Master when
    // starting the Worker, we should set it in the parameters and automatically calculate what the
    // address of the Master should be used in the end.
    if (workerArgs.master != null) {
      conf.set("rss.master.address", RpcAddress.fromRssURL(workerArgs.master).toString)
    }

    val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, WorkerSource.ServletPath)

    val rpcEnv = RpcEnv.create(
      RpcNameConstants.WORKER_SYS,
      workerArgs.host,
      workerArgs.host,
      workerArgs.port,
      conf,
      Math.max(64, Runtime.getRuntime.availableProcessors()))
    rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, new Worker(rpcEnv, conf, metricsSystem))

    if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled!")
      metricsSystem.start()

      var port = RssConf.workerPrometheusMetricPort(conf)
      var initialized = false
      while (!initialized) {
        try {
          val httpServer = new HttpServer(
            new HttpServerInitializer(
              new HttpRequestHandler(metricsSystem.getPrometheusHandler)), port)
          httpServer.start()
          initialized = true
        } catch {
          case e: Exception =>
            logWarning(s"HttpServer pushPort $port may already exist, try pushPort ${port + 1}.", e)
            port += 1
            Thread.sleep(1000)
        }
      }
    }

    rpcEnv.awaitTermination()
  }
}
