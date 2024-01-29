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

import java.io.File
import java.lang.{Long => JLong}
import java.util
import java.util.{HashMap => JHashMap, HashSet => JHashSet, Locale, Map => JMap}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicIntegerArray}

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting
import io.netty.util.HashedWheelTimer

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo, WorkerPartitionLocationInfo}
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.metrics.source.{JVMCPUSource, JVMSource, ResourceConsumptionSource, SystemMiscSource, ThreadPoolSource}
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.protocol.{PartitionType, PbRegisterWorkerResponse, PbWorkerLostResponse, RpcNameConstants, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, ShutdownHookManager, ThreadUtils, Utils}
// Can Remove this if celeborn don't support scala211 in future
import org.apache.celeborn.common.util.FunctionConverter._
import org.apache.celeborn.server.common.{HttpService, Service}
import org.apache.celeborn.service.deploy.worker.WorkerSource.ACTIVE_CONNECTION_COUNT
import org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionController
import org.apache.celeborn.service.deploy.worker.memory.{ChannelsLimiter, MemoryManager}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager.ServingState
import org.apache.celeborn.service.deploy.worker.monitor.JVMQuake
import org.apache.celeborn.service.deploy.worker.storage.{PartitionFilesSorter, StorageManager}

private[celeborn] class Worker(
    override val conf: CelebornConf,
    val workerArgs: WorkerArguments)
  extends HttpService with Logging {

  @volatile private var stopped = false

  override def serviceName: String = Service.WORKER

  override val metricsSystem: MetricsSystem =
    MetricsSystem.createMetricsSystem(serviceName, conf)
  val workerSource = new WorkerSource(conf)
  private val resourceConsumptionSource =
    new ResourceConsumptionSource(conf, MetricsSystem.ROLE_WORKER)
  private val threadPoolSource = ThreadPoolSource(conf, MetricsSystem.ROLE_WORKER)
  metricsSystem.registerSource(workerSource)
  metricsSystem.registerSource(threadPoolSource)
  metricsSystem.registerSource(resourceConsumptionSource)
  metricsSystem.registerSource(new JVMSource(conf, MetricsSystem.ROLE_WORKER))
  metricsSystem.registerSource(new JVMCPUSource(conf, MetricsSystem.ROLE_WORKER))
  metricsSystem.registerSource(new SystemMiscSource(conf, MetricsSystem.ROLE_WORKER))

  val rpcEnv: RpcEnv = RpcEnv.create(
    RpcNameConstants.WORKER_SYS,
    workerArgs.host,
    workerArgs.host,
    workerArgs.port,
    conf,
    Math.min(64, Math.max(4, Runtime.getRuntime.availableProcessors())))

  private val host = rpcEnv.address.host
  private val rpcPort = rpcEnv.address.port
  Utils.checkHost(host)

  private val WORKER_SHUTDOWN_PRIORITY = 100
  val shutdown = new AtomicBoolean(false)
  private val gracefulShutdown = conf.workerGracefulShutdown
  private var exitKind = CelebornExitKind.EXIT_IMMEDIATELY
  if (gracefulShutdown) {
    val checkPortMap = Map(
      WORKER_RPC_PORT -> conf.workerRpcPort,
      WORKER_FETCH_PORT -> conf.workerFetchPort,
      WORKER_PUSH_PORT -> conf.workerPushPort,
      WORKER_REPLICATE_PORT -> conf.workerReplicatePort)
    assert(
      !checkPortMap.values.exists(_ == 0),
      "If enable graceful shutdown, the worker should use non-zero port. " +
        s"${checkPortMap.map { case (k, v) => k.key + "=" + v }.mkString(", ")}")
    exitKind = CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN
    try {
      val recoverRoot = new File(conf.workerGracefulShutdownRecoverPath)
      if (!recoverRoot.exists()) {
        logInfo(s"Recover root path ${conf.workerGracefulShutdownRecoverPath} does not exists, create it first.")
        recoverRoot.mkdirs()
      }
    } catch {
      case e: Exception =>
        logError("Check or create recover root path failed: ", e)
        throw e
    }
  }

  val storageManager = new StorageManager(conf, workerSource)

  val memoryManager: MemoryManager = MemoryManager.initialize(conf)
  memoryManager.registerMemoryListener(storageManager)

  val partitionsSorter = new PartitionFilesSorter(memoryManager, conf, workerSource)

  if (conf.workerCongestionControlEnabled) {
    if (conf.workerCongestionControlLowWatermark.isEmpty || conf.workerCongestionControlHighWatermark.isEmpty) {
      throw new IllegalArgumentException("High watermark and low watermark must be set" +
        " when enabling rate limit")
    }

    CongestionController.initialize(
      workerSource,
      conf.workerCongestionControlSampleTimeWindowSeconds.toInt,
      conf.workerCongestionControlHighWatermark.get,
      conf.workerCongestionControlLowWatermark.get,
      conf.workerCongestionControlUserInactiveIntervalMs,
      conf.workerCongestionControlCheckIntervalMs)
  }

  var controller = new Controller(rpcEnv, conf, metricsSystem, workerSource)
  rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, controller)

  val pushDataHandler = new PushDataHandler(workerSource)
  private val pushServer = {
    val closeIdleConnections = conf.workerCloseIdleConnections
    val numThreads = conf.workerPushIoThreads.getOrElse(storageManager.totalFlusherThread)
    val transportConf =
      Utils.fromCelebornConf(conf, TransportModuleConstants.PUSH_MODULE, numThreads)
    val pushServerLimiter = new ChannelsLimiter(TransportModuleConstants.PUSH_MODULE, conf)
    val transportContext: TransportContext =
      new TransportContext(
        transportConf,
        pushDataHandler,
        closeIdleConnections,
        pushServerLimiter,
        conf.workerPushHeartbeatEnabled,
        workerSource)
    transportContext.createServer(conf.workerPushPort)
  }

  val replicateHandler = new PushDataHandler(workerSource)
  val (replicateServer, replicateClientFactory) = {
    val closeIdleConnections = conf.workerCloseIdleConnections
    val numThreads =
      conf.workerReplicateIoThreads.getOrElse(storageManager.totalFlusherThread)
    val transportConf =
      Utils.fromCelebornConf(conf, TransportModuleConstants.REPLICATE_MODULE, numThreads)
    val replicateLimiter = new ChannelsLimiter(TransportModuleConstants.REPLICATE_MODULE, conf)
    val transportContext: TransportContext =
      new TransportContext(
        transportConf,
        replicateHandler,
        closeIdleConnections,
        replicateLimiter,
        false,
        workerSource)
    (
      transportContext.createServer(conf.workerReplicatePort),
      transportContext.createClientFactory())
  }

  var fetchHandler: FetchHandler = _
  private val fetchServer = {
    val closeIdleConnections = conf.workerCloseIdleConnections
    val numThreads = conf.workerFetchIoThreads.getOrElse(storageManager.totalFlusherThread)
    val transportConf =
      Utils.fromCelebornConf(conf, TransportModuleConstants.FETCH_MODULE, numThreads)
    fetchHandler = new FetchHandler(conf, transportConf, workerSource)
    val transportContext: TransportContext =
      new TransportContext(
        transportConf,
        fetchHandler,
        closeIdleConnections,
        conf.workerFetchHeartbeatEnabled,
        workerSource)
    transportContext.createServer(conf.workerFetchPort)
  }

  private val pushPort = pushServer.getPort
  assert(pushPort > 0, "worker push bind port should be positive")

  private val fetchPort = fetchServer.getPort
  assert(fetchPort > 0, "worker fetch bind port should be positive")

  private val replicatePort = replicateServer.getPort
  assert(replicatePort > 0, "worker replica bind port should be positive")

  storageManager.updateDiskInfos()

  // WorkerInfo's diskInfos is a reference to storageManager.diskInfos
  val diskInfos = JavaUtils.newConcurrentHashMap[String, DiskInfo]()
  storageManager.disksSnapshot().foreach { diskInfo =>
    diskInfos.put(diskInfo.mountPoint, diskInfo)
  }

  val workerInfo =
    new WorkerInfo(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort,
      diskInfos,
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption])

  // whether this Worker registered to Master successfully
  val registered = new AtomicBoolean(false)
  val shuffleMapperAttempts: ConcurrentHashMap[String, AtomicIntegerArray] =
    JavaUtils.newConcurrentHashMap[String, AtomicIntegerArray]()
  val shufflePartitionType: ConcurrentHashMap[String, PartitionType] =
    JavaUtils.newConcurrentHashMap[String, PartitionType]
  var shufflePushDataTimeout: ConcurrentHashMap[String, Long] =
    JavaUtils.newConcurrentHashMap[String, Long]
  val partitionLocationInfo = new WorkerPartitionLocationInfo

  val shuffleCommitInfos: ConcurrentHashMap[String, ConcurrentHashMap[Long, CommitInfo]] =
    JavaUtils.newConcurrentHashMap[String, ConcurrentHashMap[Long, CommitInfo]]()

  private val masterClient = new MasterClient(rpcEnv, conf)

  // (workerInfo -> last connect timeout timestamp)
  val unavailablePeers: ConcurrentHashMap[WorkerInfo, Long] =
    JavaUtils.newConcurrentHashMap[WorkerInfo, Long]()

  // Threads
  private val forwardMessageScheduler: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")
  private var sendHeartbeatTask: ScheduledFuture[_] = _
  private var checkFastFailTask: ScheduledFuture[_] = _

  val replicateThreadPool: ThreadPoolExecutor =
    ThreadUtils.newDaemonCachedThreadPool("worker-data-replicator", conf.workerReplicateThreads)
  val commitThreadPool: ThreadPoolExecutor =
    ThreadUtils.newDaemonCachedThreadPool("worker-files-committer", conf.workerCommitThreads)
  val cleanThreadPool: ThreadPoolExecutor =
    ThreadUtils.newDaemonCachedThreadPool(
      "worker-expired-shuffle-cleaner",
      conf.workerCleanThreads)
  val asyncReplyPool: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-rpc-async-replier")
  val timer = new HashedWheelTimer()

  // Configs
  private val heartbeatInterval = conf.workerHeartbeatTimeout / 4
  private val replicaFastFailDuration = conf.workerReplicateFastFailDuration

  private val cleanTaskQueue = new LinkedBlockingQueue[JHashSet[String]]
  var cleaner: ExecutorService =
    ThreadUtils.newDaemonSingleThreadExecutor("worker-expired-shuffle-cleaner")

  private var jvmQuake: JVMQuake = _
  if (conf.workerJvmQuakeEnabled) {
    jvmQuake = JVMQuake.create(conf, workerInfo.toUniqueId().replace(":", "-"))
    jvmQuake.start()
  }

  workerSource.addGauge(WorkerSource.REGISTERED_SHUFFLE_COUNT) { () =>
    workerInfo.getShuffleKeySet.size
  }
  workerSource.addGauge(WorkerSource.RUNNING_APPLICATION_COUNT) { () =>
    workerInfo.getApplicationIdSet.size
  }
  workerSource.addGauge(WorkerSource.SORT_MEMORY) { () =>
    memoryManager.getSortMemoryCounter.get()
  }
  workerSource.addGauge(WorkerSource.SORTING_FILES) { () =>
    partitionsSorter.getSortingCount
  }
  workerSource.addGauge(WorkerSource.SORTED_FILES) { () =>
    partitionsSorter.getSortedCount
  }
  workerSource.addGauge(WorkerSource.SORTED_FILE_SIZE) { () =>
    partitionsSorter.getSortedSize
  }
  workerSource.addGauge(WorkerSource.DISK_BUFFER) { () =>
    memoryManager.getDiskBufferCounter.get()
  }
  workerSource.addGauge(WorkerSource.NETTY_MEMORY) { () =>
    memoryManager.getNettyUsedDirectMemory
  }
  workerSource.addGauge(WorkerSource.BUFFER_STREAM_READ_BUFFER) { () =>
    memoryManager.getReadBufferCounter
  }
  workerSource.addGauge(WorkerSource.READ_BUFFER_DISPATCHER_REQUESTS_LENGTH) { () =>
    memoryManager.dispatchRequestsLength
  }
  workerSource.addGauge(WorkerSource.READ_BUFFER_ALLOCATED_COUNT) { () =>
    memoryManager.getAllocatedReadBuffers
  }
  workerSource.addGauge(WorkerSource.ACTIVE_SHUFFLE_SIZE) { () =>
    storageManager.getActiveShuffleSize()
  }
  workerSource.addGauge(WorkerSource.ACTIVE_SHUFFLE_FILE_COUNT) { () =>
    storageManager.getActiveShuffleFileCount()
  }
  workerSource.addGauge(WorkerSource.PAUSE_PUSH_DATA_TIME) { () =>
    memoryManager.getPausePushDataTime
  }
  workerSource.addGauge(WorkerSource.PAUSE_PUSH_DATA_AND_REPLICATE_TIME) { () =>
    memoryManager.getPausePushDataAndReplicateTime
  }
  workerSource.addGauge(WorkerSource.PAUSE_PUSH_DATA_COUNT) { () =>
    memoryManager.getPausePushDataCounter
  }
  workerSource.addGauge(WorkerSource.PAUSE_PUSH_DATA_AND_REPLICATE_COUNT) { () =>
    memoryManager.getPausePushDataAndReplicateCounter
  }

  private def highWorkload: Boolean = {
    (memoryManager.currentServingState, conf.workerActiveConnectionMax) match {
      case (ServingState.PUSH_AND_REPLICATE_PAUSED, _) => true
      case (ServingState.PUSH_PAUSED, _) => true
      case (_, Some(activeConnectionMax)) =>
        workerSource.getCounterCount(ACTIVE_CONNECTION_COUNT) >= activeConnectionMax
      case _ => false
    }
  }

  private def heartbeatToMaster(): Unit = {
    val activeShuffleKeys = new JHashSet[String]()
    val estimatedAppDiskUsage = new JHashMap[String, JLong]()
    activeShuffleKeys.addAll(partitionLocationInfo.shuffleKeySet)
    activeShuffleKeys.addAll(storageManager.shuffleKeySet())
    storageManager.topAppDiskUsage.asScala.foreach { case (shuffleId, usage) =>
      estimatedAppDiskUsage.put(shuffleId, usage)
    }
    storageManager.updateDiskInfos()
    val diskInfos =
      workerInfo.updateThenGetDiskInfos(storageManager.disksSnapshot().map { disk =>
        disk.mountPoint -> disk
      }.toMap.asJava).values().asScala.toSeq ++ storageManager.hdfsDiskInfo
    val response = masterClient.askSync[HeartbeatFromWorkerResponse](
      HeartbeatFromWorker(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        diskInfos,
        handleResourceConsumption(),
        activeShuffleKeys,
        estimatedAppDiskUsage,
        highWorkload),
      classOf[HeartbeatFromWorkerResponse])
    response.expiredShuffleKeys.asScala.foreach(shuffleKey => workerInfo.releaseSlots(shuffleKey))
    cleanTaskQueue.put(response.expiredShuffleKeys)
    if (!response.registered) {
      logError("Worker not registered in master, clean expired shuffle data and register again.")
      try {
        registerWithMaster()
      } catch {
        case e: Throwable =>
          logError("Re-register worker failed after worker lost.", e)
          // Register to master failed then stop server
          System.exit(-1)
      }
    }
  }

  override def initialize(): Unit = {
    super.initialize()
    logInfo(s"Starting Worker $host:$pushPort:$fetchPort:$replicatePort" +
      s" with ${workerInfo.diskInfos} slots.")
    registerWithMaster()

    // start heartbeat
    sendHeartbeatTask = forwardMessageScheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError { heartbeatToMaster() }
      },
      heartbeatInterval,
      heartbeatInterval,
      TimeUnit.MILLISECONDS)

    checkFastFailTask = forwardMessageScheduler.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError {
          unavailablePeers.entrySet().forEach { entry: JMap.Entry[WorkerInfo, Long] =>
            if (System.currentTimeMillis() - entry.getValue > replicaFastFailDuration) {
              unavailablePeers.remove(entry.getKey)
            }
          }
        }
      },
      0,
      replicaFastFailDuration,
      TimeUnit.MILLISECONDS)

    cleaner.submit(new Runnable {
      override def run(): Unit = {
        while (true) {
          val expiredShuffleKeys = cleanTaskQueue.take()
          try {
            cleanup(expiredShuffleKeys, cleanThreadPool)
          } catch {
            case e: Throwable =>
              logError("Cleanup failed", e)
          }
        }
      }
    })

    pushDataHandler.init(this)
    replicateHandler.init(this)
    fetchHandler.init(this)
    controller.init(this)

    logInfo("Worker started.")
    rpcEnv.awaitTermination()
  }

  override def stop(exitKind: Int): Unit = {
    if (!stopped) {
      logInfo("Stopping Worker.")

      if (jvmQuake != null) {
        jvmQuake.stop()
      }
      if (sendHeartbeatTask != null) {
        if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN) {
          sendHeartbeatTask.cancel(false)
        } else {
          sendHeartbeatTask.cancel(true)
        }
        sendHeartbeatTask = null
      }
      if (checkFastFailTask != null) {
        if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN) {
          checkFastFailTask.cancel(false)
        } else {
          checkFastFailTask.cancel(true)
        }
        checkFastFailTask = null
      }
      if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN) {
        forwardMessageScheduler.shutdown()
        replicateThreadPool.shutdown()
        commitThreadPool.shutdown()
        asyncReplyPool.shutdown()
      } else {
        forwardMessageScheduler.shutdownNow()
        replicateThreadPool.shutdownNow()
        commitThreadPool.shutdownNow()
        asyncReplyPool.shutdownNow()
      }
      partitionsSorter.close(exitKind)
      storageManager.close(exitKind)
      memoryManager.close()
      Option(CongestionController.instance()).foreach(_.close())

      masterClient.close()
      replicateServer.shutdown(exitKind)
      fetchServer.shutdown(exitKind)
      pushServer.shutdown(exitKind)
      metricsSystem.stop()

      super.stop(exitKind)

      logInfo("Worker is stopped.")
      stopped = true
    }
  }

  private def registerWithMaster(): Unit = {
    var registerTimeout = conf.registerWorkerTimeout
    val interval = 2000
    var exception: Throwable = null
    while (registerTimeout > 0) {
      val resp =
        try {
          masterClient.askSync[PbRegisterWorkerResponse](
            RegisterWorker(
              host,
              rpcPort,
              pushPort,
              fetchPort,
              replicatePort,
              // Use WorkerInfo's diskInfo since re-register when heartbeat return not-registered,
              // StorageManager have update the disk info.
              workerInfo.diskInfos.asScala.toMap,
              handleResourceConsumption().asScala.toMap,
              MasterClient.genRequestId()),
            classOf[PbRegisterWorkerResponse])
        } catch {
          case throwable: Throwable =>
            logWarning(
              s"Register worker to master failed, will retry after ${Utils.msDurationToString(interval)}",
              throwable)
            exception = throwable
            null
        }
      // Register successfully
      if (null != resp && resp.getSuccess) {
        registered.set(true)
        logInfo("Register worker successfully.")
        return
      }
      // Register failed, sleep and retry
      Thread.sleep(interval)
      registerTimeout = registerTimeout - interval
    }
    // If worker register still failed after retry, throw exception to stop worker process
    throw new CelebornException("Register worker failed.", exception)
  }

  private def handleResourceConsumption(): util.Map[UserIdentifier, ResourceConsumption] = {
    val resourceConsumptionSnapshot = storageManager.userResourceConsumptionSnapshot()
    resourceConsumptionSnapshot.foreach { case (userIdentifier, _) =>
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.DISK_FILE_COUNT,
        userIdentifier.toMap) { () =>
        workerInfo.userResourceConsumption.get(userIdentifier).diskFileCount
      }
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.DISK_BYTES_WRITTEN,
        userIdentifier.toMap) { () =>
        workerInfo.userResourceConsumption.get(userIdentifier).diskBytesWritten
      }
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.HDFS_FILE_COUNT,
        userIdentifier.toMap) { () =>
        workerInfo.userResourceConsumption.get(userIdentifier).hdfsFileCount
      }
      resourceConsumptionSource.addGauge(
        ResourceConsumptionSource.HDFS_BYTES_WRITTEN,
        userIdentifier.toMap) { () =>
        workerInfo.userResourceConsumption.get(userIdentifier).hdfsBytesWritten
      }
    }
    workerInfo.updateThenGetUserResourceConsumption(resourceConsumptionSnapshot.asJava)
  }

  @VisibleForTesting
  def cleanup(expiredShuffleKeys: JHashSet[String], threadPool: ThreadPoolExecutor): Unit =
    synchronized {
      expiredShuffleKeys.asScala.foreach { shuffleKey =>
        partitionLocationInfo.removeShuffle(shuffleKey)
        shufflePartitionType.remove(shuffleKey)
        shufflePushDataTimeout.remove(shuffleKey)
        shuffleMapperAttempts.remove(shuffleKey)
        shuffleCommitInfos.remove(shuffleKey)
        workerInfo.releaseSlots(shuffleKey)
        logInfo(s"Cleaned up expired shuffle $shuffleKey")
      }
      partitionsSorter.cleanup(expiredShuffleKeys)
      fetchHandler.cleanupExpiredShuffleKey(expiredShuffleKeys)
      threadPool.execute(new Runnable {
        override def run(): Unit = storageManager.cleanupExpiredShuffleKey(expiredShuffleKeys)
      })
    }

  override def getWorkerInfo: String = {
    val sb = new StringBuilder
    sb.append("====================== WorkerInfo of Worker ===========================\n")
    sb.append(workerInfo.toString()).append("\n")
    sb.toString()
  }

  override def getThreadDump: String = {
    val sb = new StringBuilder
    sb.append("========================= Worker ThreadDump ==========================\n")
    sb.append(Utils.getThreadDump()).append("\n")
    sb.toString()
  }

  override def getApplicationList: String = {
    val sb = new StringBuilder
    sb.append("================= LifecycleManager Application List ======================\n")
    workerInfo.getApplicationIdSet.asScala.foreach { appId =>
      sb.append(s"$appId\n")
    }
    sb.toString()
  }

  override def getShuffleList: String = {
    val sb = new StringBuilder
    sb.append("======================= Shuffle Key List ============================\n")
    storageManager.shuffleKeySet().asScala.foreach { shuffleKey =>
      sb.append(s"$shuffleKey\n")
    }
    sb.toString()
  }

  override def isShutdown: String = {
    val sb = new StringBuilder
    sb.append("========================= Worker Shutdown ==========================\n")
    sb.append(shutdown.get()).append("\n")
    sb.toString()
  }

  override def isRegistered: String = {
    val sb = new StringBuilder
    sb.append("========================= Worker Registered ==========================\n")
    sb.append(registered.get()).append("\n")
    sb.toString()
  }

  override def listTopDiskUseApps: String = {
    val sb = new StringBuilder
    sb.append("================== Top Disk Usage Applications =======================\n")
    storageManager.topAppDiskUsage.asScala.foreach { case (appId, usage) =>
      sb.append(s"Application $appId used ${Utils.bytesToString(usage)}\n")
    }
    sb.toString()
  }

  override def listPartitionLocationInfo: String = {
    val sb = new StringBuilder
    sb.append("==================== Partition Location Info =========================\n")
    sb.append(partitionLocationInfo.toString).append("\n")
    sb.toString()
  }

  override def getUnavailablePeers: String = {
    val sb = new StringBuilder
    sb.append("==================== Unavailable Peers of Worker =====================\n")
    unavailablePeers.asScala.foreach { case (peer, time) =>
      sb.append(s"${peer.toUniqueId().padTo(50, " ").mkString}${Utils.formatTimestamp(time)}\n")
    }
    sb.toString()
  }

  override def exit(exitType: String): String = {
    exitType.toUpperCase(Locale.ROOT) match {
      case "DECOMMISSION" =>
        exitKind = CelebornExitKind.WORKER_DECOMMISSION
        ShutdownHookManager.get().updateTimeout(
          conf.workerDecommissionForceExitTimeout,
          TimeUnit.MILLISECONDS)
      case "GRACEFUL" =>
        exitKind = CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN
      case "IMMEDIATELY" =>
        exitKind = CelebornExitKind.EXIT_IMMEDIATELY
      case _ => // Use origin code
    }
    // Use the original EXIT_CODE
    new Thread() {
      override def run(): Unit = {
        Thread.sleep(10000)
        System.exit(0)
      }
    }.start()
    val sb = new StringBuilder
    sb.append("============================ Exit Worker =============================\n")
    sb.append(s"Exit worker by $exitType triggered: \n")
    sb.append(workerInfo.toString()).append("\n")
    sb.toString()
  }

  def shutdownGracefully(): Unit = {
    // During shutdown, to avoid allocate slots in this worker,
    // add this worker to master's excluded list. When restart, register worker will
    // make master remove this worker from excluded list.
    try {
      masterClient.askSync(
        ReportWorkerUnavailable(List(workerInfo).asJava),
        OneWayMessageResponse.getClass)
    } catch {
      case e: Throwable =>
        logError(
          s"Fail report to master, need wait PartitionLocation auto release: \n$partitionLocationInfo",
          e)
    }
    shutdown.set(true)
    val interval = conf.workerGracefulShutdownCheckSlotsFinishedInterval
    val timeout = conf.workerGracefulShutdownCheckSlotsFinishedTimeoutMs
    var waitTimes = 0

    def waitTime: Long = waitTimes * interval

    while (!partitionLocationInfo.isEmpty && waitTime < timeout) {
      Thread.sleep(interval)
      waitTimes += 1
      logWarning(
        s"Wait partitionLocation empty, current ${partitionLocationInfo.toStringSimplified}")
    }
    if (partitionLocationInfo.isEmpty) {
      logInfo(s"Waiting for all PartitionLocation released cost ${waitTime}ms.")
    } else {
      logWarning(s"Waiting for all PartitionLocation release cost ${waitTime}ms, " +
        s"unreleased PartitionLocation: \n$partitionLocationInfo")
    }
  }

  def decommissionWorker(): Unit = {
    try {
      masterClient.askSync(
        ReportWorkerUnavailable(List(workerInfo).asJava),
        OneWayMessageResponse.getClass)
    } catch {
      case e: Throwable =>
        logError(
          s"Fail report to master, need wait registered shuffle expired: " +
            s"\n${storageManager.shuffleKeySet().asScala.mkString("[", ", ", "]")}",
          e)
    }
    shutdown.set(true)
    val interval = conf.workerDecommissionCheckInterval
    val timeout = conf.workerDecommissionForceExitTimeout
    var waitTimes = 0

    def waitTime: Long = waitTimes * interval

    while (!storageManager.shuffleKeySet().isEmpty && waitTime < timeout) {
      Thread.sleep(interval)
      waitTimes += 1
    }
    if (storageManager.shuffleKeySet().isEmpty) {
      logInfo(s"Waiting for all shuffle expired cost ${waitTime}ms.")
    } else {
      logWarning(s"Waiting for all shuffle expired cost ${waitTime}ms, " +
        s"unreleased shuffle: \n${storageManager.shuffleKeySet().asScala.mkString("[", ", ", "]")}")
    }
  }

  def exitImmediately(): Unit = {
    // During shutdown, to avoid allocate slots in this worker,
    // add this worker to master's excluded list. When restart, register worker will
    // make master remove this worker from excluded list.
    try {
      masterClient.askSync[PbWorkerLostResponse](
        WorkerLost(
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          MasterClient.genRequestId()),
        classOf[PbWorkerLostResponse])
    } catch {
      case e: Throwable =>
        logError(
          s"Fail report to master, need wait PartitionLocation auto release: \n$partitionLocationInfo",
          e)
    }
    shutdown.set(true)
  }

  ShutdownHookManager.get().addShutdownHook(
    new Thread(new Runnable {
      override def run(): Unit = {
        logInfo("Shutdown hook called.")
        exitKind match {
          case CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN =>
            logInfo("Worker start to shutdown gracefully")
            shutdownGracefully()
          case CelebornExitKind.WORKER_DECOMMISSION =>
            logInfo("Worker start to decommission")
            decommissionWorker()
          case _ =>
            logInfo("Worker start to exit immediately")
            exitImmediately()
        }
        stop(exitKind)
      }
    }),
    WORKER_SHUTDOWN_PRIORITY)

  @VisibleForTesting
  def getPushFetchServerPort: (Int, Int) = (pushPort, fetchPort)
}

private[deploy] object Worker extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new CelebornConf
    val workerArgs = new WorkerArguments(args, conf)
    // There are many entries for setting the master address, and we should unify the entries as
    // much as possible. Therefore, if the user manually specifies the address of the Master when
    // starting the Worker, we should set it in the parameters and automatically calculate what the
    // address of the Master should be used in the end.
    workerArgs.master.foreach { master =>
      conf.set(MASTER_ENDPOINTS.key, RpcAddress.fromCelebornURL(master).hostPort)
    }

    try {
      val worker = new Worker(conf, workerArgs)
      worker.initialize()
    } catch {
      case e: Throwable =>
        logError("Initialize worker failed.", e)
        System.exit(-1)
    }

  }
}
