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

package com.aliyun.emr.rss.service.deploy.master

import java.util
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.RssConf.haEnabled
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.meta.WorkerInfo
import com.aliyun.emr.rss.common.network.protocol.RssMessage
import com.aliyun.emr.rss.common.protocol.{PartitionLocation, RpcNameConstants}
import com.aliyun.emr.rss.common.protocol.RssMessages._
import com.aliyun.emr.rss.common.protocol.RssMessages.MessageType._
import com.aliyun.emr.rss.common.protocol.RssMessages.StatusCode.{Failed, SlotNotAvailable, Success}
import com.aliyun.emr.rss.common.rpc._
import com.aliyun.emr.rss.common.util.{ThreadUtils, Utils}
import com.aliyun.emr.rss.common.util.Utils.WorkerResource
import com.aliyun.emr.rss.server.common.http.{HttpServer, HttpServerInitializer}
import com.aliyun.emr.rss.server.common.metrics.MetricsSystem
import com.aliyun.emr.rss.service.deploy.master.clustermeta.SingleMasterMetaManager
import com.aliyun.emr.rss.service.deploy.master.clustermeta.ha.{HAHelper, HAMasterMetaManager, MetaHandler}
import com.aliyun.emr.rss.service.deploy.master.http.HttpRequestHandler

private[deploy] class Master(
                              override val rpcEnv: RpcEnv,
                              address: RpcAddress,
                              val conf: RssConf,
                              val metricsSystem: MetricsSystem)
  extends RpcEndpoint with Logging {

  private val statusSystem = if (haEnabled(conf)) {
    val sys = new HAMasterMetaManager(rpcEnv, conf)
    val handler = new MetaHandler(sys)
    handler.setUpMasterRatisServer(conf)
    sys
  } else {
    new SingleMasterMetaManager(rpcEnv, conf)
  }

  // Threads
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _
  private var checkForApplicationTimeOutTask: ScheduledFuture[_] = _
  private val nonEagerHandler = ThreadUtils.newDaemonCachedThreadPool("master-noneager-handler", 64)

  // Config constants
  private val WorkerTimeoutMs = RssConf.workerTimeoutMs(conf)
  private val ApplicationTimeoutMs = RssConf.applicationTimeoutMs(conf)

  // States
  private def workersSnapShot: util.List[WorkerInfo] =
    statusSystem.workers.synchronized(new util.ArrayList[WorkerInfo](statusSystem.workers))

  // init and register master metrics
  private val masterSource = {
    val source = new MasterSource(conf)
    source.addGauge(MasterSource.RegisteredShuffleCount,
      _ => statusSystem.registeredShuffle.size())
    // blacklist worker count
    source.addGauge(MasterSource.BlacklistedWorkerCount,
      _ => statusSystem.blacklist.size())
    // worker count
    source.addGauge(MasterSource.WorkerCount,
      _ => statusSystem.workers.size())
    val (totalSlots, usedSlots, overloadWorkerCount, _) = getClusterLoad
    // worker slots count
    source.addGauge(MasterSource.WorkerSlotsCount, _ => totalSlots)
    // worker slots used count
    source.addGauge(MasterSource.WorkerSlotsUsedCount, _ => usedSlots)
    // slots overload worker count
    source.addGauge(MasterSource.OverloadWorkerCount, _ => overloadWorkerCount)

    metricsSystem.registerSource(source)
    source
  }

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(RssMessage.newMessage().ptype(CHECK_WORKER_TIMEOUT))
      }
    }, 0, WorkerTimeoutMs, TimeUnit.MILLISECONDS)

    checkForApplicationTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(RssMessage.newMessage().ptype(CHECK_APPLICATION_TIMEOUT))
      }
    }, 0, ApplicationTimeoutMs / 2, TimeUnit.MILLISECONDS)
  }

  override def onStop(): Unit = {
    logInfo("Stopping RSS Master.")
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    if (checkForApplicationTimeOutTask != null) {
      checkForApplicationTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    logInfo("RSS Master is stopped.")
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    // The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"Client $address got disassociated.")
  }

  def executeWithLeaderChecker[T](context: RpcCallContext, f: => T): Unit =
    if (HAHelper.checkShouldProcess(context, statusSystem)) f

  override def receive: PartialFunction[Any, Unit] = {
    case message: RssMessage =>
      message.getType match {
        case CHECK_WORKER_TIMEOUT =>
          logDebug("Received CheckForWorkerTimeOut request.")
          executeWithLeaderChecker(null, timeoutDeadWorkers())
        case CHECK_APPLICATION_TIMEOUT =>
          logDebug("Received CheckForApplicationTimeOut request.")
          executeWithLeaderChecker(null, timeoutDeadApplications())
        case WORKER_LOST =>
          val workerLost = WorkerLost.parseFrom(message.getProto)
          logDebug(s"Received worker lost ${workerLost.getHost}:${workerLost.getRpcPort}:" +
            s"${workerLost.getPushPort}:${workerLost.getFetchPort}.")
          executeWithLeaderChecker(null
            , handleWorkerLost(null, workerLost.getHost, workerLost.getRpcPort,
              workerLost.getPushPort, workerLost.getFetchPort))
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message: RssMessage =>
      message.getType match {
        case HEARTBEAT_FROM_APPLICATION =>
          val heartBeatFromApplication = HeartBeatFromApplication.parseFrom(message.getProto)
          logDebug(s"Received heartbeat from app ${heartBeatFromApplication.getAppId}")
          executeWithLeaderChecker(context,
            handleHeartBeatFromApplication(context, heartBeatFromApplication.getAppId))
        case REGISTER_WORKER =>
          val registerWorker = RegisterWorker.parseFrom(message.getProto)
          logDebug(s"Received RegisterWorker request " +
            s"${registerWorker.getHost}:${registerWorker.getPushPort}:" +
            s"${registerWorker.getNumSlots}.")
          executeWithLeaderChecker(context,
            handleRegisterWorker(context, registerWorker.getHost, registerWorker.getRpcPort,
              registerWorker.getPushPort, registerWorker.getFetchPort, registerWorker.getNumSlots))
        case REQUEST_SLOTS =>
          logDebug(s"Received RequestSlots request.")
          val requestSlots = RequestSlots.parseFrom(message.getProto)
          executeWithLeaderChecker(context, handleRequestSlots(context, requestSlots))
        case RELEASE_SLOTS =>
          val releaseSlots = ReleaseSlots.parseFrom(message.getProto)
          logDebug(s"Received ReleaseSlots request  ${releaseSlots.getApplicationId}," +
            s" ${releaseSlots.getShuffleId},workers " +
            s"${releaseSlots.getWorkerIdsList.asScala.mkString(",")}, " +
            s"slots ${releaseSlots.getSlotsList.asScala.mkString(",")}")
          executeWithLeaderChecker(context,
            handleReleaseSlots(context, releaseSlots.getApplicationId, releaseSlots.getShuffleId,
              releaseSlots.getWorkerIdsList, releaseSlots.getSlotsList))
        case UNREGISTER_SHUFFLE =>
          val unregisterShuffle = UnregisterShuffle.parseFrom(message.getProto)
          logDebug(s"Received UnregisterShuffle request , ${unregisterShuffle.getAppId}," +
            s" ${unregisterShuffle.getShuffleId}")
          executeWithLeaderChecker(context, handleUnregisterShuffle(context,
              unregisterShuffle.getAppId, unregisterShuffle.getShuffleId))
        case GET_BLACKLIST =>
          val getBlacklist = GetBlacklist.parseFrom(message.getProto)
          logDebug(s"Received Blacklist request")
          executeWithLeaderChecker(context, handleGetBlacklist(context, getBlacklist))
        case APPLICATION_LOST =>
          val applicationlost = ApplicationLost.parseFrom(message.getProto)
          logDebug(s"Received ApplicationLost request ${applicationlost.getAppId}.")
          executeWithLeaderChecker(context,
            handleApplicationLost(context, applicationlost.getAppId))
        case HEARTBEAT_FROM_WORKER =>
          val heartbeatFromWorker = HeartbeatFromWorker.parseFrom(message.getProto)
          logDebug(s"Received heartbeat from worker ${heartbeatFromWorker.getHost}" +
            s":${heartbeatFromWorker.getRpcPort}:${heartbeatFromWorker.getPushPort}" +
            s":${heartbeatFromWorker.getFetchPort}.")
          executeWithLeaderChecker(context, handleHeartBeatFromWorker(
            context, heartbeatFromWorker.getHost, heartbeatFromWorker.getRpcPort,
            heartbeatFromWorker.getPushPort, heartbeatFromWorker.getFetchPort,
            heartbeatFromWorker.getNumSlots, heartbeatFromWorker.getShuffleKeysList.asScala.toSet))
        case GET_WORKER_INFO =>
          logDebug("Received GetWorkerInfos request")
          executeWithLeaderChecker(context, handleGetWorkerInfos(context))
        case REPORT_WORKER_FAILURE =>
          logDebug("Received ReportNodeFailure request ")
          val reportWorkerFailure = ReportWorkerFailure.parseFrom(message.getProto)
          executeWithLeaderChecker(context,
            handleReportNodeFailure(context, Utils.convertPbWorkerInfosToWorkerInfos(
                reportWorkerFailure.getFailedList.asScala.toList).toList))
        case GET_CLUSTER_LOAD_STATUS =>
          logInfo(s"Received GetClusterLoad request")
          executeWithLeaderChecker(context, handleGetClusterLoadStatus(context))
      }
  }

  private def timeoutDeadWorkers() {
    val currentTime = System.currentTimeMillis()
    var ind = 0
    workersSnapShot.asScala.foreach { worker =>
      if (worker.lastHeartbeat < currentTime - WorkerTimeoutMs
        && !statusSystem.workerLostEvents.contains(worker)) {
        logWarning(s"Worker ${worker.readableAddress()} timeout! Trigger WorkerLost event.")
        self.send(RssMessage.newMessage().ptype(WORKER_LOST).proto(WorkerLost.newBuilder()
          .setHost(worker.host).setRpcPort(worker.rpcPort).setPushPort(worker.pushPort)
          .setFetchPort(worker.fetchPort).build()))
      }
      ind += 1
    }
  }

  private def timeoutDeadApplications(): Unit = {
    val currentTime = System.currentTimeMillis()
    statusSystem.appHeartbeatTime.keySet().asScala.foreach { key =>
      if (statusSystem.appHeartbeatTime.get(key) < currentTime - ApplicationTimeoutMs) {
        logWarning(s"Application $key timeout, trigger applicationLost event.")
        var res = ApplicationLostResponse.parseFrom(self.askSync[RssMessage]
          (RssMessage.newMessage().ptype(APPLICATION_LOST).proto(ApplicationLost.newBuilder()
            .setAppId(key).build())).getProto)
        var retry = 1
        while (res.getStatus != Success && retry <= 3) {
          res = ApplicationLostResponse.parseFrom(self.askSync[RssMessage](RssMessage.newMessage()
            .ptype(APPLICATION_LOST).proto(ApplicationLost.newBuilder().setAppId(key).build()))
            .getProto)
          retry += 1
        }
        if (retry > 3) {
          logWarning(s"Handle ApplicationLost event for $key failed more than 3 times!")
        }
      }
    }
  }

  private def handleHeartBeatFromWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      numSlots: Int,
      shuffleKeys: Set[String]): Unit = {
    val targetWorker = new WorkerInfo(host, rpcPort,
      pushPort, fetchPort, -1, null)
    val worker: WorkerInfo = workersSnapShot
      .asScala
      .find(_ == targetWorker)
      .orNull
    if (worker == null) {
      logWarning(
        s"""Received heartbeat from unknown worker!
           | Worker details :  $host:$rpcPort$pushPort$fetchPort.""".stripMargin)
      return
    }

    worker.setupEndpoint(rpcEnv.setupEndpointRef(RpcAddress(host, rpcPort),
      RpcNameConstants.WORKER_EP))

    statusSystem.handleWorkerHeartBeat(host, rpcPort, pushPort, fetchPort, numSlots,
      System.currentTimeMillis())

    val expiredShuffleKeys = new util.HashSet[String]
    shuffleKeys.foreach { shuffleKey =>
      if (!statusSystem.registeredShuffle.contains(shuffleKey)) {
        logWarning(s"Shuffle $shuffleKey expired on $host:$rpcPort:$pushPort:$fetchPort.")
        expiredShuffleKeys.add(shuffleKey)
      }
    }
    context.reply(RssMessage.newMessage().ptype(HEARTBEAT_RESPONSE).proto(HeartbeatResponse
      .newBuilder().addAllExpiredShuffleKeys(expiredShuffleKeys).build()))
  }

  private def handleWorkerLost(context: RpcCallContext, host: String, rpcPort: Int, pushPort: Int,
    fetchPort: Int): Unit = {
    val targetWorker = new WorkerInfo(host,
      rpcPort, pushPort, fetchPort, -1, null)
    val worker: WorkerInfo = workersSnapShot
      .asScala
      .find(_ == targetWorker)
      .orNull
    if (worker == null) {
      logWarning(s"Unknown worker $host:$rpcPort:$pushPort:$fetchPort" +
        s" for WorkerLost handler!")
      return
    }

    statusSystem.handleWorkerLost(host, rpcPort, pushPort, fetchPort)

    if (context != null) {
      context.reply(RssMessage.newMessage().ptype(WORKER_LOST_RESPONSE)
        .proto(WorkerLostResponse.newBuilder().setSuccess(true).build()))
    }
  }

  def handleRegisterWorker(
      context: RpcCallContext,
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
    numSlots: Int): Unit = {
    val workerToRegister = new WorkerInfo(host, rpcPort,
      pushPort, fetchPort, numSlots, null)
    val hostPort = workerToRegister.pushPort
    if (workersSnapShot.contains(workerToRegister)) {
      logWarning(s"Receive RegisterWorker while worker" +
        s" ${workerToRegister.toString()} already exists,trigger WorkerLost.")
      if (!statusSystem.workerLostEvents.contains(hostPort)) {
        self.send(RssMessage.newMessage().ptype(WORKER_LOST))
      }
      context.reply(RssMessage.newMessage().ptype(REGISTER_WORKER_RESPONSE)
        .proto(RegisterWorkerResponse.newBuilder().setSuccess(false)
          .setMessage("Worker already registered!").build()))
    } else if (statusSystem.workerLostEvents.contains(hostPort)) {
      logWarning(s"Receive RegisterWorker while worker $hostPort in workerLostEvents.")
      context.reply(RssMessage.newMessage().ptype(REGISTER_WORKER_RESPONSE)
        .proto(RegisterWorkerResponse.newBuilder().setSuccess(false)
          .setMessage("Worker in workerLostEvents.").build()))
    } else {
      statusSystem.handleRegisterWorker(host, rpcPort, pushPort, fetchPort, numSlots)
      logInfo(s"Registered worker $workerToRegister.")
      context.reply(RssMessage.newMessage().proto(RegisterWorkerResponse.newBuilder()
        .setSuccess(true).build()).ptype(REGISTER_WORKER_RESPONSE))
    }
  }

  def handleRequestSlots(context: RpcCallContext, requestSlots: RequestSlots): Unit = {
    val numReducers = requestSlots.getReduceIdListCount
    val shuffleKey = Utils.makeShuffleKey(requestSlots.getApplicationId, requestSlots.getShuffleId)

    val slots = statusSystem.workers.synchronized {
      MasterUtil.offerSlots(
        shuffleKey,
        workersNotBlacklisted(),
        requestSlots.getReduceIdListList,
        requestSlots.getShouldReplicate
      )
    }

    // reply false if offer slots failed
    if (slots == null || slots.isEmpty) {
      logError(s"Offer slots for $numReducers reducers of $shuffleKey failed!")
      context.reply(RssMessage.newMessage().ptype(REQUEST_SLOTS_RESPONSE)
        .proto(RequestSlotsResponse.newBuilder().setStatus(SlotNotAvailable).build()))
      return
    }

    // register shuffle success, update status
    statusSystem.handleRequestSlots(shuffleKey, requestSlots.getHostname,
      Utils.workerToAllocatedSlots(slots.asInstanceOf[WorkerResource]))

    logInfo(s"Offer slots successfully for $numReducers reducers of $shuffleKey" +
      s" on ${slots.size()} workers.")

    val workersNotSelected = workersNotBlacklisted().asScala.filter(!slots.containsKey(_))
    val extraSlotsSize = Math.min(RssConf.offerSlotsExtraSize(conf), workersNotSelected.size)
    if (extraSlotsSize > 0) {
      var index = Random.nextInt(workersNotSelected.size)
      (1 to extraSlotsSize).foreach(_ => {
        slots.put(workersNotSelected(index),
          (new util.ArrayList[PartitionLocation](), new util.ArrayList[PartitionLocation]()))
        index = (index + 1) % workersNotSelected.size
      })
      logInfo(s"Offered extra $extraSlotsSize slots for $shuffleKey")
    }

    context.reply(RssMessage.newMessage().ptype(REQUEST_SLOTS_RESPONSE)
      .proto(RequestSlotsResponse.newBuilder().setStatus(Success)
        .putAllWorkerResource(Utils.convertWorkerResourceToPbWorkerResource(
          slots.asInstanceOf[WorkerResource])).build()))
  }

  def handleReleaseSlots(
      context: RpcCallContext,
      applicationId: String,
      shuffleId: Int,
      workerIds: util.List[String],
      slots: util.List[Integer]): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    statusSystem.handleReleaseSlots(shuffleKey, workerIds, slots)
    logInfo(s"[handleReleaseSlots] Release all slots of $shuffleKey")
    context.reply(RssMessage.newMessage().ptype(RELEASE_SLOTS_RESPONSE).proto(ReleaseSlotsResponse
      .newBuilder().setStatus(Success).build()))
  }

  def handleUnregisterShuffle(
    context: RpcCallContext,
    applicationId: String,
    shuffleId: Int): Unit = {
    val shuffleKey = Utils.makeShuffleKey(applicationId, shuffleId)
    statusSystem.handleUnRegisterShuffle(shuffleKey)
    logInfo(s"Unregister shuffle $shuffleKey")
    context.reply(RssMessage.newMessage().ptype(UNREGISTER_SHUFFLE_RESPONSE)
      .proto(UnregisterShuffleResponse.newBuilder().setStatus(Success).build()))
  }

  def handleGetBlacklist(context: RpcCallContext, msg: GetBlacklist): Unit = {
    msg.getLocalBlackListList.removeAll(workersSnapShot)
    context.reply(RssMessage.newMessage().ptype(GET_BLACKLIST_RESPONSE).proto(GetBlacklistResponse
      .newBuilder().addAllBlacklist(Utils.convertWorkerInfosToPbWorkerInfos(statusSystem.blacklist
      .asScala.toList).asJava).build()))
  }

  private def handleGetWorkerInfos(context: RpcCallContext): Unit = {
    context.reply(RssMessage.newMessage().ptype(GET_WORKER_INFO_RESPONSE)
      .proto(GetWorkerInfosResponse.newBuilder().setStatus(Success)
        .addAllWorkerInfos(Utils.convertWorkerInfosToPbWorkerInfos(workersSnapShot.asScala.toList)
          .asJava).build()))
  }

  private def handleReportNodeFailure(context: RpcCallContext,
                                      failedWorkers: List[WorkerInfo]): Unit = {
    logInfo(s"Receive ReportNodeFailure $failedWorkers, current blacklist" +
        s"${statusSystem.blacklist}")
    statusSystem.handleReportWorkerFailure(failedWorkers.asJava)
    context.reply(RssMessage.newMessage().ptype(ONE_WAY_MESSAGE_RESPONSE))
  }

  def handleApplicationLost(context: RpcCallContext, appId: String): Unit = {
    nonEagerHandler.submit(new Runnable {
      override def run(): Unit = {
        statusSystem.handleAppLost(appId)
        logInfo(s"Removed application $appId")
        context.reply(RssMessage.newMessage().ptype(APPLICATION_LOST_RESPONSE)
          .proto(ApplicationLostResponse.newBuilder().setStatus(Success).build()))
      }
    })
  }

  private def handleHeartBeatFromApplication(context: RpcCallContext, appId: String): Unit = {
    statusSystem.handleAppHeartbeat(appId, System.currentTimeMillis())
    context.reply(RssMessage.newMessage().ptype(ONE_WAY_MESSAGE_RESPONSE))
  }

  private def handleGetClusterLoadStatus(context: RpcCallContext): Unit = {
    val (_, _, _, result) = getClusterLoad
    context.reply(RssMessage.newMessage().ptype(GET_CLUSTER_LOAD_STATUS_RESPONSE)
      .proto(GetClusterLoadStatusResponse.newBuilder().setIsOverload(result).build()))
  }

  private def getClusterLoad: (Int, Int, Int, Boolean) = {
    if (workersSnapShot.isEmpty) {
      return (0, 0, 0, false)
    }

    val clusterSlotsUsageLimit: Double = RssConf.clusterSlotsUsageLimitPercent(conf)

    val (totalSlots, usedSlots, overloadWorkers) = workersSnapShot.asScala.map(workerInfo => {
      val allSlots: Int = workerInfo.numSlots
      val usedSlots: Int = workerInfo.usedSlots()
      val flag: Int = if (usedSlots/allSlots.toDouble >= clusterSlotsUsageLimit) 1 else 0
      (allSlots, usedSlots, flag)
    }).reduce((pair1, pair2) => {
      (pair1._1 + pair2._1, pair1._2 + pair2._2, pair1._3 + pair2._3)
    })

    val totalUsedRatio: Double = usedSlots / totalSlots.toDouble
    val result = totalUsedRatio >= clusterSlotsUsageLimit
    logInfo(s"Current cluster slots usage:$totalUsedRatio, conf:$clusterSlotsUsageLimit, " +
        s"overload:$result")
    (totalSlots, usedSlots, overloadWorkers, result)
  }

  private def workersNotBlacklisted(
      tmpBlacklist: Set[WorkerInfo] = Set.empty): util.List[WorkerInfo] = {
    workersSnapShot.asScala.filter { w =>
      !statusSystem.blacklist.contains(w) && !tmpBlacklist.contains(w)
    }.asJava
  }

  def getWorkerInfos: String = {
    val sb = new StringBuilder
    workersSnapShot.asScala.foreach { w =>
      sb.append("==========WorkerInfos in Master==========\n")
      sb.append(w).append("\n")

      val workerInfo = Utils.convertPbWorkerInfosToWorkerInfos(requestGetWorkerInfos(w.endpoint)
        .getWorkerInfosList.asScala.toList).asInstanceOf[util.List[WorkerInfo]]
        .get(0)

      sb.append("==========WorkerInfos in Workers==========\n")
      sb.append(workerInfo).append("\n")

      if (w.hasSameInfoWith(workerInfo)) {
        sb.append("Consist!").append("\n")
      } else {
        sb.append("[ERROR] Inconsistent!").append("\n")
      }
    }

    sb.toString()
  }

  def getThreadDump: String = {
    val sb = new StringBuilder
    val threadDump = Utils.getThreadDump()
    sb.append("==========Master ThreadDump==========\n")
    sb.append(threadDump).append("\n")
    workersSnapShot.asScala.foreach(w => {
      sb.append(s"==========Worker ${w.readableAddress()} ThreadDump==========\n")
      val res = requestThreadDump(w.endpoint)
      sb.append(res.getThreadDump).append("\n")
    })

    sb.toString()
  }

  def getHostnameList: String = {
    statusSystem.hostnameSet.asScala.mkString(",")
  }

  private def requestGetWorkerInfos(endpoint: RpcEndpointRef): GetWorkerInfosResponse = {
    try {
      GetWorkerInfosResponse.parseFrom(endpoint.askSync[RssMessage](RssMessage.newMessage()
        .ptype(GET_WORKER_INFO)).getProto)
    } catch {
      case e: Exception =>
        logError(s"AskSync GetWorkerInfos failed.", e)
        val result = new util.ArrayList[WorkerInfo]
        result.add(new WorkerInfo("unknown", -1, -1, -1, 0, null))
        GetWorkerInfosResponse.newBuilder().setStatus(Failed).addAllWorkerInfos(Utils.
          convertWorkerInfosToPbWorkerInfos(result.asScala.toList).asJava).build()
    }
  }

  private def requestThreadDump(endpoint: RpcEndpointRef): ThreadDumpResponse = {
    try {
      ThreadDumpResponse.parseFrom(endpoint.askSync[RssMessage](RssMessage.newMessage()
        .ptype(THREAD_DUMP)).getProto)
    } catch {
      case e: Exception =>
        logError(s"AskSync ThreadDump failed.", e)
        ThreadDumpResponse.newBuilder().setThreadDump("Unknown").build()
    }
  }
}

private[deploy] object Master extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new RssConf()

    val metricsSystem = MetricsSystem.createMetricsSystem("master", conf, MasterSource.ServletPath)

    val masterArgs = new MasterArguments(args, conf)
    val rpcEnv = RpcEnv.create(
      RpcNameConstants.MASTER_SYS,
      masterArgs.host,
      masterArgs.host,
      masterArgs.port,
      conf,
      Math.max(64, Runtime.getRuntime.availableProcessors()))
    val master = new Master(rpcEnv, rpcEnv.address, conf, metricsSystem)
    rpcEnv.setupEndpoint(RpcNameConstants.MASTER_EP, master)

    val handlers = if (RssConf.metricsSystemEnable(conf)) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
      new HttpRequestHandler(master, metricsSystem.getPrometheusHandler)
    } else {
      new HttpRequestHandler(master, null)
    }

    val httpServer = new HttpServer(new HttpServerInitializer(handlers),
      RssConf.masterPrometheusMetricPort(conf))
    httpServer.start()

    rpcEnv.awaitTermination()
  }
}
