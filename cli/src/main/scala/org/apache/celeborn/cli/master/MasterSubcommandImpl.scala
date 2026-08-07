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

package org.apache.celeborn.cli.master

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import picocli.CommandLine.{Command, ParameterException}

import org.apache.celeborn.cli.config.CliConfigManager
import org.apache.celeborn.rest.v1.model._
import org.apache.celeborn.rest.v1.model.SendWorkerEventRequest.EventTypeEnum

@Command(name = "master")
class MasterSubcommandImpl extends MasterSubcommand {
  override def run(): Unit = {
    if (masterOptions.showMastersInfo) log(runShowMastersInfo)
    if (masterOptions.showClusterApps) log(runShowClusterApps)
    if (masterOptions.showClusterAppsInfo) log(runShowClusterAppsInfo)
    if (masterOptions.showClusterShuffles) log(runShowClusterShuffles)
    if (masterOptions.unregisterShuffles) log(runUnregisterShuffles)
    if (masterOptions.excludeWorkers) log(runExcludeWorkers)
    if (masterOptions.removeExcludedWorkers) log(runRemoveExcludedWorkers)
    if (masterOptions.removeWorkersUnavailableInfo) log(runRemoveWorkersUnavailableInfo)
    if (masterOptions.sendWorkerEvent != null && masterOptions.sendWorkerEvent.nonEmpty)
      log(runSendWorkerEvent)
    if (masterOptions.showWorkerEventInfo) log(runShowWorkerEventInfo)
    if (masterOptions.showLostWorkers) log(runShowLostWorkers)
    if (masterOptions.showExcludedWorkers) log(runShowExcludedWorkers)
    if (masterOptions.showManualExcludedWorkers) log(runShowManualExcludedWorkers)
    if (masterOptions.showShutdownWorkers) log(runShowShutdownWorkers)
    if (masterOptions.showDecommissioningWorkers) log(runShowDecommissioningWorkers)
    if (masterOptions.showLifecycleManagers) log(runShowLifecycleManagers)
    if (masterOptions.showWorkers) log(runShowWorkers)
    if (masterOptions.showWorkersTopology) log(runShowWorkersTopology)
    if (masterOptions.showConf) log(runShowConf)
    if (masterOptions.showContainerInfo) log(runShowContainerInfo)
    if (masterOptions.showDynamicConf) log(runShowDynamicConf)
    if (masterOptions.upsertDynamicConf) log(runUpsertDynamicConf)
    if (masterOptions.deleteDynamicConf) log(runDeleteDynamicConf)
    if (masterOptions.showThreadDump) log(runShowThreadDump)
    if (masterOptions.showLoggers) log(runShowLoggers)
    if (masterOptions.setLogLevel) log(runSetLogLevel)
    if (masterOptions.reviseLostShuffles) log(reviseLostShuffles)
    if (masterOptions.deleteApps) log(deleteApps)
    if (!StringUtils.isBlank(masterOptions.updateInterruptionNotices))
      log(updateInterruptionNotices)
    if (masterOptions.ratisElectionTransfer) log(runRatisElectionTransfer)
    if (masterOptions.ratisElectionStepDown) log(runRatisElectionStepDown)
    if (masterOptions.ratisElectionPause) log(runRatisElectionPause)
    if (masterOptions.ratisElectionResume) log(runRatisElectionResume)
    if (masterOptions.ratisPeerAdd) log(runRatisPeerAdd)
    if (masterOptions.ratisPeerRemove) log(runRatisPeerRemove)
    if (masterOptions.ratisPeerSetPriority) log(runRatisPeerSetPriority)
    if (masterOptions.ratisSnapshotCreate) log(runRatisSnapshotCreate)
    if (!StringUtils.isBlank(masterOptions.ratisDownloadRaftMetaConf))
      runRatisDownloadRaftMetaConf
    if (!StringUtils.isBlank(masterOptions.ratisGenerateNewRaftMetaConf))
      runRatisGenerateNewRaftMetaConf
    if (masterOptions.addClusterAlias != null && masterOptions.addClusterAlias.nonEmpty)
      runAddClusterAlias
    if (masterOptions.removeClusterAlias != null && masterOptions.removeClusterAlias.nonEmpty)
      runRemoveClusterAlias
  }

  private[master] def runShowMastersInfo: MasterInfoResponse =
    masterApi.getMasterGroupInfo(commonOptions.getAuthHeader)

  private[master] def runShowClusterApps: ApplicationsHeartbeatResponse =
    applicationApi.getApplications(commonOptions.getAuthHeader)

  private[master] def runShowClusterAppsInfo: ApplicationInfoResponse =
    applicationApi.getApplicationsInfo(commonOptions.getAuthHeader)

  private[master] def runShowClusterShuffles: ShufflesResponse =
    shuffleApi.getShuffles(commonOptions.getAuthHeader)

  private[master] def runUnregisterShuffles: HandleResponse = {
    val (appId, shuffleIds) = getSingleAppShuffleIds
    if (shuffleIds.asScala.exists(_ < 0)) {
      throw new ParameterException(
        spec.commandLine(),
        "Shuffle ids must be nonnegative.")
    }

    val request = new UnregisterShufflesRequest()
      .appId(appId)
      .shuffleIds(shuffleIds)
    shuffleApi.unregisterShuffles(request, commonOptions.getAuthHeader)
  }

  private[master] def runExcludeWorkers: HandleResponse = {
    val workerIds = getWorkerIds
    val excludeWorkerRequest = new ExcludeWorkerRequest().add(workerIds)
    logInfo(s"Sending exclude worker requests to master for the following workers: $workerIds")
    workerApi.excludeWorker(excludeWorkerRequest, commonOptions.getAuthHeader)
  }

  private[master] def runRemoveExcludedWorkers: HandleResponse = {
    val workerIds = getWorkerIds
    val removeExcludeWorkerRequest = new ExcludeWorkerRequest().remove(workerIds)
    logInfo(
      s"Sending remove exclude worker requests to master for the following workers: $workerIds")
    workerApi.excludeWorker(removeExcludeWorkerRequest, commonOptions.getAuthHeader)
  }

  private[master] def runRemoveWorkersUnavailableInfo: HandleResponse = {
    val workerIds = getWorkerIds
    val removeWorkersUnavailableInfoRequest =
      new RemoveWorkersUnavailableInfoRequest().workers(workerIds)
    logInfo(
      s"Sending remove workers unavailable info requests to master for the following workers: $workerIds")
    workerApi.removeWorkersUnavailableInfo(
      removeWorkersUnavailableInfoRequest,
      commonOptions.getAuthHeader)
  }

  private[master] def runSendWorkerEvent: HandleResponse = {
    val eventType = {
      try {
        EventTypeEnum.valueOf(masterOptions.sendWorkerEvent.toUpperCase)
      } catch {
        case _: IllegalArgumentException => throw new ParameterException(
            spec.commandLine(),
            "Worker event type must be " +
              EventTypeEnum.values().toStream.map(_.name()).mkString(","))
      }
    }
    val workerIds = getWorkerIds
    val sendWorkerEventRequest =
      new SendWorkerEventRequest().workers(workerIds).eventType(eventType)
    logInfo(s"Sending workerEvent $eventType to workers: $workerIds")
    workerApi.sendWorkerEvent(sendWorkerEventRequest, commonOptions.getAuthHeader)
  }

  private[master] def runShowWorkerEventInfo: WorkerEventsResponse =
    workerApi.getWorkerEvents(commonOptions.getAuthHeader)

  private[master] def runShowLostWorkers: Seq[WorkerTimestampData] = {
    val lostWorkers = runShowWorkers.getLostWorkers.asScala.toSeq
    if (lostWorkers.isEmpty) {
      log("No lost workers found.")
      Seq.empty[WorkerTimestampData]
    } else {
      lostWorkers.sortBy(_.getWorker.getHost)
    }
  }

  private[master] def runShowExcludedWorkers: Seq[WorkerData] = {
    val excludedWorkers = runShowWorkers.getExcludedWorkers.asScala.toSeq
    if (excludedWorkers.isEmpty) {
      log("No excluded workers found.")
      Seq.empty[WorkerData]
    } else {
      excludedWorkers.sortBy(_.getHost)
    }
  }

  private[master] def runShowManualExcludedWorkers: Seq[WorkerData] = {
    val manualExcludedWorkers = runShowWorkers.getManualExcludedWorkers.asScala.toSeq
    if (manualExcludedWorkers.isEmpty) {
      log("No manual excluded workers found.")
      Seq.empty[WorkerData]
    } else {
      manualExcludedWorkers.sortBy(_.getHost)
    }
  }

  private[master] def runShowShutdownWorkers: Seq[WorkerData] = {
    val shutdownWorkers = runShowWorkers.getShutdownWorkers.asScala.toSeq
    if (shutdownWorkers.isEmpty) {
      log("No shutdown workers found.")
      Seq.empty[WorkerData]
    } else {
      shutdownWorkers.sortBy(_.getHost)
    }
  }

  private[master] def runShowDecommissioningWorkers: Seq[WorkerData] = {
    val decommissioningWorkers = runShowWorkers.getDecommissioningWorkers.asScala.toSeq
    if (decommissioningWorkers.isEmpty) {
      log("No decommissioning workers found.")
      Seq.empty[WorkerData]
    } else {
      decommissioningWorkers.sortBy(_.getHost)
    }
  }

  private[master] def runShowLifecycleManagers: HostnamesResponse =
    applicationApi.getApplicationHostNames(commonOptions.getAuthHeader)

  private[master] def runShowWorkers: WorkersResponse =
    workerApi.getWorkers(commonOptions.getAuthHeader)

  private[master] def runShowWorkersTopology: TopologyResponse =
    workerApi.getWorkersTopology(commonOptions.getAuthHeader)

  private[master] def getWorkerIds: util.List[WorkerId] = {
    val workerIds = commonOptions.workerIds
    if (workerIds == null || workerIds.isEmpty) {
      throw new ParameterException(
        spec.commandLine(),
        "Host list must be provided for this command.")
    }
    workerIds
      .trim
      .split(",")
      .map(toWorkerId)
      .toList
      .asJava
  }

  private[master] def toWorkerId(workerIdString: String): WorkerId = {
    val splitWorkerId = workerIdString.split(":")
    val host = splitWorkerId(0)
    val rpcPort = splitWorkerId(1).toInt
    val pushPort = splitWorkerId(2).toInt
    val fetchPort = splitWorkerId(3).toInt
    val replicatePort = splitWorkerId(4).toInt
    new WorkerId().host(host).rpcPort(rpcPort).pushPort(pushPort).fetchPort(
      fetchPort).replicatePort(replicatePort)
  }

  private[master] def runShowConf: ConfResponse = confApi.getConf(commonOptions.getAuthHeader)

  private[master] def runShowDynamicConf: DynamicConfigResponse =
    confApi.getDynamicConf(
      commonOptions.configLevel,
      commonOptions.configTenant,
      commonOptions.configName,
      commonOptions.getAuthHeader)

  private[master] def runUpsertDynamicConf: HandleResponse = {
    upsertDynamicConf(commonOptions, spec, confApi.upsertDynamicConf)
  }

  private[master] def runDeleteDynamicConf: HandleResponse = {
    deleteDynamicConf(commonOptions, spec, confApi.deleteDynamicConf)
  }

  private[master] def runShowThreadDump: ThreadStackResponse =
    defaultApi.getThreadDump(commonOptions.getAuthHeader)

  private[master] def runShowLoggers: LoggerInfos =
    loggerApi.getLogger(commonOptions.loggerName, null, commonOptions.getAuthHeader)

  private[master] def runSetLogLevel: HandleResponse = {
    setLogLevel(commonOptions, spec, loggerApi.setLogger)
  }

  private[master] def runAddClusterAlias: Unit = {
    val aliasToAdd = masterOptions.addClusterAlias
    val hosts = commonOptions.hostList
    if (hosts == null || hosts.isEmpty) {
      throw new ParameterException(
        spec.commandLine(),
        "Host list must be supplied via --host-list to add to alias.")
    }
    cliConfigManager.add(aliasToAdd, hosts)
    logInfo(s"Cluster alias $aliasToAdd added to ${CliConfigManager.cliConfigFilePath}. You can now use the --cluster" +
      s" command with this alias.")
  }

  private[master] def runRemoveClusterAlias: Unit = {
    val aliasToRemove = masterOptions.removeClusterAlias
    cliConfigManager.remove(aliasToRemove)
    logInfo(s"Cluster alias $aliasToRemove removed.")
  }

  private[master] def runShowContainerInfo: ContainerInfo =
    defaultApi.getContainerInfo(commonOptions.getAuthHeader)

  private def getSingleAppShuffleIds: (String, util.List[Integer]) = {
    val appId = commonOptions.apps
    val shuffleIds = Option(shuffleOptions).map(_.shuffleIds).orNull
    if (StringUtils.isBlank(appId) || shuffleIds == null || shuffleIds.isEmpty) {
      throw new ParameterException(
        spec.commandLine(),
        "Application id and shuffle ids must be provided for this command.")
    }
    if (appId.contains(",")) {
      throw new ParameterException(
        spec.commandLine(),
        "Only one application id can be provided for this command.")
    }
    (appId, shuffleIds)
  }

  override private[master] def reviseLostShuffles: HandleResponse = {
    val (appId, shuffleIds) = getSingleAppShuffleIds
    val request =
      new ReviseLostShufflesRequest().appId(appId).shuffleIds(shuffleIds)
    applicationApi.reviseLostShuffles(request, commonOptions.getAuthHeader)
  }

  override private[master] def deleteApps: HandleResponse = {
    if (StringUtils.isBlank(commonOptions.apps)) {
      throw new ParameterException(
        spec.commandLine(),
        "Applications must be provided for this command.")
    }
    val appIds = util.Arrays.asList[String](commonOptions.apps.split(","): _*)
    val request = new DeleteAppsRequest().apps(appIds)
    applicationApi.deleteApps(request, commonOptions.getAuthHeader)
  }

  override private[master] def updateInterruptionNotices: HandleResponse = {
    val workerInterruptionNotices = masterOptions.updateInterruptionNotices
      .split(",")
      .toList
      .map { pair =>
        val parts = pair.split("=", 2)
        if (parts.length != 2) {
          throw new ParameterException(
            spec.commandLine(),
            s"Invalid format for interruption notice: '$pair'. Expected format: workerId=timestamp")
        }
        val workerIdStr = parts(0)
        val timestampStr = parts(1)
        val timestamp =
          try {
            timestampStr.toLong
          } catch {
            case _: NumberFormatException =>
              throw new ParameterException(
                spec.commandLine(),
                s"Invalid timestamp for worker '$workerIdStr': '$timestampStr' is not a valid long")
          }
        new WorkerInterruptionNotice()
          .workerId(toWorkerId(workerIdStr))
          .interruptionTimestamp(timestamp)
      }

    val request = new UpdateInterruptionNoticeRequest().workers(workerInterruptionNotices.asJava)
    workerApi.updateInterruptionNotice(request, commonOptions.getAuthHeader)
  }

  override private[master] def runRatisElectionTransfer: HandleResponse = {
    val peerAddress = commonOptions.peerAddress
    if (StringUtils.isBlank(peerAddress)) {
      throw new ParameterException(
        spec.commandLine(),
        "Peer address must be provided via --peer-address for this command.")
    }
    val request = new RatisElectionTransferRequest().peerAddress(peerAddress)
    logInfo(s"Transferring ratis group leader to peer $peerAddress.")
    ratisApi.transferRatisLeader(request, commonOptions.getAuthHeader)
  }

  override private[master] def runRatisElectionStepDown: HandleResponse = {
    logInfo("Making the ratis group leader step down its leadership.")
    ratisApi.stepDownRatisLeader(commonOptions.getAuthHeader)
  }

  override private[master] def runRatisElectionPause: HandleResponse = {
    logInfo("Pausing leader election at the current master.")
    ratisApi.pauseRatisElection(commonOptions.getAuthHeader)
  }

  override private[master] def runRatisElectionResume: HandleResponse = {
    logInfo("Resuming leader election at the current master.")
    ratisApi.resumeRatisElection(commonOptions.getAuthHeader)
  }

  override private[master] def runRatisPeerAdd: HandleResponse = {
    val peers = getRatisPeers
    logInfo(s"Adding ratis peers: $peers")
    val request = new RatisPeerAddRequest().peers(peers)
    ratisApi.addRatisPeer(request, commonOptions.getAuthHeader)
  }

  override private[master] def runRatisPeerRemove: HandleResponse = {
    val peers = getRatisPeers
    logInfo(s"Removing ratis peers: $peers")
    val request = new RatisPeerRemoveRequest().peers(peers)
    ratisApi.removeRatisPeer(request, commonOptions.getAuthHeader)
  }

  override private[master] def runRatisPeerSetPriority: HandleResponse = {
    val addressPriorities = getRatisPeerPriorities
    logInfo(s"Setting ratis peer priorities: $addressPriorities")
    val request = new RatisPeerSetPriorityRequest().addressPriorities(addressPriorities)
    ratisApi.setRatisPeerPriority(request, commonOptions.getAuthHeader)
  }

  override private[master] def runRatisSnapshotCreate: HandleResponse = {
    logInfo("Triggering the current master to take a ratis snapshot.")
    ratisApi.createRatisSnapshot(commonOptions.getAuthHeader)
  }

  override private[master] def runRatisDownloadRaftMetaConf: Unit = {
    val targetPath = validatedTargetPath(masterOptions.ratisDownloadRaftMetaConf)
    val file = ratisApi.getLocalRaftMetaConf(commonOptions.getAuthHeader)
    Files.copy(file.toPath, targetPath, StandardCopyOption.REPLACE_EXISTING)
    logInfo(s"raft-meta.conf downloaded to $targetPath.")
  }

  override private[master] def runRatisGenerateNewRaftMetaConf: Unit = {
    val peers = getRatisPeers
    val targetPath = validatedTargetPath(masterOptions.ratisGenerateNewRaftMetaConf)
    val request = new RatisLocalRaftMetaConfRequest().peers(peers)
    val file = ratisApi.generateNewRaftMetaConf(request, commonOptions.getAuthHeader)
    Files.copy(file.toPath, targetPath, StandardCopyOption.REPLACE_EXISTING)
    logInfo(s"new-raft-meta.conf generated at $targetPath.")
  }

  private def getRatisPeers: util.List[RatisPeer] = {
    val peersStr = commonOptions.ratisPeers
    if (StringUtils.isBlank(peersStr)) {
      throw new ParameterException(
        spec.commandLine(),
        "Ratis peers must be provided via --ratis-peers in the format of `id|host:port`.")
    }
    peersStr.trim.split(",").map(toRatisPeer).toList.asJava
  }

  private def toRatisPeer(peerString: String): RatisPeer = {
    val idAndAddress = peerString.split("\\|", -1)
    if (idAndAddress.length != 2 || idAndAddress(0).isEmpty) {
      throw new ParameterException(
        spec.commandLine(),
        s"Invalid ratis peer: '$peerString'. Expected format: id|host:port")
    }
    val hostAndPort = idAndAddress(1).split(":", -1)
    if (hostAndPort.length != 2 || hostAndPort(0).isEmpty || hostAndPort(1).isEmpty) {
      throw new ParameterException(
        spec.commandLine(),
        s"Invalid ratis peer address: '${idAndAddress(1)}'. Expected format: host:port")
    }
    try {
      hostAndPort(1).toInt
    } catch {
      case _: NumberFormatException =>
        throw new ParameterException(
          spec.commandLine(),
          s"Invalid ratis peer address: '${idAndAddress(1)}'. Port is not a valid integer")
    }
    new RatisPeer().id(idAndAddress(0)).address(idAndAddress(1))
  }

  private def getRatisPeerPriorities: util.Map[String, Integer] = {
    val prioritiesStr = commonOptions.peerPriorities
    if (StringUtils.isBlank(prioritiesStr)) {
      throw new ParameterException(
        spec.commandLine(),
        "Peer priorities must be provided via --peer-priorities" +
          " in the format of `host:port=priority`.")
    }
    prioritiesStr.trim.split(",").map { pair =>
      val parts = pair.split("=", 2)
      if (parts.length != 2 || parts(0).isEmpty) {
        throw new ParameterException(
          spec.commandLine(),
          s"Invalid peer priority: '$pair'. Expected format: host:port=priority")
      }
      val priority =
        try {
          parts(1).toInt
        } catch {
          case _: NumberFormatException =>
            throw new ParameterException(
              spec.commandLine(),
              s"Invalid priority for peer '${parts(0)}': '${parts(1)}' is not a valid integer")
        }
      (parts(0), Integer.valueOf(priority))
    }.toMap.asJava
  }

  private def validatedTargetPath(pathStr: String): Path = {
    val targetPath = Paths.get(pathStr)
    val parent = targetPath.toAbsolutePath.getParent
    if (parent == null || !Files.isDirectory(parent)) {
      throw new ParameterException(
        spec.commandLine(),
        s"Invalid file path: '$pathStr'. The parent directory does not exist.")
    }
    targetPath
  }

}
