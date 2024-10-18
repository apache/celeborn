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

import java.util

import scala.collection.JavaConverters._

import picocli.CommandLine.{Command, ParameterException}

import org.apache.celeborn.cli.config.CliConfigManager
import org.apache.celeborn.rest.v1.model._
import org.apache.celeborn.rest.v1.model.SendWorkerEventRequest.EventTypeEnum

@Command(name = "master", mixinStandardHelpOptions = true)
class MasterSubcommandImpl extends Runnable with MasterSubcommand {
  override def run(): Unit = {
    if (masterOptions.showMastersInfo) log(runShowMastersInfo)
    if (masterOptions.showClusterApps) log(runShowClusterApps)
    if (masterOptions.showClusterShuffles) log(runShowClusterShuffles)
    if (masterOptions.showTopDiskUsedApps) log(runShowTopDiskUsedApps)
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
    if (masterOptions.showConf) log(runShowConf)
    if (masterOptions.showContainerInfo) log(runShowContainerInfo)
    if (masterOptions.showDynamicConf) log(runShowDynamicConf)
    if (masterOptions.showThreadDump) log(runShowThreadDump)
    if (masterOptions.reviseLostShuffles) log(reviseLostShuffles)
    if (masterOptions.deleteApp) log(deleteApp)
    if (masterOptions.addClusterAlias != null && masterOptions.addClusterAlias.nonEmpty)
      runAddClusterAlias
    if (masterOptions.removeClusterAlias != null && masterOptions.removeClusterAlias.nonEmpty)
      runRemoveClusterAlias
  }

  private[master] def runShowMastersInfo: MasterInfoResponse = masterApi.getMasterGroupInfo

  private[master] def runShowClusterApps: ApplicationsHeartbeatResponse =
    applicationApi.getApplications

  private[master] def runShowClusterShuffles: ShufflesResponse = shuffleApi.getShuffles

  private[master] def runShowTopDiskUsedApps: AppDiskUsageSnapshotsResponse =
    applicationApi.getApplicationsDiskUsageSnapshots

  private[master] def runExcludeWorkers: HandleResponse = {
    val workerIds = getWorkerIds
    val excludeWorkerRequest = new ExcludeWorkerRequest().add(workerIds)
    logInfo(s"Sending exclude worker requests to master for the following workers: $workerIds")
    workerApi.excludeWorker(excludeWorkerRequest)
  }

  private[master] def runRemoveExcludedWorkers: HandleResponse = {
    val workerIds = getWorkerIds
    val removeExcludeWorkerRequest = new ExcludeWorkerRequest().remove(workerIds)
    logInfo(
      s"Sending remove exclude worker requests to master for the following workers: $workerIds")
    workerApi.excludeWorker(removeExcludeWorkerRequest)
  }

  private[master] def runRemoveWorkersUnavailableInfo: HandleResponse = {
    val workerIds = getWorkerIds
    val removeWorkersUnavailableInfoRequest =
      new RemoveWorkersUnavailableInfoRequest().workers(workerIds)
    logInfo(
      s"Sending remove workers unavailable info requests to master for the following workers: $workerIds")
    workerApi.removeWorkersUnavailableInfo(removeWorkersUnavailableInfoRequest)
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
    workerApi.sendWorkerEvent(sendWorkerEventRequest)
  }

  private[master] def runShowWorkerEventInfo: WorkerEventsResponse = workerApi.getWorkerEvents

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
    applicationApi.getApplicationHostNames

  private[master] def runShowWorkers: WorkersResponse = workerApi.getWorkers

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
      .map(workerId => {
        val splitWorkerId = workerId.split(":")
        val host = splitWorkerId(0)
        val rpcPort = splitWorkerId(1).toInt
        val pushPort = splitWorkerId(2).toInt
        val fetchPort = splitWorkerId(3).toInt
        val replicatePort = splitWorkerId(4).toInt
        new WorkerId().host(host).rpcPort(rpcPort).pushPort(pushPort).fetchPort(
          fetchPort).replicatePort(replicatePort)
      })
      .toList
      .asJava
  }

  private[master] def runShowConf: ConfResponse = confApi.getConf

  private[master] def runShowDynamicConf: DynamicConfigResponse =
    confApi.getDynamicConf(
      commonOptions.configLevel,
      commonOptions.configTenant,
      commonOptions.configName)

  private[master] def runShowThreadDump: ThreadStackResponse = defaultApi.getThreadDump

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

  private[master] def runShowContainerInfo: ContainerInfo = defaultApi.getContainerInfo

  override private[master] def reviseLostShuffles: HandleResponse = {
    val app = commonOptions.app
    val shuffleIds = reviseLostShuffleOptions.shuffleIds
    applicationApi.reviseLostShuffles(app, shuffleIds)
  }

  override private[master] def deleteApp: HandleResponse = {
    val app = commonOptions.app
    applicationApi.deleteApp(app)
  }
}
