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

package org.apache.celeborn.cli.worker

import picocli.CommandLine.Command

import org.apache.celeborn.rest.v1.model._
import org.apache.celeborn.rest.v1.model.WorkerExitRequest.TypeEnum

@Command(name = "worker", mixinStandardHelpOptions = true)
class WorkerSubcommandImpl extends Runnable with WorkerSubcommand {

  override def run(): Unit = {
    if (workerOptions.showWorkerInfo) log(runShowWorkerInfo)
    if (workerOptions.showAppsOnWorker) log(runShowAppsOnWorker)
    if (workerOptions.showShufflesOnWorker) log(runShowShufflesOnWorker)
    if (workerOptions.showPartitionLocationInfo) log(runShowPartitionLocationInfo)
    if (workerOptions.showUnavailablePeers) log(runShowUnavailablePeers)
    if (workerOptions.isShutdown) log(runIsShutdown)
    if (workerOptions.isDecommissioning) log(runIsDecommissioning)
    if (workerOptions.isRegistered) log(runIsRegistered)
    if (workerOptions.exitType != null && workerOptions.exitType.nonEmpty) log(runExit)
    if (workerOptions.showConf) log(runShowConf)
    if (workerOptions.showContainerInfo) log(runShowContainerInfo)
    if (workerOptions.showDynamicConf) log(runShowDynamicConf)
    if (workerOptions.showThreadDump) log(runShowThreadDump)
  }

  private[worker] def runShowWorkerInfo: WorkerInfoResponse =
    workerApi.getWorkerInfo(commonOptions.getAuthHeader)

  private[worker] def runShowAppsOnWorker: ApplicationsResponse =
    applicationApi.getApplicationList(commonOptions.getAuthHeader)

  private[worker] def runShowShufflesOnWorker: ShufflesResponse =
    shuffleApi.getShuffles(commonOptions.getAuthHeader)

  private[worker] def runShowPartitionLocationInfo: ShufflePartitionsResponse =
    shuffleApi.getShufflePartitions(commonOptions.getAuthHeader)

  private[worker] def runShowUnavailablePeers: UnAvailablePeersResponse =
    workerApi.unavailablePeers(commonOptions.getAuthHeader)

  private[worker] def runIsShutdown: Boolean = runShowWorkerInfo.getIsShutdown

  private[worker] def runIsDecommissioning: Boolean = runShowWorkerInfo.getIsDecommissioning

  private[worker] def runIsRegistered: Boolean = runShowWorkerInfo.getIsRegistered

  private[worker] def runExit: HandleResponse = {
    val workerExitType: TypeEnum = TypeEnum.valueOf(workerOptions.exitType)
    val workerExitRequest: WorkerExitRequest = new WorkerExitRequest().`type`(workerExitType)
    logInfo(s"Sending worker exit type: ${workerExitType.getValue}")
    workerApi.workerExit(workerExitRequest, commonOptions.getAuthHeader)
  }

  private[worker] def runShowConf: ConfResponse = confApi.getConf(commonOptions.getAuthHeader)

  private[worker] def runShowDynamicConf: DynamicConfigResponse =
    confApi.getDynamicConf(
      commonOptions.configLevel,
      commonOptions.configTenant,
      commonOptions.configName,
      commonOptions.getAuthHeader)

  private[worker] def runShowThreadDump: ThreadStackResponse =
    defaultApi.getThreadDump(commonOptions.getAuthHeader)

  private[worker] def runShowContainerInfo: ContainerInfo =
    defaultApi.getContainerInfo(commonOptions.getAuthHeader)
}
