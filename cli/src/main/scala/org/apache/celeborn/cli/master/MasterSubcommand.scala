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

import picocli.CommandLine.{ArgGroup, Mixin, ParameterException, ParentCommand, Spec}
import picocli.CommandLine.Model.CommandSpec

import org.apache.celeborn.cli.CelebornCli
import org.apache.celeborn.cli.common.{CliLogging, CommonOptions}
import org.apache.celeborn.cli.config.CliConfigManager
import org.apache.celeborn.rest.v1.master.{ApplicationApi, ConfApi, DefaultApi, MasterApi, ShuffleApi, WorkerApi}
import org.apache.celeborn.rest.v1.master.invoker.ApiClient
import org.apache.celeborn.rest.v1.model._

trait MasterSubcommand extends CliLogging {

  @ParentCommand
  private var celebornCli: CelebornCli = _

  @ArgGroup(exclusive = true, multiplicity = "1")
  private[master] var masterOptions: MasterOptions = _

  @Mixin
  private[master] var commonOptions: CommonOptions = _

  @Spec
  private[master] var spec: CommandSpec = _

  private[master] val cliConfigManager = new CliConfigManager
  private def apiClient: ApiClient = {
    val connectionUrl =
      if (commonOptions.hostPort != null && commonOptions.hostPort.nonEmpty) {
        commonOptions.hostPort
      } else {
        val endpoints = cliConfigManager.get(commonOptions.cluster)
        endpoints.getOrElse("").split(",")(0)
      }
    if (connectionUrl != null && connectionUrl.nonEmpty) {
      log(s"Using connectionUrl: $connectionUrl")
      new ApiClient().setBasePath(s"http://$connectionUrl")
    } else {
      throw new ParameterException(
        spec.commandLine(),
        "No valid connection url found, please provide either --hostport or " + "valid cluster alias.")
    }
  }
  private[master] def applicationApi: ApplicationApi = new ApplicationApi(apiClient)
  private[master] def confApi: ConfApi = new ConfApi(apiClient)
  private[master] def defaultApi: DefaultApi = new DefaultApi(apiClient)
  private[master] def masterApi: MasterApi = new MasterApi(apiClient)
  private[master] def shuffleApi: ShuffleApi = new ShuffleApi(apiClient)
  private[master] def workerApi: WorkerApi = new WorkerApi(apiClient)

  private[master] def runShowMastersInfo: MasterInfoResponse

  private[master] def runShowClusterApps: ApplicationsHeartbeatResponse

  private[master] def runShowClusterShuffles: ShufflesResponse

  private[master] def runShowTopDiskUsedApps: AppDiskUsageSnapshotsResponse

  private[master] def runExcludeWorkers: HandleResponse

  private[master] def runRemoveExcludedWorkers: HandleResponse

  private[master] def runRemoveWorkersUnavailableInfo: HandleResponse

  private[master] def runSendWorkerEvent: HandleResponse

  private[master] def runShowWorkerEventInfo: WorkerEventsResponse

  private[master] def runShowLostWorkers: Seq[WorkerTimestampData]

  private[master] def runShowExcludedWorkers: Seq[WorkerData]

  private[master] def runShowManualExcludedWorkers: Seq[WorkerData]

  private[master] def runShowShutdownWorkers: Seq[WorkerData]

  private[master] def runShowDecommissioningWorkers: Seq[WorkerData]

  private[master] def runShowLifecycleManagers: HostnamesResponse

  private[master] def runShowWorkers: WorkersResponse

  private[master] def getWorkerIds: util.List[WorkerId]

  private[master] def runShowConf: ConfResponse

  private[master] def runShowDynamicConf: DynamicConfigResponse

  private[master] def runShowThreadDump: ThreadStackResponse

}
