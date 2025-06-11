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

import picocli.CommandLine.{ArgGroup, Mixin, ParameterException, ParentCommand, Spec}
import picocli.CommandLine.Model.CommandSpec

import org.apache.celeborn.cli.CelebornCli
import org.apache.celeborn.cli.common.{BaseCommand, CliLogging, CommonOptions}
import org.apache.celeborn.rest.v1.model._
import org.apache.celeborn.rest.v1.worker.{ApplicationApi, ConfApi, DefaultApi, ShuffleApi, WorkerApi}
import org.apache.celeborn.rest.v1.worker.invoker.ApiClient

trait WorkerSubcommand extends BaseCommand {

  @ParentCommand
  private var celebornCli: CelebornCli = _

  @ArgGroup(exclusive = true, multiplicity = "1")
  private[worker] var workerOptions: WorkerOptions = _

  @Mixin
  private[worker] var commonOptions: CommonOptions = _

  @Spec
  private[worker] var spec: CommandSpec = _

  private[worker] def apiClient = {
    if (commonOptions.hostPort != null && commonOptions.hostPort.nonEmpty) {
      log(s"Using connectionUrl: ${commonOptions.hostPort}")
      new ApiClient().setBasePath(s"http://${commonOptions.hostPort}")
    } else {
      throw new ParameterException(
        spec.commandLine(),
        "No valid connection url found, please provide --hostport.")
    }
  }
  private[worker] def applicationApi = new ApplicationApi(apiClient)
  private[worker] def confApi = new ConfApi(apiClient)
  private[worker] def defaultApi = new DefaultApi(apiClient)
  private[worker] def shuffleApi = new ShuffleApi(apiClient)
  private[worker] def workerApi = new WorkerApi(apiClient)

  private[worker] def runShowWorkerInfo: WorkerInfoResponse

  private[worker] def runShowAppsOnWorker: ApplicationsResponse

  private[worker] def runShowShufflesOnWorker: ShufflesResponse

  private[worker] def runShowPartitionLocationInfo: ShufflePartitionsResponse

  private[worker] def runShowUnavailablePeers: UnAvailablePeersResponse

  private[worker] def runIsShutdown: Boolean

  private[worker] def runIsDecommissioning: Boolean

  private[worker] def runIsRegistered: Boolean

  private[worker] def runExit: HandleResponse

  private[worker] def runShowConf: ConfResponse

  private[worker] def runShowContainerInfo: ContainerInfo

  private[worker] def runShowDynamicConf: DynamicConfigResponse

  private[worker] def runShowThreadDump: ThreadStackResponse

}
