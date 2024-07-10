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

package org.apache.celeborn.service.deploy.web

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.MASTER_ENDPOINTS
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.protocol.{PbMasterGroupResponse, RpcNameConstants, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.ControlMessages.MasterGroupRequest
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.SignalUtils
import org.apache.celeborn.server.common.{HttpService, Service}
import org.apache.celeborn.service.deploy.web.http.api.dto.ClusterSummary

private[celeborn] class Web(
    override val conf: CelebornConf,
    val webArgs: WebArguments)
  extends HttpService with RpcEndpoint with Logging {

  @volatile private var stopped = false

  override def serviceName: String = Service.WEB

  override val metricsSystem: MetricsSystem =
    MetricsSystem.createMetricsSystem(serviceName, conf)

  if (conf.logCelebornConfEnabled) {
    logInfo(getConf)
  }

  override val rpcEnv: RpcEnv = {
    RpcEnv.create(
      RpcNameConstants.WEB_SYS,
      TransportModuleConstants.RPC_SERVICE_MODULE,
      webArgs.host,
      webArgs.host,
      webArgs.port,
      conf,
      Math.max(64, Runtime.getRuntime.availableProcessors()))
  }

  rpcEnv.setupEndpoint(RpcNameConstants.WEB_EP, this)

  override def initialize(): Unit = {
    super.initialize()
    logInfo("Web started.")
    rpcEnv.awaitTermination()
  }

  override def stop(exitKind: Int): Unit = synchronized {
    if (!stopped) {
      logInfo("Stopping Web.")
      rpcEnv.stop(self)
      super.stop(exitKind)
      logInfo("Web is stopped.")
      stopped = true
    }
  }

  private val masterClient = new MasterClient(rpcEnv, conf, false)

  def clusterOverview: ClusterSummary = {
    val response =
      masterClient.askSync[PbMasterGroupResponse](
        MasterGroupRequest(),
        classOf[PbMasterGroupResponse])
    new ClusterSummary(conf.masterEndpoints.mkString(","), response.getMasterLeader)
  }
}

private[deploy] object Web extends Logging {

  def main(args: Array[String]): Unit = {
    SignalUtils.registerLogger(log)
    val conf = new CelebornConf()
    val webArgs = new WebArguments(args, conf)
    // There are many entries for setting the master address, and we should unify the entries as
    // much as possible. Therefore, if the user manually specifies the address of the Master when
    // starting the Web, we should set it in the parameters and automatically calculate what the
    // address of the Master should be used in the end.
    webArgs.master.foreach { master =>
      conf.set(MASTER_ENDPOINTS.key, RpcAddress.fromCelebornURL(master).hostPort)
    }
    try {
      new Web(conf, webArgs).initialize()
    } catch {
      case e: Throwable =>
        logError("Initialize web failed.", e)
        System.exit(-1)
    }
  }
}
