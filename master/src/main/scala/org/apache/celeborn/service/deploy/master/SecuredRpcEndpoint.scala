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

package org.apache.celeborn.service.deploy.master

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PbUnregisterShuffle
import org.apache.celeborn.common.protocol.message.ControlMessages.{ApplicationLost, CheckQuota, HeartbeatFromApplication, RequestSlots}
import org.apache.celeborn.common.rpc._

/**
 * Secured RPC endpoint used by the Master to communicate with the Clients.
 */
private[celeborn] class SecuredRpcEndpoint(
    val master: Master,
    override val rpcEnv: RpcEnv,
    val conf: CelebornConf)
  extends RpcEndpoint with Logging {

  // start threads to check timeout for workers and applications
  override def onStart(): Unit = {
    master.onStart()
  }

  override def onStop(): Unit = {
    master.onStop()
  }

  override def onDisconnected(address: RpcAddress): Unit = {
    logDebug(s"Client $address got disconnected.")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case HeartbeatFromApplication(
          appId,
          totalWritten,
          fileCount,
          needCheckedWorkerList,
          requestId,
          shouldResponse) =>
      logDebug(s"Received heartbeat from app $appId")
      master.executeWithLeaderChecker(
        context,
        master.handleHeartbeatFromApplication(
          context,
          appId,
          totalWritten,
          fileCount,
          needCheckedWorkerList,
          requestId,
          shouldResponse))

    case requestSlots @ RequestSlots(_, _, _, _, _, _, _, _, _, _, _) =>
      logTrace(s"Received RequestSlots request $requestSlots.")
      master.executeWithLeaderChecker(context, master.handleRequestSlots(context, requestSlots))

    case pb: PbUnregisterShuffle =>
      val applicationId = pb.getAppId
      val shuffleId = pb.getShuffleId
      val requestId = pb.getRequestId
      logDebug(s"Received UnregisterShuffle request $requestId, $applicationId, $shuffleId")
      master.executeWithLeaderChecker(
        context,
        master.handleUnregisterShuffle(context, applicationId, shuffleId, requestId))

    case ApplicationLost(appId, requestId) =>
      logDebug(
        s"Received ApplicationLost request $requestId, $appId from ${context.senderAddress}.")
      master.executeWithLeaderChecker(
        context,
        master.handleApplicationLost(context, appId, requestId))

    case CheckQuota(userIdentifier) =>
      master.executeWithLeaderChecker(context, master.handleCheckQuota(userIdentifier, context))
  }
}
