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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.sasl.SecretRegistry
import org.apache.celeborn.common.protocol.PbApplicationMeta
import org.apache.celeborn.common.rpc._

/**
 * Internal RPC endpoint used by the Workers to communicate with the Masters.
 * internal port.
 */
private[celeborn] class InternalRpcEndpoint(
    override val rpcEnv: RpcEnv,
    val conf: CelebornConf,
    val secretRegistry: SecretRegistry)
  extends RpcEndpoint with Logging {

  override def onDisconnected(address: RpcAddress): Unit = {
    logDebug(s"Client $address got disconnected.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case pb: PbApplicationMeta =>
      val appId = pb.getAppId
      val secret = pb.getSecret
      if (!secretRegistry.isRegistered(appId)) {
        logInfo(s"Received application meta for $appId from the Master.")
        secretRegistry.register(appId, secret)
      }
  }
}
