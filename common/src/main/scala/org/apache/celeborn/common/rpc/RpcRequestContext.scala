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

package org.apache.celeborn.common.rpc

import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.security.AuthorizationRequest

/** Receiver-created request origin. It is never inferred from a serialized sender address. */
trait RpcRequestContext {
  // Unknown custom call contexts are not implicitly trusted as local calls.
  def isLocal: Boolean = false
  def client: Option[TransportClient] = None

  final def requireLocal(): Unit = {
    if (!isLocal) {
      throw new SecurityException("This operation is only available to local RPC messages.")
    }
  }

  final def authorize(request: AuthorizationRequest): UserIdentifier = {
    if (isLocal) {
      request.getUserIdentifier
    } else {
      client match {
        case Some(connection) => connection.authorize(request)
        case _ => throw new SecurityException("RPC request has no authenticated connection origin.")
      }
    }
  }
}

private[celeborn] object RpcRequestContext {
  def local(): RpcRequestContext = new RpcRequestContext {
    override val isLocal: Boolean = true
  }

  def remote(connection: TransportClient): RpcRequestContext = {
    require(connection != null, "A remote request requires its transport connection")
    new RpcRequestContext {
      override val client: Option[TransportClient] = Some(connection)
    }
  }
}
