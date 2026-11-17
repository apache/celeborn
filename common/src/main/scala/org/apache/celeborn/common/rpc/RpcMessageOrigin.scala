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

/**
 * The origin of an RPC message as determined by the receiver: local dispatch or the receiving RpcEnv.
 * This is independent of the sender address in the message and does not authenticate the caller.
 */
trait RpcMessageOrigin {
  // An unknown origin does not acquire local or internal authority.
  def isLocal: Boolean = false

  /** The receiving environment, assigned by the network handler rather than the wire message. */
  def receivingRpcEnv: Option[RpcEnv] = None
}

private[celeborn] object RpcMessageOrigin {
  private val localOrigin: RpcMessageOrigin = new RpcMessageOrigin {
    override val isLocal: Boolean = true
  }

  def local(): RpcMessageOrigin = localOrigin

  def remote(receiver: RpcEnv): RpcMessageOrigin = {
    require(receiver != null, "A remote message requires its receiving RPC environment")
    new RpcMessageOrigin {
      override val receivingRpcEnv: Option[RpcEnv] = Some(receiver)
    }
  }
}
