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

import io.netty.channel.embedded.EmbeddedChannel
import org.mockito.Mockito.mock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.{TransportClient, TransportResponseHandler}
import org.apache.celeborn.common.network.security.{AuthorizationRequest, SecurityOperation}

class RpcRequestContextSuite extends CelebornFunSuite {
  test("an unknown context cannot acquire local authority by declaring a sender address") {
    val address = RpcAddress("localhost", 12345)
    val context = new RpcRequestContext {
      override val senderAddress: RpcAddress = address
    }
    intercept[SecurityException](context.requireLocal())
    intercept[SecurityException](context.authorize(
      AuthorizationRequest.forService(SecurityOperation.WORKER_LOST)))
  }

  test("local authority is assigned by the dispatcher and preserves the supplied user") {
    val user = UserIdentifier("tenant", "user")
    val context = RpcRequestContext.local(null)
    context.requireLocal()
    assert(context.authorize(AuthorizationRequest.forApplication(
      SecurityOperation.CHECK_QUOTA,
      null,
      user)) eq user)
  }

  test("a remote request retains its connection even when its sender address looks local") {
    val channel = new EmbeddedChannel()
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    client.setClientId("app")
    try {
      val context = RpcRequestContext.remote(RpcAddress("localhost", 12345), client)
      assert(context.client.contains(client))
      assert(!context.isLocal)
      intercept[SecurityException](context.requireLocal())
      context.authorize(AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "app"))
      intercept[SecurityException](context.authorize(
        AuthorizationRequest.forService(SecurityOperation.WORKER_LOST)))
    } finally {
      channel.finishAndReleaseAll()
    }
  }
}
