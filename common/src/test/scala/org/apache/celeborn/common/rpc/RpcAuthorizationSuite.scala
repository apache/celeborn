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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise

import io.netty.channel.embedded.EmbeddedChannel
import org.mockito.Mockito.mock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.{TransportClient, TransportResponseHandler}
import org.apache.celeborn.common.network.security.{AuthorizationRequest, ConnectionSecurityContext, SecurityOperation}
import org.apache.celeborn.common.rpc.netty.{LocalNettyRpcCallContext, NettyRpcEnv, RemoteNettyRpcCallContext}

class RpcAuthorizationSuite extends CelebornFunSuite {
  test("an unknown context has neither local authority nor a transport connection") {
    val context = new RpcMessageOrigin {}
    intercept[SecurityException](RpcAuthorization.requireLocal(context))
    intercept[SecurityException](RpcAuthorization.authorize(
      context,
      AuthorizationRequest.forService(SecurityOperation.WORKER_LOST)))
  }

  test("local authority is assigned explicitly and does not require a connection") {
    val context = RpcMessageOrigin.local()
    RpcAuthorization.requireLocal(context)
    RpcAuthorization.authorize(
      context,
      AuthorizationRequest.forApplication(
        SecurityOperation.CHECK_QUOTA,
        null,
        UserIdentifier("tenant", "user")))
  }

  test("a remote request retains its connection and cannot acquire local authority") {
    val channel = new EmbeddedChannel()
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    client.setClientId("app")
    try {
      val receiver = mock(classOf[RpcEnv])
      val context = RpcMessageOrigin.remote(receiver, client)
      assert(context.receivingRpcEnv.contains(receiver))
      assert(context.client.contains(client))
      assert(!context.isLocal)
      intercept[IllegalArgumentException](RpcMessageOrigin.remote(null, client))
      intercept[IllegalArgumentException](RpcMessageOrigin.remote(receiver, null))
      intercept[SecurityException](RpcAuthorization.requireLocal(context))
      RpcAuthorization.authorize(
        context,
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "app"))
      intercept[SecurityException](RpcAuthorization.authorize(
        context,
        AuthorizationRequest.forService(SecurityOperation.WORKER_LOST)))
    } finally {
      channel.finishAndReleaseAll()
    }
  }

  test("legacy checkAuth uses the plugin policy independently of native application identity") {
    val channel = new EmbeddedChannel()
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    // Supply a native binding to test policy selection; this does not perform a SASL handshake.
    client.setClientId("native-application")
    val requests = ArrayBuffer.empty[AuthorizationRequest]
    client.setSecurityContext(new ConnectionSecurityContext {
      override def authorize(request: AuthorizationRequest): Unit = {
        requests += request
        if (request.getApplicationId != "tenant/application") {
          throw new SecurityException("Plugin rejected application")
        }
      }
    })
    try {
      val context = new RemoteNettyRpcCallContext(mock(classOf[NettyRpcEnv]), null, null, client)
      endpoint.checkAuth(context, "tenant/application")
      val rejection = intercept[SecurityException] {
        endpoint.checkAuth(context, "foreign/application")
      }
      assert(rejection.getMessage == "Plugin rejected application")
      assert(requests.map(_.getApplicationId) == Seq("tenant/application", "foreign/application"))
      assert(requests.forall(_.getScope == AuthorizationRequest.Scope.APPLICATION))
      assert(requests.forall(_.getOperation == SecurityOperation.APPLICATION_ACCESS))
    } finally {
      channel.finishAndReleaseAll()
    }
  }

  test("legacy checkAuth rejects an unknown call context and accepts an explicit local context") {
    val address = RpcAddress("localhost", 12345)
    val unknown = new RpcCallContext {
      override val senderAddress: RpcAddress = address
      override def reply(response: Any): Unit = ()
      override def sendFailure(e: Throwable): Unit = ()
    }
    intercept[SecurityException](endpoint.checkAuth(unknown, "app"))
    endpoint.checkAuth(new LocalNettyRpcCallContext(address, Promise[Any]()), "app")
  }

  test("legacy checkAuth preserves native same-application authorization without a plugin") {
    val channel = new EmbeddedChannel()
    val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
    client.setClientId("app")
    try {
      val context = new RemoteNettyRpcCallContext(mock(classOf[NettyRpcEnv]), null, null, client)
      endpoint.checkAuth(context, "app")
      intercept[SecurityException](endpoint.checkAuth(context, "other-app"))
    } finally {
      channel.finishAndReleaseAll()
    }
  }

  private val endpoint = new RpcEndpoint {
    override val rpcEnv: RpcEnv = null
  }
}
