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

import java.util

import scala.collection.mutable.ArrayBuffer

import io.netty.channel.embedded.EmbeddedChannel
import org.mockito.Mockito.{mock, when}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.WorkerStatus
import org.apache.celeborn.common.network.client.{TransportClient, TransportResponseHandler}
import org.apache.celeborn.common.network.security.{AuthorizationRequest, ConnectionSecurityContext, SecurityOperation}
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.rpc.{RpcEnv, RpcMessageOrigin}
import org.apache.celeborn.common.rpc.netty.{NettyRpcEnv, RemoteNettyRpcCallContext}

class MasterRpcAuthorizationSuite extends AnyFunSuite with MasterClusterFeature {
  private val protectedMessages = Seq(
    PbRegisterWorker.getDefaultInstance,
    HeartbeatFromWorker(
      "localhost",
      1,
      2,
      3,
      4,
      Seq.empty,
      util.Collections.emptyMap(),
      util.Collections.emptySet(),
      false,
      WorkerStatus.normalWorkerStatus()),
    ReportWorkerUnavailable(util.Collections.emptyList()),
    ReportWorkerDecommission(util.Collections.emptyList()),
    PbWorkerExclude.getDefaultInstance,
    PbWorkerLost.getDefaultInstance,
    PbWorkerEventRequest.getDefaultInstance,
    PbRemoveWorkersUnavailableInfo.getDefaultInstance,
    PbCheckForWorkerTimeout.getDefaultInstance,
    CheckForWorkerUnavailableInfoTimeout,
    CheckForApplicationTimeOut,
    CheckForDFSExpiredDirsTimeout)

  Seq((true, true), (false, true), (false, false)).foreach { case (auth, internal) =>
    test(s"Master authorization uses receiver origin: auth=$auth internal=$internal") {
      val master = setupMasterWithRandomPort(Map(
        CelebornConf.AUTH_ENABLED.key -> auth.toString,
        CelebornConf.INTERNAL_PORT_ENABLED.key -> internal.toString))
      def remote(env: RpcEnv, clientId: String): RpcMessageOrigin = {
        val client = mock(classOf[TransportClient])
        when(client.getClientId).thenReturn(clientId)
        new RemoteNettyRpcCallContext(
          env.asInstanceOf[NettyRpcEnv],
          null,
          master.rpcEnv.address,
          client)
      }
      try {
        protectedMessages.foreach { message =>
          withClue(message.getClass.getSimpleName) {
            Seq(null, "registered-app").foreach { clientId =>
              val external = remote(master.rpcEnv, clientId)
              if (auth) {
                intercept[SecurityException](master.authorize(external, message))
              } else {
                master.authorize(external, message)
              }
              master.authorize(remote(master.internalRpcEnvInUse, clientId), message)
            }
            master.authorize(RpcMessageOrigin.local(), message)
            if (auth) {
              intercept[SecurityException](master.authorize(new RpcMessageOrigin {}, message))
            }
          }
        }
        if (auth) {
          intercept[SecurityException] {
            master.authorize(remote(master.rpcEnv, null), "unknown request")
          }
        }
      } finally {
        try {
          shutdownMaster()
        } finally {
          if (internal) {
            master.internalRpcEnvInUse.shutdown()
            master.internalRpcEnvInUse.awaitTermination()
          }
        }
      }
    }
  }

  test("plugin policy cannot widen external Master worker-event access") {
    val master = setupMasterWithRandomPort(Map(
      CelebornConf.AUTH_ENABLED.key -> "true",
      CelebornConf.INTERNAL_PORT_ENABLED.key -> "true"))
    val message = PbWorkerEventRequest.getDefaultInstance
    try {
      Seq(null, "native-application").foreach { clientId =>
        withClue(s"native clientId=$clientId: ") {
          val channel = new EmbeddedChannel()
          try {
            val client = new TransportClient(channel, mock(classOf[TransportResponseHandler]))
            // Supply the native binding directly; this test does not perform a SASL handshake.
            if (clientId != null) {
              client.setClientId(clientId)
            }
            val requests = ArrayBuffer.empty[AuthorizationRequest]
            client.setSecurityContext(new ConnectionSecurityContext {
              override def authorize(request: AuthorizationRequest): Unit = {
                requests += request
              }
            })

            intercept[SecurityException] {
              master.authorize(RpcMessageOrigin.remote(master.rpcEnv, client), message)
            }
            assert(requests.isEmpty)

            master.authorize(RpcMessageOrigin.remote(master.internalRpcEnvInUse, client), message)
            assert(requests.map(_.getOperation) == Seq(SecurityOperation.WORKER_EVENT))
            assert(requests.head.getScope == AuthorizationRequest.Scope.SERVICE)

            master.authorize(RpcMessageOrigin.local(), message)
            assert(requests.size == 1)
          } finally {
            channel.finishAndReleaseAll()
          }
        }
      }
    } finally {
      try {
        shutdownMaster()
      } finally {
        master.rpcEnv.shutdown()
        master.internalRpcEnvInUse.shutdown()
        master.rpcEnv.awaitTermination()
        master.internalRpcEnvInUse.awaitTermination()
      }
    }
  }
}
