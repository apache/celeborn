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

import java.nio.file.Files
import java.util
import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import io.netty.channel.Channel
import org.mockito.Mockito.mock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{ApplicationMeta, WorkerInfo}
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.client.{TransportClient, TransportResponseHandler}
import org.apache.celeborn.common.network.security.{AuthorizationRequest, AuthorizationTestBootstrap, ConnectionSecurityContext}
import org.apache.celeborn.common.network.security.SecurityOperation._
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, PbSerDeUtils}

class MasterSecuritySuite extends CelebornFunSuite {
  private val appId = "tenant/application"
  private val claimedUser = UserIdentifier("claimed", "other")
  private val plugin = classOf[AuthorizationTestBootstrap].getName

  test("application policy checks request claims before changing Master state") {
    withMaster(customConf()) { (master, conf, clients, _) =>
      def registration(applicationId: String): RegisterApplicationInfo =
        RegisterApplicationInfo(
          applicationId,
          claimedUser,
          util.Collections.emptyMap[String, String]())

      val observed =
        AuthorizationTestBootstrap.requests(conf.get("celeborn.test.security.serverId"))
      val deniedPeer = connect(master, conf, clients, "denied")
      val requests = Seq[(String, Any, String)](
        (REGISTER_APPLICATION_INFO, registration(appId), appId),
        (REQUEST_SLOTS, requestSlots(), appId),
        (REQUEST_WORKERS, requestWorkers(), appId),
        (CHECK_QUOTA, CheckQuota(claimedUser), null))
      requests.foreach { case (operation, message, targetApp) =>
        denied(deniedPeer, message, operation)
        val decision = observed.poll()
        assert(decision != null && decision.getOperation == operation)
        assert(decision.getScope == AuthorizationRequest.Scope.APPLICATION)
        assert(decision.getApplicationId == targetApp)
        assert(decision.getUserIdentifier == claimedUser)
        assert(observed.isEmpty)
      }
      assert(master.statusSystem.applicationInfos.isEmpty)
      assert(master.statusSystem.registeredAppAndShuffles.isEmpty)

      val application = connect(master, conf, clients, "application")
      application.askSync[Any](registration(appId))
      assert(master.statusSystem.applicationInfos.get(appId).userIdentifier == claimedUser)
      val accepted = observed.poll()
      assert(accepted != null && accepted.getOperation == REGISTER_APPLICATION_INFO)
      assert(accepted.getUserIdentifier == claimedUser)
      assert(observed.isEmpty)

      denied(application, registration("foreign/application"), REGISTER_APPLICATION_INFO)
      assert(!master.statusSystem.applicationInfos.containsKey("foreign/application"))
    }
  }

  test("internal forwarding retains service policy for ask and send") {
    withMaster(customConf()) { (master, conf, clients, _) =>
      val worker = new WorkerInfo("localhost", 19001, 19002, 19003, 19004, 19005)
      val service = connect(master, conf, clients, "service", internal = true)
      val application = connect(master, conf, clients, "application", internal = true)
      assert(service.askSync[PbRegisterWorkerResponse](registerWorker()).getSuccess)
      val foreignApp = "foreign/application"
      master.statusSystem.applicationMetas.put(foreignApp, ApplicationMeta(foreignApp, "secret"))
      assert(service.askSync[PbApplicationMeta](metadata(foreignApp)).getSecret == "secret")
      denied(application, metadata(foreignApp), GET_APPLICATION_META)
      denied(application, workerLost(), WORKER_LOST)

      application.send(workerLost())
      // This channel uses one business dispatcher, so the following reply fences the send.
      application.askSync[PbCheckWorkersAvailableResponse](
        PbCheckWorkersAvailable.getDefaultInstance)
      assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))

      service.send(workerLost())
      JavaUtils.timeOutOrMeetCondition(() =>
        !master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))

      assert(master.self.askSync[PbRegisterWorkerResponse](registerWorker()).getSuccess)
      master.self.send(workerLost())
      JavaUtils.timeOutOrMeetCondition(() =>
        !master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))
    }
  }

  test("plugin maintenance requires local dispatch or a dedicated internal listener") {
    Seq(false, true).foreach { internal =>
      withMaster(customConf().set(CelebornConf.INTERNAL_PORT_ENABLED.key, internal.toString)) {
        (master, conf, clients, errors) =>
          val service = connect(master, conf, clients, "service")
          service.send(CheckForApplicationTimeOut)
          val error = errors.poll(5, TimeUnit.SECONDS)
          assert(error != null && error.getMessage.contains("local RPC messages"))

          // This maintenance object has no wire encoding; check its receiver boundary directly.
          val connection =
            new TransportClient(mock(classOf[Channel]), mock(classOf[TransportResponseHandler]))
          connection.setSecurityContext(new ConnectionSecurityContext {
            override def authorize(request: AuthorizationRequest): Unit = ()
          })
          intercept[SecurityException] {
            master.authorize(
              RpcMessageOrigin.remote(master.rpcEnv, connection),
              CheckForWorkerUnavailableInfoTimeout)
          }
          master.authorize(RpcMessageOrigin.local(), CheckForWorkerUnavailableInfoTimeout)
          val internalOrigin = RpcMessageOrigin.remote(master.internalRpcEnvInUse, connection)
          if (internal) {
            master.authorize(internalOrigin, CheckForWorkerUnavailableInfoTimeout)
          } else {
            intercept[SecurityException] {
              master.authorize(internalOrigin, CheckForWorkerUnavailableInfoTimeout)
            }
          }
      }
    }
  }

  private def requestSlots(): RequestSlots = RequestSlots(
    appId,
    1,
    new util.ArrayList[Integer](),
    "localhost",
    false,
    false,
    claimedUser,
    1,
    StorageInfo.ALL_TYPES_AVAILABLE_MASK)

  private def requestWorkers(): PbRequestWorkers = PbRequestWorkers.newBuilder()
    .setApplicationId(appId)
    .setUserIdentifier(PbSerDeUtils.toPbUserIdentifier(claimedUser))
    .setMaxWorkers(1)
    .build()

  private def metadata(applicationId: String): PbApplicationMetaRequest =
    PbApplicationMetaRequest.newBuilder().setAppId(applicationId).build()

  private def workerLost(): PbWorkerLost =
    WorkerLost("localhost", 19001, 19002, 19003, 19004, "security-worker-lost")

  private def denied(reference: RpcEndpointRef, request: Any, expected: String): Unit = {
    val failure = intercept[Exception] { reference.askSync[Any](request) }
    val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toSeq
    assert(
      causes.exists(cause => Option(cause.getMessage).exists(_.contains(expected))),
      s"Expected authorization rejection containing '$expected', got $failure")
    assert(!causes.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
  }

  private def customConf(): CelebornConf = new CelebornConf(false)
    .set("celeborn.auth.enabled", "false")
    .set("celeborn.rpc_service.server.bootstrap.classes", plugin)
    .set("celeborn.test.security.serverId", UUID.randomUUID().toString)

  private def connect(
      master: Master,
      conf: CelebornConf,
      clients: ArrayBuffer[RpcEnv],
      token: String,
      internal: Boolean = false): RpcEndpointRef = {
    val clientConf = conf.clone()
      .set("celeborn.rpc_service.client.bootstrap.classes", plugin)
      .set("celeborn.test.security.token", token)
    val env = RpcEnv.create(
      s"policy-client-${clients.size}",
      "rpc_service",
      "localhost",
      0,
      clientConf,
      Role.CLIENT,
      None)
    clients += env
    env.setupEndpointRef(
      if (internal) master.internalRpcEnvInUse.address else master.rpcEnv.address,
      if (internal) RpcNameConstants.MASTER_INTERNAL_EP else RpcNameConstants.MASTER_EP)
  }

  private def registerWorker(): PbRegisterWorker =
    PbRegisterWorker.newBuilder()
      .setHost("localhost").setRpcPort(19001).setPushPort(19002).setFetchPort(19003)
      .setReplicatePort(19004).setInternalPort(19005).setNetworkLocation("/test")
      .setRequestId("security-register-worker").build()

  private def withMaster(config: CelebornConf)(
      test: (
          Master,
          CelebornConf,
          ArrayBuffer[RpcEnv],
          LinkedBlockingQueue[Throwable]) => Unit): Unit = {
    val directory = Files.createTempDirectory("celeborn-master-security-")
    val properties = Files.createFile(directory.resolve("empty.conf"))
    val conf = config
      .set(CelebornConf.MASTER_HOST.key, "localhost")
      .set(
        CelebornConf.INTERNAL_PORT_ENABLED.key,
        config.get(CelebornConf.INTERNAL_PORT_ENABLED.key, "true"))
      .set(CelebornConf.HA_ENABLED.key, "false")
      .set("celeborn.metrics.enabled", "false")
      .set("celeborn.rpc.io.threads", "1")
      .set("celeborn.rpc.dispatcher.threads", "1")
      .set(CelebornConf.ENDPOINT_VERIFIER_SEPARATE_ENABLED.key, "false")
      .set("celeborn.rpc.askTimeout", "5s")
    val errors = new LinkedBlockingQueue[Throwable]()
    val args = new MasterArguments(
      Array(
        "--host",
        "localhost",
        "--port",
        "0",
        "--internal-port",
        "0",
        "--properties-file",
        properties.toString),
      conf)
    val master = new Master(conf, args) {
      override def onError(cause: Throwable): Unit = {
        if (cause.isInstanceOf[SecurityException]) errors.add(cause)
        else super.onError(cause)
      }
    }
    val clients = ArrayBuffer.empty[RpcEnv]
    try test(master, conf, clients, errors)
    finally {
      clients.foreach(_.shutdown())
      clients.foreach(_.awaitTermination())
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
      master.rpcEnv.awaitTermination()
      if (conf.internalPortEnabled) {
        master.internalRpcEnvInUse.shutdown()
        master.internalRpcEnvInUse.awaitTermination()
      }
      AuthorizationTestBootstrap.clear(conf.get("celeborn.test.security.serverId"))
      JavaUtils.deleteRecursively(directory.toFile)
    }
  }
}
