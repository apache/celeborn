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
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import io.netty.channel.Channel
import org.mockito.Mockito.mock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{ApplicationMeta, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.client.{TransportClient, TransportResponseHandler}
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo
import org.apache.celeborn.common.network.security.{AuthorizationRequest, AuthorizationTestBootstrap, ConnectionSecurityContext}
import org.apache.celeborn.common.network.security.SecurityOperation._
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.rpc.netty.NettyRpcEnv
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, PbSerDeUtils}
import org.apache.celeborn.service.deploy.master.quota.{QuotaManager, QuotaStatus}

class MasterSecuritySuite extends CelebornFunSuite {
  private val appId = "tenant/application"
  private val effectiveUser = UserIdentifier("tenant", "alice")
  private val claimedUser = UserIdentifier("claimed", "other")
  private val plugin = classOf[AuthorizationTestBootstrap].getName

  test("remote application cannot remove a worker with a one-way request") {
    withMaster() { (master, conf, clients, errors) =>
      val worker = new WorkerInfo("localhost", 19001, 19002, 19003, 19004, 19005)
      withClue("local worker registration: ") {
        assert(master.self.askSync[PbRegisterWorkerResponse](registerWorker()).getSuccess)
      }
      assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      val application = withClue("native application endpoint setup: ") {
        connectNative(master, conf, clients, "application")
      }
      application.send(WorkerLost(
        worker.host,
        worker.rpcPort,
        worker.pushPort,
        worker.fetchPort,
        worker.replicatePort,
        "security-remote-worker-lost"))
      val failure = errors.poll(5, TimeUnit.SECONDS)
      assert(
        master.statusSystem.workersMap.containsKey(worker.toUniqueId),
        "A remotely authenticated application must not remove a worker via send")
      assert(failure != null, "The denied one-way request must report an authorization failure")
      assert(failure.isInstanceOf[SecurityException], failure.toString)
    }
  }

  test("custom policy covers wire-supported Master requests on external and internal endpoints") {
    withMaster(customConf()) { (master, conf, clients, _) =>
      Seq(false, true).foreach { internal =>
        val peer = connectCustom(master, conf, clients, "denied", internal)
        wireRequests.foreach { case (operation, message) => denied(peer, message, operation) }
      }
      assert(master.statusSystem.workersMap.isEmpty)
      assert(master.statusSystem.applicationInfos.isEmpty)
      assert(master.statusSystem.registeredAppAndShuffles.isEmpty)
    }
  }

  test("worker exclusion retains local dispatch and maps authorization to a service operation") {
    withMaster() { (master, _, _, _) =>
      val worker = new WorkerInfo("localhost", 19001, 19002, 19003, 19004, 19005)
      val request = WorkerExclude(
        util.Collections.singletonList(worker),
        util.Collections.emptyList(),
        "security-exclude")
      val observed = ArrayBuffer.empty[AuthorizationRequest]
      val rejection = new SecurityException("Worker exclusion denied by plugin")
      val connection =
        new TransportClient(mock(classOf[Channel]), mock(classOf[TransportResponseHandler]))
      connection.setSecurityContext(new ConnectionSecurityContext {
        override def authorize(request: AuthorizationRequest): Unit = {
          observed += request
          throw rejection
        }
      })

      // WorkerExclude is dispatched by Master.exclude through self.askSync and has no wire decoder.
      // Exercise the authorization mapping directly without introducing a remote RPC capability.
      val remote = RpcRequestContext.remote(RpcAddress("localhost", 12345), connection)
      assert(intercept[SecurityException](master.authorize(remote, request)) eq rejection)
      assert(observed.size == 1)
      assert(observed.head.getOperation == EXCLUDE_WORKERS)
      assert(observed.head.getScope == AuthorizationRequest.Scope.SERVICE)

      assert(master.exclude(Seq(worker), Seq.empty)._1)
      assert(master.statusSystem.manuallyExcludedWorkers.contains(worker))
      assert(master.exclude(Seq.empty, Seq(worker))._1)
      assert(!master.statusSystem.manuallyExcludedWorkers.contains(worker))
    }
  }

  test("application policy resolves the same user for ownership tags and quota") {
    withMaster(customConf()) { (master, conf, clients, _) =>
      val peer = connectCustom(master, conf, clients, "application")
      peer.askSync[Any](RegisterApplicationInfo(
        appId,
        claimedUser,
        util.Collections.emptyMap[String, String](),
        "security-app-info"))
      assert(master.statusSystem.applicationInfos.get(appId).userIdentifier == effectiveUser)

      peer.askSync[RequestSlotsResponse](requestSlots())
      peer.askSync[PbRequestWorkersResponse](requestWorkers())
      assert(master.effectiveUsers.get(REQUEST_SLOTS) == effectiveUser)
      assert(master.effectiveUsers.get(REQUEST_WORKERS) == effectiveUser)

      val quotaField =
        classOf[Master].getDeclaredFields.find(_.getType == classOf[QuotaManager]).get
      quotaField.setAccessible(true)
      val quota = quotaField.get(master).asInstanceOf[QuotaManager]
      quota.userQuotaStatus.put(effectiveUser, QuotaStatus(true, "effective user exceeded"))
      quota.userQuotaStatus.remove(claimedUser)
      assert(!peer.askSync[CheckQuotaResponse](CheckQuota(claimedUser)).isAvailable)
      assert(!peer.askSync[PbCheckWorkersAvailableResponse](CheckWorkersAvailable()).getAvailable)

      // The plugin permits two apps in a tenant even though the native client ID is different.
      assert(peer.askSync[PbReviseLostShufflesResponse](ReviseLostShuffles(
        "tenant/other",
        util.Collections.emptyList[Integer](),
        "security-revise")).getSuccess)
      denied(
        peer,
        ReviseLostShuffles(
          "foreign/application",
          util.Collections.emptyList[Integer](),
          "security-foreign"),
        REVISE_LOST_SHUFFLES)

      val missingUser = connectCustom(master, conf, clients, "missing-user")
      denied(
        missingUser,
        RegisterApplicationInfo(
          "tenant/missing-user",
          claimedUser,
          util.Collections.emptyMap[String, String](),
          "security-missing-user"),
        "did not resolve")
      assert(!master.statusSystem.applicationInfos.containsKey("tenant/missing-user"))
    }
  }

  test("quota and worker availability remain explicit application operations") {
    val conf = customConf()
      .set("celeborn.test.security.deniedOperations", s"$CHECK_QUOTA,$CHECK_WORKERS_AVAILABLE")
    withMaster(conf) { (master, config, clients, _) =>
      val peer = connectCustom(master, config, clients, "application")
      denied(peer, CheckQuota(claimedUser), CHECK_QUOTA)
      denied(peer, CheckWorkersAvailable(), CHECK_WORKERS_AVAILABLE)
    }
  }

  test("internal forwarding retains the caller policy for worker loss and metadata") {
    withMaster(customConf()) { (master, conf, clients, _) =>
      val worker = new WorkerInfo("localhost", 19001, 19002, 19003, 19004, 19005)
      val service = connectCustom(master, conf, clients, "service", internal = true)
      val application = connectCustom(master, conf, clients, "application", internal = true)
      assert(service.askSync[PbRegisterWorkerResponse](registerWorker()).getSuccess)
      val foreignApp = "foreign/application"
      master.statusSystem.applicationMetas.put(foreignApp, ApplicationMeta(foreignApp, "secret"))
      assert(service.askSync[PbApplicationMeta](metadata(foreignApp)).getSecret == "secret")
      denied(application, metadata(foreignApp), GET_APPLICATION_META)
      denied(application, workerLost(), WORKER_LOST)

      val observed =
        AuthorizationTestBootstrap.requests(conf.get("celeborn.test.security.serverId"))
      observed.clear()
      application.send(workerLost())
      val decision = observed.poll(5, TimeUnit.SECONDS)
      assert(decision != null && decision.getOperation == WORKER_LOST)
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

  test("Master maintenance messages require a local origin") {
    withMaster(customConf()) { (master, conf, clients, errors) =>
      val service = connectCustom(master, conf, clients, "service")
      Seq(
        PbCheckForWorkerTimeout.newBuilder().build(),
        CheckForApplicationTimeOut,
        CheckForDFSExpiredDirsTimeout).foreach { message =>
        service.send(message)
        val error = errors.poll(5, TimeUnit.SECONDS)
        assert(error != null && error.isInstanceOf[SecurityException])
        assert(error.getMessage.contains("local RPC messages"))
      }

      // This internal object has no wire encoding, so exercise its authorization hook directly.
      val clientEnv = clients.head.asInstanceOf[NettyRpcEnv]
      val remote = RpcRequestContext.remote(
        clientEnv.address,
        clientEnv.createClient(master.rpcEnv.address))
      val failure = intercept[SecurityException] {
        master.authorize(remote, CheckForWorkerUnavailableInfoTimeout)
      }
      assert(failure.getMessage.contains("local RPC messages"))
      assert(master.authorize(
        RpcRequestContext.local(master.rpcEnv.address),
        CheckForWorkerUnavailableInfoTimeout) == CheckForWorkerUnavailableInfoTimeout)
    }
  }

  private def wireRequests: Seq[(String, Any)] = Seq(
    REGISTER_APPLICATION_INFO -> RegisterApplicationInfo(
      appId,
      claimedUser,
      util.Collections.emptyMap[String, String]()),
    APPLICATION_HEARTBEAT -> HeartbeatFromApplication(
      appId,
      0L,
      0L,
      0L,
      0L,
      util.Collections.emptyMap(),
      util.Collections.emptyMap(),
      util.Collections.emptyList()),
    REGISTER_WORKER -> registerWorker(),
    REQUEST_SLOTS -> requestSlots(),
    REQUEST_WORKERS -> requestWorkers(),
    BATCH_UNREGISTER_SHUFFLES -> BatchUnregisterShuffles(
      appId,
      util.Collections.emptyList[Integer](),
      "security-batch-unregister"),
    UNREGISTER_SHUFFLE -> UnregisterShuffle(appId, 1, "security-unregister"),
    APPLICATION_LOST -> ApplicationLost(appId),
    WORKER_HEARTBEAT -> HeartbeatFromWorker(
      "localhost",
      19001,
      19002,
      19003,
      19004,
      Seq.empty,
      util.Collections.emptyMap(),
      util.Collections.emptySet(),
      false,
      WorkerStatus.normalWorkerStatus()),
    REPORT_WORKER_UNAVAILABLE -> ReportWorkerUnavailable(util.Collections.emptyList()),
    REPORT_WORKER_DECOMMISSION -> ReportWorkerDecommission(util.Collections.emptyList()),
    REVISE_LOST_SHUFFLES -> ReviseLostShuffles(
      appId,
      util.Collections.emptyList[Integer](),
      "security-revise"),
    WORKER_LOST -> workerLost(),
    CHECK_QUOTA -> CheckQuota(claimedUser),
    CHECK_WORKERS_AVAILABLE -> CheckWorkersAvailable(),
    WORKER_EVENT -> PbWorkerEventRequest.newBuilder().build(),
    GET_APPLICATION_META -> metadata(appId),
    REMOVE_WORKERS_UNAVAILABLE_INFO -> PbRemoveWorkersUnavailableInfo.newBuilder().build())

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

  private def connectCustom(
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

  private def connectNative(
      master: Master,
      conf: CelebornConf,
      clients: ArrayBuffer[RpcEnv],
      appId: String): RpcEndpointRef = {
    val security = new RpcSecurityContextBuilder()
      .withClientSaslContext(new ClientSaslContextBuilder()
        .withAddRegistrationBootstrap(true).withAppId(appId)
        .withSaslUser(appId).withSaslPassword(s"secret-$appId")
        .withRegistrationInfo(new RegistrationInfo()).build()).build()
    val env = RpcEnv.create(
      s"security-client-${clients.size}",
      "rpc_service",
      "localhost",
      0,
      conf.clone(),
      Role.CLIENT,
      Some(security))
    clients += env
    env.setupEndpointRef(master.rpcEnv.address, RpcNameConstants.MASTER_EP)
  }

  private def withMaster(config: CelebornConf = new CelebornConf(false))(
      test: (
          ObservedMaster,
          CelebornConf,
          ArrayBuffer[RpcEnv],
          LinkedBlockingQueue[Throwable]) => Unit): Unit = {
    val directory = Files.createTempDirectory("celeborn-master-security-")
    val properties = Files.createFile(directory.resolve("empty.conf"))
    val conf = config
      .set("celeborn.auth.enabled", config.get("celeborn.auth.enabled", "true"))
      .set(CelebornConf.MASTER_HOST.key, "localhost")
      .set("celeborn.internal.port.enabled", "true")
      .set(CelebornConf.HA_ENABLED.key, "false")
      .set("celeborn.metrics.enabled", "false")
      .set("celeborn.rpc.io.threads", "1")
      .set("celeborn.rpc.dispatcher.threads", "2")
      .set("celeborn.rpc.askTimeout", "5s")
      .set("celeborn.quota.enabled", "true")
      .set("celeborn.quota.user.enabled", "true")
      .set("celeborn.master.userResourceConsumption.update.interval", "1h")
    val errors = new LinkedBlockingQueue[Throwable]()
    val master = new ObservedMaster(
      conf,
      new MasterArguments(
        Array(
          "--host",
          "localhost",
          "--port",
          "0",
          "--internal-port",
          "0",
          "--properties-file",
          properties.toString),
        conf),
      errors)
    val clients = ArrayBuffer.empty[RpcEnv]
    try test(master, conf, clients, errors)
    finally {
      clients.foreach(_.shutdown())
      clients.foreach(_.awaitTermination())
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
      master.internalRpcEnvInUse.shutdown()
      master.rpcEnv.awaitTermination()
      master.internalRpcEnvInUse.awaitTermination()
      AuthorizationTestBootstrap.clear(conf.get("celeborn.test.security.serverId", "native"))
      JavaUtils.deleteRecursively(directory.toFile)
    }
  }

  private class ObservedMaster(
      conf: CelebornConf,
      args: MasterArguments,
      errors: LinkedBlockingQueue[Throwable]) extends Master(conf, args) {
    val effectiveUsers = new ConcurrentHashMap[String, UserIdentifier]()

    override def onError(cause: Throwable): Unit = {
      if (cause.isInstanceOf[SecurityException]) errors.add(cause)
      else super.onError(cause)
    }

    override def handleRequestSlots(context: RpcCallContext, request: RequestSlots): Unit = {
      effectiveUsers.put(REQUEST_SLOTS, request.userIdentifier)
      super.handleRequestSlots(context, request)
    }

    override def handleRequestWorkers(context: RpcCallContext, request: PbRequestWorkers): Unit = {
      effectiveUsers.put(
        REQUEST_WORKERS,
        PbSerDeUtils.fromPbUserIdentifier(request.getUserIdentifier))
      super.handleRequestWorkers(context, request)
    }
  }
}
