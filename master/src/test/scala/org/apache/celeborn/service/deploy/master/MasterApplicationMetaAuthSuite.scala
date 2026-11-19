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

import java.io.{ByteArrayOutputStream, DataOutputStream, ObjectOutputStream, PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.util

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo
import org.apache.celeborn.common.protocol.{PbApplicationMeta, PbApplicationMetaRequest, PbRegisterWorkerResponse, PbReviseLostShufflesResponse, PbWorkerEventResponse, RpcNameConstants, StorageInfo, TransportModuleConstants, WorkerEventType}
import org.apache.celeborn.common.protocol.message.ControlMessages.{CheckQuota, CheckQuotaResponse, CheckWorkersAvailable, HeartbeatFromWorker, HeartbeatFromWorkerResponse, RegisterWorker, RemoveWorkersUnavailableInfo, ReportWorkerDecommission, ReportWorkerUnavailable, ReviseLostShuffles, WorkerEventRequest, WorkerExclude, WorkerLost}
import org.apache.celeborn.common.rpc.{ClientSaslContextBuilder, RpcAddress, RpcEndpointRef, RpcEnv, RpcSecurityContextBuilder}
import org.apache.celeborn.common.rpc.netty.{NettyRpcEnv, RpcFailure}
import org.apache.celeborn.common.serializer.JavaSerializer
import org.apache.celeborn.common.util.ByteBufferInputStream

/**
 * End-to-end authorization checks for application-scoped Master RPCs with auth enabled,
 * driving the real SASL registration path rather than a mocked client id (as in [[MasterSuite]]).
 *
 * It covers the whole chain the security guarantee rests on: registration sets the
 * connection's client id, and checkAuth enforces it. A regression that stopped setting
 * the client id would silently turn checkAuth into a no-op and reopen the cross-tenant
 * secret leak while the mocked unit test still passed; this suite catches that.
 */
class MasterApplicationMetaAuthSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with MasterClusterFeature {

  private val conf = new CelebornConf()
    .set(CelebornConf.AUTH_ENABLED.key, "true")
    .set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
    // A single dispatcher makes the reply after a one-way message a completion barrier.
    .set("celeborn.rpc.dispatcher.threads", "1")
    .set("celeborn.rpc.RpcEndpointVerifier.separate.enabled", "false")

  private var master: Master = _
  private var externalAddress: RpcAddress = _
  private var internalAddress: RpcAddress = _
  private val clientEnvs = new ArrayBuffer[RpcEnv]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    master = setupMasterWithRandomPort(conf.getAll.toMap)
    externalAddress = master.rpcEnv.address
    internalAddress = master.internalRpcEnvInUse.address
  }

  override def afterAll(): Unit = {
    clientEnvs.foreach(_.shutdown())
    if (master != null) {
      try {
        shutdownMaster()
      } finally {
        master.internalRpcEnvInUse.shutdown()
        master.internalRpcEnvInUse.awaitTermination()
      }
    }
    super.afterAll()
  }

  private def metaRequest(appId: String): PbApplicationMetaRequest =
    PbApplicationMetaRequest.newBuilder().setAppId(appId).build()

  // A client env that authenticates and registers `appId` with the master over the
  // external port, exactly as an application (LifecycleManager) does.
  private def registeredAppRef(appId: String, secret: String): RpcEndpointRef = {
    val securityContext = new RpcSecurityContextBuilder()
      .withClientSaslContext(
        new ClientSaslContextBuilder()
          .withAddRegistrationBootstrap(true)
          .withAppId(appId)
          .withSaslUser(appId)
          .withSaslPassword(secret)
          .withRegistrationInfo(new RegistrationInfo())
          .build())
      .build()
    val env = RpcEnv.create(
      s"client-$appId",
      TransportModuleConstants.RPC_SERVICE_MODULE,
      "localhost",
      0,
      conf,
      Role.CLIENT,
      Some(securityContext))
    clientEnvs += env
    env.setupEndpointRef(externalAddress, RpcNameConstants.MASTER_EP)
  }

  // A worker-like env reaching the master over the unauthenticated internal port, where
  // no per-application client id is set on the connection.
  private def workerInternalRef(): RpcEndpointRef = {
    val env = RpcEnv.create(
      "worker-internal",
      TransportModuleConstants.RPC_SERVICE_MODULE,
      "localhost",
      0,
      conf,
      Role.WORKER,
      None)
    clientEnvs += env
    env.setupEndpointRef(internalAddress, RpcNameConstants.MASTER_INTERNAL_EP)
  }

  private def stackTraceOf(t: Throwable): String = {
    val sw = new StringWriter()
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }

  test("PbApplicationMetaRequest is authorized against the registered application") {
    val victimApp = "victim-app"
    val victimSecret = "victim-secret"
    val attackerApp = "attacker-app"
    val attackerSecret = "attacker-secret"

    val victimRef = registeredAppRef(victimApp, victimSecret)
    // Reading its own meta confirms the victim's secret is planted in the master.
    assert(victimRef.askSync[PbApplicationMeta](metaRequest(victimApp)).getSecret == victimSecret)

    // The attacker successfully registers its own app (so the connection is genuine and
    // authenticated), but the registration path set its client id to the attacker's app,
    // so checkAuth rejects the cross-application read of the victim's secret.
    val attackerRef = registeredAppRef(attackerApp, attackerSecret)
    assert(
      attackerRef.askSync[PbApplicationMeta](metaRequest(attackerApp)).getSecret == attackerSecret)
    val e = intercept[Exception] {
      attackerRef.askSync[PbApplicationMeta](metaRequest(victimApp))
    }
    assert(stackTraceOf(e).contains(s"not authorized for application $victimApp"))

    // A worker over the internal port carries no client id, so it can still fetch the
    // victim's meta — the legitimate path must keep working.
    assert(
      workerInternalRef().askSync[PbApplicationMeta](metaRequest(victimApp)).getSecret
        == victimSecret)
  }

  test("PbReviseLostShuffles is authorized against the registered application") {
    val victimApp = "revise-victim-app"
    val attackerApp = "revise-attacker-app"

    val victimRef = registeredAppRef(victimApp, "revise-victim-secret")
    val ownResponse = victimRef.askSync[PbReviseLostShufflesResponse](
      ReviseLostShuffles(victimApp, util.Arrays.asList[Integer](1), "victim-request"))
    assert(ownResponse.getSuccess)

    val victimShuffles = master.statusSystem.registeredAppAndShuffles.get(victimApp)
    assert(victimShuffles.size() == 1)
    assert(victimShuffles.contains(1))

    val attackerRef = registeredAppRef(attackerApp, "revise-attacker-secret")
    val attackerResponse = attackerRef.askSync[PbReviseLostShufflesResponse](
      ReviseLostShuffles(attackerApp, util.Arrays.asList[Integer](2), "attacker-request"))
    assert(attackerResponse.getSuccess)

    val e = intercept[Exception] {
      attackerRef.askSync[PbReviseLostShufflesResponse](
        ReviseLostShuffles(victimApp, util.Arrays.asList[Integer](3), "cross-app-request"))
    }
    assert(stackTraceOf(e).contains(s"not authorized for application $victimApp"))
    assert(victimShuffles.size() == 1)
    assert(!victimShuffles.contains(3))
  }

  test("registered applications cannot issue worker events") {
    val worker = new WorkerInfo("localhost", 18001, 18002, 18003, 18004, 18005)
    val disk = new DiskInfo("/event-test", 1024L * 1024 * 1024, 0L, 0L, 0L, StorageInfo.Type.HDD)
    val internal = workerInternalRef()
    assert(internal.askSync[PbRegisterWorkerResponse](RegisterWorker(
      worker.host,
      worker.rpcPort,
      worker.pushPort,
      worker.fetchPort,
      worker.replicatePort,
      worker.internalPort,
      "/default-rack",
      Map(disk.mountPoint -> disk),
      Map.empty,
      "event-register-worker")).getSuccess)

    val appId = "worker-event-app"
    val secret = "worker-event-secret"
    val app = registeredAppRef(appId, secret)
    assert(app.askSync[PbApplicationMeta](metaRequest(appId)).getSecret == secret)
    assert(master.statusSystem.availableWorkers.contains(worker))

    val heartbeat = HeartbeatFromWorker(
      worker.host,
      worker.rpcPort,
      worker.pushPort,
      worker.fetchPort,
      worker.replicatePort,
      Seq(disk),
      util.Collections.emptyMap(),
      util.Collections.emptySet(),
      false,
      WorkerStatus.normalWorkerStatus())

    try {
      Seq(WorkerEventType.Immediately, WorkerEventType.Decommission).foreach { event =>
        val failure = intercept[Exception] {
          app.askSync[PbWorkerEventResponse](WorkerEventRequest(
            util.Arrays.asList(worker),
            event,
            "app-worker-event"))
        }
        assertSecurityFailure(failure)
      }
      assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(master.statusSystem.availableWorkers.contains(worker))
      assert(!master.statusSystem.workerEventInfos.containsKey(worker))
      val unchanged = internal.askSync[HeartbeatFromWorkerResponse](heartbeat)
      assert(unchanged.registered)
      assert(unchanged.workerEvent == WorkerEventType.None)

      // The HTTP management path issues the same event through local self.askSync.
      assert(master.handleWorkerEvent(WorkerEventType.Immediately, Seq(worker))._1)
      assert(!master.statusSystem.availableWorkers.contains(worker))
      val delivered = internal.askSync[HeartbeatFromWorkerResponse](heartbeat)
      assert(delivered.registered)
      assert(delivered.workerEvent == WorkerEventType.Immediately)
    } finally {
      master.statusSystem.workersMap.remove(worker.toUniqueId)
      master.statusSystem.availableWorkers.remove(worker)
      master.statusSystem.workerEventInfos.remove(worker)
    }
  }

  private def assertSecurityFailure(failure: Throwable): Unit = {
    val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toSeq
    assert(causes.exists(_.isInstanceOf[SecurityException]), stackTraceOf(failure))
    assert(stackTraceOf(failure).contains("External Master RPC is not authorized"))
  }

  private def registration(worker: WorkerInfo, disk: DiskInfo) = RegisterWorker(
    worker.host,
    worker.rpcPort,
    worker.pushPort,
    worker.fetchPort,
    worker.replicatePort,
    worker.internalPort,
    "/default-rack",
    Map(disk.mountPoint -> disk),
    Map.empty,
    "origin-register-worker")

  private def workerLost(worker: WorkerInfo) = WorkerLost(
    worker.host,
    worker.rpcPort,
    worker.pushPort,
    worker.fetchPort,
    worker.replicatePort,
    "origin-worker-lost")

  private def withWorker(f: (WorkerInfo, DiskInfo, RpcEndpointRef) => Unit): Unit = {
    val worker = new WorkerInfo("localhost", 18101, 18102, 18103, 18104, 18105)
    val disk = new DiskInfo("/origin-test", 1024L * 1024 * 1024, 0L, 0L, 0L, StorageInfo.Type.HDD)
    val internal = workerInternalRef()
    assert(internal.askSync[PbRegisterWorkerResponse](registration(worker, disk)).getSuccess)
    try {
      f(worker, disk, internal)
    } finally {
      master.statusSystem.workersMap.remove(worker.toUniqueId)
      master.statusSystem.availableWorkers.remove(worker)
      master.statusSystem.lostWorkers.remove(worker)
      master.statusSystem.workerEventInfos.remove(worker)
      master.statusSystem.excludedWorkers.remove(worker)
      master.statusSystem.manuallyExcludedWorkers.remove(worker)
      master.statusSystem.shutdownWorkers.remove(worker)
      master.statusSystem.decommissionWorkers.remove(worker)
      master.statusSystem.workerLostEvents.remove(worker)
    }
  }

  // Exercise the existing Java object stream path without adding a protobuf codec.
  private def askLegacy(env: NettyRpcEnv, message: Any): Unit = {
    val bytes = new ByteArrayOutputStream()
    val header = new DataOutputStream(bytes)
    header.writeBoolean(false)
    header.writeBoolean(true)
    header.writeUTF(externalAddress.host)
    header.writeInt(externalAddress.port)
    header.writeUTF(RpcNameConstants.MASTER_EP)
    val objects = new ObjectOutputStream(header)
    objects.writeObject(message)
    objects.close()
    val connection = env.createClient(externalAddress)
    val response = connection.sendRpcSync(ByteBuffer.wrap(bytes.toByteArray), 5000)
    val input = new JavaSerializer(conf).newInstance()
      .deserializeStream(new ByteBufferInputStream(response))
    try {
      input.readObject[Any]() match {
        case RpcFailure(cause) => throw cause
        case _ => // Successful legacy responses need no application payload decoding here.
      }
    } finally {
      input.close()
    }
  }

  Seq("register", "heartbeat", "unavailable", "decommission", "exclude", "lost", "remove")
    .foreach { operation =>
      test(s"registered applications cannot issue worker $operation requests") {
        withWorker { (worker, disk, _) =>
          val appId = s"worker-$operation-app"
          val app = registeredAppRef(appId, s"$appId-secret")
          assert(app.askSync[PbApplicationMeta](metaRequest(appId)).getSecret == s"$appId-secret")
          val workers = util.Collections.singletonList(worker)
          val request = operation match {
            case "register" => registration(worker, disk)
            case "heartbeat" => HeartbeatFromWorker(
                worker.host,
                worker.rpcPort,
                worker.pushPort,
                worker.fetchPort,
                worker.replicatePort,
                Seq(disk),
                util.Collections.emptyMap(),
                util.Collections.emptySet(),
                false,
                WorkerStatus.normalWorkerStatus())
            case "unavailable" => ReportWorkerUnavailable(workers)
            case "decommission" => ReportWorkerDecommission(workers)
            case "exclude" => WorkerExclude(workers, util.Collections.emptyList(), "exclude")
            case "lost" => workerLost(worker)
            case "remove" => RemoveWorkersUnavailableInfo(workers, "remove")
          }
          val lostTime = System.currentTimeMillis()
          master.statusSystem.lostWorkers.put(worker, lostTime)
          val failure = intercept[Exception] {
            if (operation == "exclude") {
              askLegacy(clientEnvs.last.asInstanceOf[NettyRpcEnv], request)
            } else {
              app.askSync[Any](request)
            }
          }
          assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
          assert(master.statusSystem.availableWorkers.contains(worker))
          // A reply encoding failure can happen after mutation; it is not an authorization denial.
          assert(master.statusSystem.lostWorkers.containsKey(worker))
          assert(master.statusSystem.lostWorkers.get(worker) == lostTime)
          assert(!master.statusSystem.manuallyExcludedWorkers.contains(worker))
          assert(!master.statusSystem.shutdownWorkers.contains(worker))
          assert(!master.statusSystem.decommissionWorkers.contains(worker))
          assertSecurityFailure(failure)
        }
      }
    }

  test("registered applications cannot send one-way worker loss") {
    withWorker { (worker, _, internal) =>
      val appId = "one-way-worker-app"
      val app = registeredAppRef(appId, "one-way-secret")
      app.send(workerLost(worker))
      // This fixture has one dispatcher thread; the following reply is a completion barrier.
      assert(app.askSync[PbApplicationMeta](metaRequest(appId)).getSecret == "one-way-secret")
      assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(master.statusSystem.availableWorkers.contains(worker))

      internal.askSync[Any](workerLost(worker))
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))

      val disk = new DiskInfo("/local-test", 1024L * 1024 * 1024, 0L, 0L, 0L, StorageInfo.Type.HDD)
      assert(internal.askSync[PbRegisterWorkerResponse](registration(worker, disk)).getSuccess)
      master.self.send(workerLost(worker))
      master.self.askSync[Any](CheckWorkersAvailable())
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))
    }
  }

  test("registered applications retain quota and worker availability queries") {
    val app = registeredAppRef("queries-app", "queries-secret")
    assert(
      app.askSync[CheckQuotaResponse](CheckQuota(UserIdentifier("tenant", "user"))).isAvailable)
    app.askSync[Any](CheckWorkersAvailable())
  }
}
