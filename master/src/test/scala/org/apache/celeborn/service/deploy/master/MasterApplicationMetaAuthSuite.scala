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

import java.io.{PrintWriter, StringWriter}

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo
import org.apache.celeborn.common.protocol.{PbApplicationMeta, PbApplicationMetaRequest, RpcNameConstants, TransportModuleConstants}
import org.apache.celeborn.common.rpc.{ClientSaslContextBuilder, RpcAddress, RpcEndpointRef, RpcEnv, RpcSecurityContextBuilder}

/**
 * End-to-end authorization check for PbApplicationMetaRequest with auth enabled, driving
 * the real SASL registration path rather than a mocked client id (as in [[MasterSuite]]).
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
      shutdownMaster()
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
}
