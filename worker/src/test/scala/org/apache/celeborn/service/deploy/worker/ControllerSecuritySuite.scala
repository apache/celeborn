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

import java.nio.file.Files
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.network.sasl.SecretRegistryImpl
import org.apache.celeborn.common.network.security.AuthorizationTestBootstrap
import org.apache.celeborn.common.network.security.SecurityOperation._
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, Utils}
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class ControllerSecuritySuite extends CelebornFunSuite {
  private val plugin = classOf[AuthorizationTestBootstrap].getName
  private val appId = "tenant/application"
  private val claimedUser = UserIdentifier("claimed", "other")
  private val effectiveUser = UserIdentifier("tenant", "alice")

  test("Controller authorizes every operation and records the effective writer owner") {
    withWorker(custom = true) { (worker, conf, clients) =>
      val allowed = connect(worker.rpcEnv, conf, clients, "application", RpcNameConstants.WORKER_EP)
      val deniedPeer = connect(worker.rpcEnv, conf, clients, "denied", RpcNameConstants.WORKER_EP)
      val request = reserveSlots(1)
      denied(deniedPeer, request, RESERVE_SLOTS)
      assert(worker.partitionLocationInfo.isEmpty)

      assert(allowed.askSync[ReserveSlotsResponse](request).status == StatusCode.SUCCESS)
      val shuffleKey = Utils.makeShuffleKey(appId, 1)
      val primary = request.primaryLocations.get(0)
      val replica = request.replicaLocations.get(0)
      Seq(
        worker.partitionLocationInfo.getPrimaryLocation(shuffleKey, primary.getUniqueId),
        worker.partitionLocationInfo.getReplicaLocation(shuffleKey, replica.getUniqueId))
        .foreach { location =>
          val writer = location.asInstanceOf[WorkingPartition].getFileWriter
          assert(writer.getCurrentFileInfo.getUserIdentifier == effectiveUser)
        }

      val commitsBefore = worker.shuffleCommitInfos.size()
      denied(
        deniedPeer,
        CommitFiles(
          appId,
          1,
          util.Collections.singletonList(primary.getUniqueId),
          util.Collections.singletonList(replica.getUniqueId),
          Array.emptyIntArray,
          1L),
        COMMIT_FILES)
      assert(worker.shuffleCommitInfos.size() == commitsBefore)
      denied(
        deniedPeer,
        DestroyWorkerSlots(
          shuffleKey,
          util.Collections.singletonList(primary.getUniqueId),
          util.Collections.singletonList(replica.getUniqueId)),
        DESTROY_WORKER_SLOTS)
      assert(worker.partitionLocationInfo.getPrimaryLocation(
        shuffleKey,
        primary.getUniqueId) != null)
      assert(worker.partitionLocationInfo.getReplicaLocation(
        shuffleKey,
        replica.getUniqueId) != null)

      denied(allowed, reserveSlots(2).copy(applicationId = "foreign/application"), RESERVE_SLOTS)
      val missingUser =
        connect(worker.rpcEnv, conf, clients, "missing-user", RpcNameConstants.WORKER_EP)
      denied(missingUser, reserveSlots(3), "did not resolve")
      assert(worker.partitionLocationInfo.getPrimaryLocation(
        Utils.makeShuffleKey(appId, 3),
        primary.getUniqueId) == null)
    }
  }

  test("default Controller policy retains claimed ownership when authentication is disabled") {
    withWorker(custom = false) { (worker, conf, clients) =>
      val peer = connect(worker.rpcEnv, conf, clients, "", RpcNameConstants.WORKER_EP)
      val request = reserveSlots(1)
      assert(peer.askSync[ReserveSlotsResponse](request).status == StatusCode.SUCCESS)
      val location = worker.partitionLocationInfo.getPrimaryLocation(
        Utils.makeShuffleKey(appId, 1),
        request.primaryLocations.get(0).getUniqueId)
      assert(location.asInstanceOf[WorkingPartition].getFileWriter
        .getCurrentFileInfo.getUserIdentifier == claimedUser)
    }
  }

  test("only an authorized service can install application metadata on the internal endpoint") {
    val conf = baseConf().set("celeborn.rpc_service.server.bootstrap.classes", plugin)
    val server =
      RpcEnv.create("worker-meta-security", "rpc_service", "localhost", 0, conf, Role.WORKER, None)
    val registry = new SecretRegistryImpl()
    server.setupEndpoint(
      RpcNameConstants.WORKER_INTERNAL_EP,
      new InternalRpcEndpoint(server, conf, registry))
    val clients = ArrayBuffer.empty[RpcEnv]
    try {
      val application =
        connect(server, conf, clients, "application", RpcNameConstants.WORKER_INTERNAL_EP)
      val service = connect(server, conf, clients, "service", RpcNameConstants.WORKER_INTERNAL_EP)
      val meta = PbApplicationMeta.newBuilder().setAppId(appId).setSecret("test-secret").build()
      val message = new TransportMessage(MessageType.APPLICATION_META, meta.toByteArray)
      val observed =
        AuthorizationTestBootstrap.requests(conf.get("celeborn.test.security.serverId"))
      observed.clear()
      application.send(message)
      val decision = observed.poll(5, TimeUnit.SECONDS)
      assert(
        !registry.isRegistered(appId),
        "An application must not install a secret on the Worker")
      assert(decision != null && decision.getOperation == INSTALL_APPLICATION_META)
      service.send(message)
      JavaUtils.timeOutOrMeetCondition(() => registry.isRegistered(appId))
      assert(registry.getSecretKey(appId) == "test-secret")
    } finally {
      clients.foreach(_.shutdown())
      clients.foreach(_.awaitTermination())
      server.shutdown()
      server.awaitTermination()
      AuthorizationTestBootstrap.clear(conf.get("celeborn.test.security.serverId"))
    }
  }

  private def reserveSlots(shuffleId: Int): ReserveSlots = {
    val primary =
      new PartitionLocation(0, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY)
    val replica =
      new PartitionLocation(1, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.REPLICA)
    ReserveSlots(
      appId,
      shuffleId,
      util.Collections.singletonList(primary),
      util.Collections.singletonList(replica),
      1048576L,
      PartitionSplitMode.SOFT,
      PartitionType.REDUCE,
      false,
      claimedUser,
      10000L)
  }

  private def denied(reference: RpcEndpointRef, request: Any, expected: String): Unit = {
    val failure = intercept[Exception] { reference.askSync[Any](request) }
    val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toSeq
    assert(
      causes.exists(cause => Option(cause.getMessage).exists(_.contains(expected))),
      s"Expected authorization rejection containing '$expected', got $failure")
    assert(!causes.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
  }

  private def baseConf(): CelebornConf = new CelebornConf(false)
    .set("celeborn.auth.enabled", "false")
    .set("celeborn.internal.port.enabled", "false")
    .set("celeborn.metrics.enabled", "false")
    .set("celeborn.rpc.io.threads", "1")
    .set("celeborn.rpc.dispatcher.threads", "2")
    .set("celeborn.rpc.askTimeout", "5s")
    .set("celeborn.test.security.serverId", UUID.randomUUID().toString)

  private def connect(
      server: RpcEnv,
      conf: CelebornConf,
      clients: ArrayBuffer[RpcEnv],
      token: String,
      endpoint: String): RpcEndpointRef = {
    val clientConf = conf.clone()
    if (token.nonEmpty) {
      clientConf.set("celeborn.rpc_service.client.bootstrap.classes", plugin)
        .set("celeborn.test.security.token", token)
    }
    val client = RpcEnv.create(
      s"worker-policy-${clients.size}",
      "rpc_service",
      "localhost",
      0,
      clientConf,
      Role.CLIENT,
      None)
    clients += client
    client.setupEndpointRef(RpcAddress("localhost", server.address.port), endpoint)
  }

  private def withWorker(custom: Boolean)(
      test: (Worker, CelebornConf, ArrayBuffer[RpcEnv]) => Unit): Unit = {
    val directory = Files.createTempDirectory("celeborn-controller-security-")
    val properties = Files.createFile(directory.resolve("empty.conf"))
    val conf = baseConf()
      .set("celeborn.worker.storage.dirs", directory.resolve("storage").toString)
      .set(CelebornConf.WORKER_DISK_MONITOR_ENABLED.key, "false")
      .set("celeborn.push.io.threads", "1")
      .set("celeborn.fetch.io.threads", "1")
    if (custom) conf.set("celeborn.rpc_service.server.bootstrap.classes", plugin)
    val clients = ArrayBuffer.empty[RpcEnv]
    val worker = new Worker(
      conf,
      new WorkerArguments(
        Array("--host", "localhost", "--port", "0", "--properties-file", properties.toString),
        conf))
    worker.controller.init(worker)
    worker.rpcEnv.setupEndpoint(RpcNameConstants.WORKER_EP, worker.controller)
    try test(worker, conf, clients)
    finally {
      clients.foreach(_.shutdown())
      clients.foreach(_.awaitTermination())
      worker.rpcEnv.shutdown()
      worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      worker.rpcEnv.awaitTermination()
      MemoryManager.reset()
      AuthorizationTestBootstrap.clear(conf.get("celeborn.test.security.serverId"))
      JavaUtils.deleteRecursively(directory.toFile)
    }
  }
}
