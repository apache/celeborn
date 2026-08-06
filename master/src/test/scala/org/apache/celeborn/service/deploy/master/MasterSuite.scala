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

import scala.collection.JavaConverters._

import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo}
import org.apache.celeborn.common.network.client.{RpcResponseCallback, TransportClient}
import org.apache.celeborn.common.protocol.{PbApplicationMetaRequest, PbCheckForWorkerTimeout, PbRegisterWorker, PbRequestWorkers, PbRequestWorkersResponse}
import org.apache.celeborn.common.protocol.StorageInfo
import org.apache.celeborn.common.protocol.message.ControlMessages.{RequestSlots, RequestSlotsResponse, ReviseLostShuffles}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc.{RpcAddress, RpcCallContext}
import org.apache.celeborn.common.rpc.netty.{NettyRpcEnv, RemoteNettyRpcCallContext}
import org.apache.celeborn.common.util.{CelebornExitKind, PbSerDeUtils, ThreadUtils}

class MasterSuite extends AnyFunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MasterClusterFeature {

  // Builds a remote call context whose connection is authenticated as `clientId`;
  // null models a connection when authentication is disabled or an internal worker connection.
  private def contextForClient(clientId: String): RemoteNettyRpcCallContext = {
    val client = mock(classOf[TransportClient])
    when(client.getClientId).thenReturn(clientId)
    new RemoteNettyRpcCallContext(
      mock(classOf[NettyRpcEnv]),
      mock(classOf[RpcResponseCallback]),
      RpcAddress("localhost", 1234),
      client)
  }

  private def requestWorkers(
      master: Master,
      request: PbRequestWorkers): PbRequestWorkersResponse = {
    val context = mock(classOf[RpcCallContext])
    val captor = ArgumentCaptor.forClass(classOf[Any])
    master.handleRequestWorkers(context, request)
    verify(context).reply(captor.capture())
    captor.getValue.asInstanceOf[PbRequestWorkersResponse]
  }

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDirectory(null).toFile
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  test("test single node startup functionality") {
    withRetryOnPortBindException { () =>
      val conf = new CelebornConf()
      val randomMasterPort = selectRandomPort()
      val randomHttpPort = selectRandomPort()
      conf.set(CelebornConf.HA_ENABLED.key, "false")
      conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
      conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
      conf.set(CelebornConf.METRICS_ENABLED.key, "true")
      conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
      conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

      val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

      val masterArgs = new MasterArguments(args, conf)
      val master = new Master(conf, masterArgs)
      ThreadUtils.newThread(
        new Runnable {
          override def run(): Unit = {
            master.initialize()
          }
        },
        "master-init-thread").start()
      Thread.sleep(5000L)
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
    }
  }

  test("test dedicated internal port receives") {
    withRetryOnPortBindException { () =>
      val conf = new CelebornConf()
      val randomMasterPort = selectRandomPort()
      val randomHttpPort = selectRandomPort()
      val randomInternalPort = selectRandomPort()
      conf.set(CelebornConf.HA_ENABLED.key, "false")
      conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
      conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
      conf.set(CelebornConf.METRICS_ENABLED.key, "true")
      conf.set(CelebornConf.INTERNAL_PORT_ENABLED.key, "true")
      conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

      val args = Array(
        "-h",
        "localhost",
        "-p",
        randomMasterPort.toString,
        "--internal-port",
        randomInternalPort.toString)

      val masterArgs = new MasterArguments(args, conf)
      val master = new Master(conf, masterArgs)
      ThreadUtils.newThread(
        new Runnable {
          override def run(): Unit = {
            master.initialize()
          }
        },
        "master-init-thread").start()
      Thread.sleep(5000L)
      master.receive.applyOrElse(
        PbCheckForWorkerTimeout.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))
      master.internalRpcEndpoint.receive.applyOrElse(
        PbCheckForWorkerTimeout.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))

      master.internalRpcEndpoint.receiveAndReply(
        mock(classOf[org.apache.celeborn.common.rpc.RpcCallContext])).applyOrElse(
        PbRegisterWorker.newBuilder().build(),
        (_: Any) => fail("Unexpected message"))
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
      master.internalRpcEnvInUse.shutdown()
    }
  }

  test("test master worker host allow and deny pattern") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    conf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    conf.set(CelebornConf.METRICS_ENABLED.key, "true")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)
    conf.set(CelebornConf.ALLOW_WORKER_HOST_PATTERN.key, ".*\\.k8s\\.io$")
    conf.set(CelebornConf.DENY_WORKER_HOST_PATTERN.key, "^deny\\.k8s\\.io$")

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, conf)
    var master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(!master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(!master.workerHostAllowedToRegister("test.example.com"))
    assert(!master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()

    conf.unset(CelebornConf.ALLOW_WORKER_HOST_PATTERN)
    master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(master.workerHostAllowedToRegister("test.example.com"))
    assert(!master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()

    conf.unset(CelebornConf.DENY_WORKER_HOST_PATTERN)
    master = new Master(conf, masterArgs)
    assert(master.workerHostAllowedToRegister("test.k8s.io"))
    assert(master.workerHostAllowedToRegister("test.k8s.io.com"))
    assert(master.workerHostAllowedToRegister("test.example.com"))
    assert(master.workerHostAllowedToRegister("deny.k8s.io"))
    master.rpcEnv.shutdown()
  }

  test("handleRequestSlots replies WORKER_EXCLUDED and does not throw when no workers available") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)
    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)

    // No workers registered — availableWorkers is empty
    val requestSlots = RequestSlots(
      "app1",
      0,
      new util.ArrayList[Integer](),
      "localhost",
      shouldReplicate = false,
      shouldRackAware = false,
      new UserIdentifier("tenant", "user"),
      maxWorkers = 0,
      availableStorageTypes = 0)

    val context = mock(classOf[RpcCallContext])
    val captor = ArgumentCaptor.forClass(classOf[Any])

    // Should not throw IllegalArgumentException
    master.handleRequestSlots(context, requestSlots)

    verify(context).reply(captor.capture())
    val response = captor.getValue.asInstanceOf[RequestSlotsResponse]
    assert(response.status === StatusCode.WORKER_EXCLUDED)

    master.rpcEnv.shutdown()
  }

  test("handleRequestWorkers returns one failure when no workers are available") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, selectRandomPort().toString)
    val masterArgs = new MasterArguments(
      Array("-h", "localhost", "-p", selectRandomPort().toString),
      conf)
    val master = new Master(conf, masterArgs)
    val request = PbRequestWorkers.newBuilder()
      .setApplicationId("app1")
      .setUserIdentifier(
        PbSerDeUtils.toPbUserIdentifier(new UserIdentifier("tenant", "user")))
      .setMaxWorkers(10)
      .build()
    val response = requestWorkers(master, request)
    assert(StatusCode.fromValue(response.getStatus) === StatusCode.WORKER_EXCLUDED)
    assert(response.getWorkersCount === 0)
    master.rpcEnv.shutdown()
  }

  test("handleRequestWorkers applies limits without allocating slots") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, selectRandomPort().toString)
    conf.set(CelebornConf.MASTER_SPLIT_SLOT_ASSIGN_MAX_WORKERS.key, "2")
    val masterArgs = new MasterArguments(
      Array("-h", "localhost", "-p", selectRandomPort().toString),
      conf)
    val master = new Master(conf, masterArgs)
    val workers = (1 to 4).map { index =>
      val worker =
        new WorkerInfo(s"host$index", 1000 + index, 2000 + index, 3000 + index, 4000 + index)
      worker.networkLocation = s"/rack-$index"
      worker
    }
    master.statusSystem.availableWorkers.addAll(workers.asJava)
    val registeredShufflesBefore = master.statusSystem.registeredAppAndShuffles.size()
    val request = PbRequestWorkers.newBuilder()
      .setApplicationId("app1")
      .setUserIdentifier(
        PbSerDeUtils.toPbUserIdentifier(new UserIdentifier("tenant", "user")))
      .setMaxWorkers(3)
      .setAvailableStorageTypes(StorageInfo.MEMORY_MASK)
      .build()
    val response = requestWorkers(master, request)
    val responseWorkers =
      response.getWorkersList.asScala.map(PbSerDeUtils.fromPbWorkerInfo).toSet
    assert(StatusCode.fromValue(response.getStatus) === StatusCode.SUCCESS)
    assert(responseWorkers.size === 2)
    assert(responseWorkers.subsetOf(workers.toSet))
    val networkLocationsByHost = workers.map(worker => worker.host -> worker.networkLocation).toMap
    response.getWorkersList.asScala.foreach { worker =>
      assert(worker.getNetworkLocation === networkLocationsByHost(worker.getHost))
    }
    assert(master.statusSystem.registeredAppAndShuffles.size() === registeredShufflesBefore)

    val defaultLimitResponse = requestWorkers(master, request.toBuilder.setMaxWorkers(0).build())
    assert(StatusCode.fromValue(defaultLimitResponse.getStatus) === StatusCode.SUCCESS)
    assert(defaultLimitResponse.getWorkersCount === 2)

    val singleWorkerResponse = requestWorkers(master, request.toBuilder.setMaxWorkers(1).build())
    assert(StatusCode.fromValue(singleWorkerResponse.getStatus) === StatusCode.SUCCESS)
    assert(singleWorkerResponse.getWorkersCount === 1)

    val replicatedResponse = requestWorkers(
      master,
      request.toBuilder
        .setMaxWorkers(1)
        .setShouldReplicate(true)
        .build())
    assert(StatusCode.fromValue(replicatedResponse.getStatus) === StatusCode.SUCCESS)
    assert(replicatedResponse.getWorkersCount === 2)

    val excludedWorkers = workers.take(3)
    val excludedResponse = requestWorkers(
      master,
      request.toBuilder
        .addAllExcludedWorkerSet(
          excludedWorkers.map(PbSerDeUtils.toPbWorkerInfo(_, true, true)).asJava)
        .build())
    assert(StatusCode.fromValue(excludedResponse.getStatus) === StatusCode.SUCCESS)
    assert(
      excludedResponse.getWorkersList.asScala.map(PbSerDeUtils.fromPbWorkerInfo).toSet ===
        workers.drop(3).toSet)

    val partialReplicaResponse = requestWorkers(
      master,
      request.toBuilder
        .setShouldReplicate(true)
        .addAllExcludedWorkerSet(
          excludedWorkers.map(PbSerDeUtils.toPbWorkerInfo(_, true, true)).asJava)
        .build())
    assert(
      StatusCode.fromValue(partialReplicaResponse.getStatus) ===
        StatusCode.SUCCESS)
    assert(partialReplicaResponse.getWorkersCount === 1)

    val taggedResponse =
      requestWorkers(master, request.toBuilder.setTagsExpr("missing-tag").build())
    assert(StatusCode.fromValue(taggedResponse.getStatus) === StatusCode.WORKER_EXCLUDED)
    assert(taggedResponse.getWorkersCount === 0)

    master.rpcEnv.shutdown()
  }

  test("handleRequestWorkers applies storage eligibility") {
    val conf = new CelebornConf()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, selectRandomPort().toString)
    val masterArgs = new MasterArguments(
      Array("-h", "localhost", "-p", selectRandomPort().toString),
      conf)
    val master = new Master(conf, masterArgs)

    def workerWithDisk(host: String, port: Int): WorkerInfo = {
      val disk = new DiskInfo("/disk", 1024L, 0L, 0L, 0L, StorageInfo.Type.HDD)
      val disks = new util.HashMap[String, DiskInfo]()
      disks.put(disk.mountPoint, disk)
      new WorkerInfo(
        host,
        port,
        port + 1,
        port + 2,
        port + 3,
        port + 4,
        disks,
        new util.HashMap[UserIdentifier, ResourceConsumption]())
    }

    val diskWorker = workerWithDisk("disk", 1000)
    val disklessWorker = new WorkerInfo("diskless", 2000, 2001, 2002, 2003)
    val workers = Seq(diskWorker, disklessWorker)
    master.statusSystem.availableWorkers.addAll(workers.asJava)

    val request = PbRequestWorkers.newBuilder()
      .setApplicationId("app1")
      .setUserIdentifier(
        PbSerDeUtils.toPbUserIdentifier(new UserIdentifier("tenant", "user")))
      .setMaxWorkers(10)
      .build()

    val localResponse = requestWorkers(
      master,
      request.toBuilder
        .setAvailableStorageTypes(StorageInfo.LOCAL_DISK_MASK)
        .build())
    assert(StatusCode.fromValue(localResponse.getStatus) === StatusCode.SUCCESS)
    assert(localResponse.getWorkersList.asScala.map(_.getHost).toSet === Set(diskWorker.host))

    val noLocalDiskResponse = requestWorkers(
      master,
      request.toBuilder
        .setAvailableStorageTypes(StorageInfo.LOCAL_DISK_MASK)
        .addExcludedWorkerSet(PbSerDeUtils.toPbWorkerInfo(diskWorker, true, true))
        .build())
    assert(StatusCode.fromValue(noLocalDiskResponse.getStatus) === StatusCode.SLOT_NOT_AVAILABLE)
    assert(noLocalDiskResponse.getWorkersCount === 0)

    val remoteResponse = requestWorkers(
      master,
      request.toBuilder
        .setAvailableStorageTypes(StorageInfo.HDFS_MASK)
        .build())
    assert(StatusCode.fromValue(remoteResponse.getStatus) === StatusCode.SUCCESS)
    assert(remoteResponse.getWorkersList.asScala.map(_.getHost).toSet === workers.map(_.host).toSet)

    val mixedResponse = requestWorkers(
      master,
      request.toBuilder
        .setAvailableStorageTypes(StorageInfo.MEMORY_MASK | StorageInfo.LOCAL_DISK_MASK)
        .build())
    assert(StatusCode.fromValue(mixedResponse.getStatus) === StatusCode.SUCCESS)
    assert(mixedResponse.getWorkersList.asScala.map(_.getHost).toSet === Set(diskWorker.host))

    master.rpcEnv.shutdown()
  }

  test("PbApplicationMetaRequest rejects a caller requesting another application's secret") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)
    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)

    val request = PbApplicationMetaRequest.newBuilder().setAppId("victim-app").build()
    val unhandled = (_: Any) => fail("PbApplicationMetaRequest was not handled")

    try {
      // An application authenticated as "attacker-app" on the external port must not
      // be able to read "victim-app"'s secret.
      val e = intercept[IllegalStateException] {
        master.receiveAndReply(contextForClient("attacker-app")).applyOrElse(request, unhandled)
      }
      assert(e.getMessage.contains("not authorized for application victim-app"))

      // A worker carries no client id, so the guard is a no-op and the request is served.
      master.receiveAndReply(contextForClient(null)).applyOrElse(request, unhandled)
    } finally {
      master.rpcEnv.shutdown()
    }
  }

  test("PbReviseLostShuffles authorizes application metadata changes") {
    val conf = new CelebornConf()
    val randomMasterPort = selectRandomPort()
    val randomHttpPort = selectRandomPort()
    conf.set(CelebornConf.HA_ENABLED.key, "false")
    conf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    conf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)
    val masterArgs = new MasterArguments(args, conf)
    val master = new Master(conf, masterArgs)

    val victimApp = "revise-victim-app"
    val ownRequest = ReviseLostShuffles(
      victimApp,
      util.Arrays.asList[Integer](1),
      "own-request")
    val crossAppRequest = ReviseLostShuffles(
      victimApp,
      util.Arrays.asList[Integer](2),
      "cross-app-request")
    val authDisabledApp = "auth-disabled-app"
    val authDisabledRequest = ReviseLostShuffles(
      authDisabledApp,
      util.Arrays.asList[Integer](3),
      "auth-disabled-request")
    val unhandled = (_: Any) => fail("PbReviseLostShuffles was not handled")

    try {
      master.receiveAndReply(contextForClient(victimApp)).applyOrElse(ownRequest, unhandled)
      val victimShuffles = master.statusSystem.registeredAppAndShuffles.get(victimApp)
      assert(victimShuffles.size() == 1)
      assert(victimShuffles.contains(1))

      val e = intercept[IllegalStateException] {
        master.receiveAndReply(contextForClient("attacker-app"))
          .applyOrElse(crossAppRequest, unhandled)
      }
      assert(e.getMessage.contains(s"not authorized for application $victimApp"))
      assert(victimShuffles.size() == 1)
      assert(!victimShuffles.contains(2))

      // Authentication-disabled connections have no client id, so existing behavior is preserved.
      master.receiveAndReply(contextForClient(null)).applyOrElse(authDisabledRequest, unhandled)
      val authDisabledShuffles =
        master.statusSystem.registeredAppAndShuffles.get(authDisabledApp)
      assert(authDisabledShuffles.size() == 1)
      assert(authDisabledShuffles.contains(3))
    } finally {
      master.rpcEnv.shutdown()
    }
  }
}
