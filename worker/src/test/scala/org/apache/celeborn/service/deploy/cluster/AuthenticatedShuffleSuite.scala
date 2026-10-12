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

package org.apache.celeborn.service.deploy.cluster

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.file.Files
import java.util.{Collections, UUID}
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.DiskFileInfo
import org.apache.celeborn.common.network.TransportContext
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.network.sasl.{SaslClientBootstrap, SaslCredentials}
import org.apache.celeborn.common.network.security.{AuthorizationRequest, AuthorizationTestBootstrap, SecurityOperation}
import org.apache.celeborn.common.network.server.BaseMessageHandler
import org.apache.celeborn.common.network.util.TransportConf
import org.apache.celeborn.common.protocol.{MessageType, PartitionLocation, PbApplicationMeta, PbApplicationMetaRequest, PbOpenStream}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature

class AuthenticatedShuffleSuite extends AnyFunSuite with MiniClusterFeature {
  private val plugin = classOf[AuthorizationTestBootstrap].getName

  test("native authentication supports replicated shuffle and protects push and fetch") {
    runShuffle(nativeAuth = true)
  }

  test("configured connection policies authorize application data and service replication") {
    runShuffle(nativeAuth = false)
  }

  private def runShuffle(nativeAuth: Boolean): Unit = {
    val app = if (nativeAuth) "application-auth-native" else "tenant/shuffle-auth"
    val serverId = s"authenticated-shuffle-${UUID.randomUUID()}"
    val baseConf = new CelebornConf(false)
      .set(CelebornConf.AUTH_ENABLED.key, nativeAuth.toString)
      .set(CelebornConf.INTERNAL_PORT_ENABLED.key, nativeAuth.toString)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set(CelebornConf.ACTIVE_STORAGE_TYPES.key, "HDD")
      .set(CelebornConf.WORKER_STORAGE_CREATE_FILE_POLICY.key, "HDD")
      .set(CelebornConf.WORKER_DISK_RESERVE_SIZE.key, "0")
      .set(CelebornConf.READ_LOCAL_SHUFFLE_FILE.key, "false")
      .set("celeborn.data.io.maxRetries", "1")
    if (!nativeAuth) {
      Seq("rpc_service", "rpc_app_lifecyclemanager", "rpc_app_client", "replicate")
        .foreach { module =>
          baseConf.set(s"celeborn.$module.client.bootstrap.classes", plugin)
          baseConf.set(s"celeborn.$module.server.bootstrap.classes", plugin)
        }
      baseConf.set("celeborn.data.client.bootstrap.classes", plugin)
      baseConf.set("celeborn.push.server.bootstrap.classes", plugin)
      baseConf.set("celeborn.fetch.server.bootstrap.classes", plugin)
      baseConf.set("celeborn.test.security.serverId", serverId)
    }
    val clusterConf = baseConf.clone().set("celeborn.test.security.token", "service")
    try {
      setupMiniClusterWithRandomPorts(
        clusterConf.getAll.toMap,
        clusterConf.getAll.toMap,
        workerNum = 2)
      val clientConf = baseConf.clone()
        .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:${masterInfo._1.conf.masterPort}")
        .set("celeborn.test.security.token", "application")
      val lifecycleManager = new LifecycleManager(app, clientConf)
      var client: ShuffleClientImpl = null
      try {
        client = new ShuffleClientImpl(app, clientConf, UserIdentifier("claimed", "user"))
        client.setupLifecycleManagerRef(
          lifecycleManager.rpcEnv.address.host,
          lifecycleManager.rpcEnv.address.port)
        val metadata =
          if (nativeAuth) {
            val value = lifecycleManager.self.askSync[PbApplicationMeta](
              PbApplicationMetaRequest.newBuilder().setAppId(app).build())
            assert(value.getSecret.nonEmpty, "LifecycleManager must generate the native secret")
            Some(value)
          } else {
            None
          }
        val location = client.getPartitionLocation(1, 1, 1).get(0)
        assert(location != null && location.hasPeer, "The shuffle must reserve a replica")
        assert(location.getFetchPort != location.getPeer.getFetchPort)
        val payload = Array.tabulate[Byte](32768)(i => (i % 127).toByte)
        client.pushData(1, 0, 0, 0, payload, 0, payload.length, 1, 1)
        assert(!client.getPushState(Utils.makeMapKey(1, 0, 0)).limitZeroInFlight())
        client.mergeData(1, 0, 0, 0, payload, 0, payload.length, 1, 1)
        client.mergeData(1, 0, 0, 0, payload, 0, payload.length, 1, 1)
        client.pushMergedData(1, 0, 0)
        client.mapperEnd(1, 0, 0, 1, 1)
        val metrics = new MetricsCallback {
          override def incBytesRead(bytes: Long): Unit = {}
          override def incReadTime(time: Long): Unit = {}
        }
        val input = client.readPartition(1, 0, 0, 0L, 0, Integer.MAX_VALUE, metrics)
        try {
          val output = new ByteArrayOutputStream()
          val buffer = new Array[Byte](4096)
          var size = input.read(buffer)
          while (size != -1) {
            output.write(buffer, 0, size)
            size = input.read(buffer)
          }
          assert(output.toByteArray.sameElements(payload ++ payload ++ payload))
        } finally input.close()

        val shuffleKey = Utils.makeShuffleKey(app, 1)
        val primary = storedFile(shuffleKey, location)
        val replica = storedFile(shuffleKey, location.getPeer)
        val primaryBytes = Files.readAllBytes(primary.getFile.toPath)
        assert(primaryBytes.nonEmpty)
        assert(primaryBytes.sameElements(Files.readAllBytes(replica.getFile.toPath)))
        if (nativeAuth) {
          workerInfos.keys.foreach { worker =>
            val (pushPort, fetchPort) = worker.getPushFetchServerPort
            Seq(pushPort, fetchPort).foreach { port =>
              checkNativeAuthentication(
                clientConf,
                app,
                worker.rpcEnv.address.host,
                port,
                metadata.get.getSecret)
            }
          }
        } else {
          assert(primary.getUserIdentifier == UserIdentifier("tenant", "alice"))
          assert(replica.getUserIdentifier == UserIdentifier("tenant", "alice"))
          assertDataDecisions(serverId, app)
          workerInfos.keys.foreach { worker =>
            val (pushPort, fetchPort) = worker.getPushFetchServerPort
            Seq(pushPort, fetchPort).foreach { port =>
              val error = intercept[Exception] {
                withPluginClient(
                  clientConf.clone().set("celeborn.test.security.token", "unknown"),
                  worker.rpcEnv.address.host,
                  port)(_ => ())
              }
              assertRejected(error, "Unknown test identity")
            }
          }
          withPluginClient(
            clientConf.clone().set("celeborn.test.security.token", "foreign"),
            location.getHost,
            location.getFetchPort) { connection =>
            val request = new TransportMessage(
              MessageType.OPEN_STREAM,
              PbOpenStream.newBuilder()
                .setShuffleKey(shuffleKey)
                .setFileName(location.getFileName)
                .setEndIndex(Integer.MAX_VALUE)
                .build().toByteArray)
            val error = intercept[IOException] {
              connection.sendRpcSync(request.toByteBuffer, 10000)
            }
            assertRejected(error, "Denied OPEN_STREAM")
          }
        }
      } finally {
        try {
          if (client != null) client.shutdown()
        } finally {
          // LifecycleManager.onStop awaits its dispatcher; match the asynchronous test teardown.
          lifecycleManager.rpcEnv.shutdown()
        }
      }
    } finally {
      try {
        if (masterInfo != null) shutdownMiniCluster()
      } finally {
        masterInfo = null
        AuthorizationTestBootstrap.clear(serverId)
      }
    }
  }

  private def assertRejected(error: Throwable, reason: String): Unit = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
    val details = causes.map(_.toString).mkString("\nCaused by: ")
    assert(
      !causes.exists(_.isInstanceOf[TimeoutException]) && !details.contains("TimeoutException"),
      s"Expected an explicit rejection, but encountered a timeout: $details")
    assert(
      causes.exists(cause => Option(cause.getMessage).exists(_.contains(reason))),
      s"Expected rejection containing '$reason', got: $details")
  }

  private def storedFile(shuffleKey: String, location: PartitionLocation): DiskFileInfo = {
    val worker = workerInfos.keys.find(_.workerInfo.fetchPort == location.getFetchPort).get
    val info = worker.storageManager.getFileInfo(shuffleKey, location.getFileName)
    assert(info.isInstanceOf[DiskFileInfo], s"Expected a disk file for $shuffleKey $location")
    info.asInstanceOf[DiskFileInfo]
  }

  private def assertDataDecisions(serverId: String, app: String): Unit = {
    val requests = AuthorizationTestBootstrap.requests(serverId).iterator().asScala.toSeq
    Seq(
      AuthorizationRequest.Scope.APPLICATION -> SecurityOperation.PUSH_DATA,
      AuthorizationRequest.Scope.SERVICE -> SecurityOperation.PUSH_DATA,
      AuthorizationRequest.Scope.APPLICATION -> SecurityOperation.PUSH_MERGED_DATA,
      AuthorizationRequest.Scope.SERVICE -> SecurityOperation.PUSH_MERGED_DATA,
      AuthorizationRequest.Scope.APPLICATION -> SecurityOperation.CHUNK_FETCH).foreach {
      case (scope, operation) =>
        assert(
          requests.exists(r =>
            r.getScope == scope && r.getOperation == operation && r.getApplicationId == app),
          s"Missing $scope $operation authorization for $app")
    }
  }

  private def withPluginClient(
      conf: CelebornConf,
      host: String,
      port: Int)(check: TransportClient => Unit): Unit = {
    val context = new TransportContext(new TransportConf("data", conf), new BaseMessageHandler())
    try {
      val factory = context.createClientFactory()
      try {
        val connection = factory.createClient(host, port)
        try check(connection)
        finally connection.close()
      } finally factory.close()
    } finally context.close()
  }

  private def checkNativeAuthentication(
      conf: CelebornConf,
      app: String,
      host: String,
      port: Int,
      secret: String): Unit = {
    val transportConf = new TransportConf("data", conf)
    def connect(password: String): Unit = {
      val context = new TransportContext(transportConf, new BaseMessageHandler())
      try {
        val factory = context.createClientFactory(Collections.singletonList(
          new SaslClientBootstrap(transportConf, app, new SaslCredentials(app, password))))
        try {
          val connection = factory.createClient(host, port)
          try assert(connection.getClientId == app)
          finally connection.close()
        } finally factory.close()
      } finally context.close()
    }
    connect(secret)
    val error = intercept[Exception] { connect("incorrect-secret") }
    assertRejected(error, "DIGEST-MD5: digest response format violation. Mismatched response.")
  }
}
