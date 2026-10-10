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

package org.apache.celeborn.client

import java.util
import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.ApplicationMeta
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.network.security.{AuthorizationRequest, AuthorizationTestBootstrap}
import org.apache.celeborn.common.network.security.SecurityOperation._
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._

class LifecycleManagerSecuritySuite extends CelebornFunSuite {
  private val appId = "tenant/lifecycle"
  private val plugin = classOf[AuthorizationTestBootstrap].getName

  test("LifecycleManager authorizes all remote requests against its own application") {
    val serverId = UUID.randomUUID().toString
    val conf = new CelebornConf(false)
      .set("celeborn.auth.enabled", "false")
      .set("celeborn.metrics.enabled", "false")
      .set("celeborn.rpc.io.threads", "1")
      .set("celeborn.rpc.dispatcher.threads", "2")
      .set("celeborn.rpc.askTimeout", "5s")
      .set("celeborn.client.application.unregister.enabled", "false")
      .set(
        s"celeborn.${TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE}.server.bootstrap.classes",
        plugin)
      .set("celeborn.test.security.serverId", serverId)
    val master = RpcEnv.create(
      "lifecycle-security-master",
      "rpc_service",
      "localhost",
      0,
      conf,
      Role.MASTER,
      None)
    master.setupEndpoint(
      RpcNameConstants.MASTER_EP,
      new RpcEndpoint {
        override val rpcEnv: RpcEnv = master
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case _: RegisterApplicationInfo => context.reply(OneWayMessageResponse)
          case _: HeartbeatFromApplication =>
            context.reply(HeartbeatFromApplicationResponse(
              StatusCode.SUCCESS,
              util.Collections.emptyList(),
              util.Collections.emptyList(),
              util.Collections.emptyList(),
              util.Collections.emptyList(),
              CheckQuotaResponse(true, "")))
        }
      })
    conf.set(CelebornConf.MASTER_ENDPOINTS.key, master.address.toString)
    val errors = new LinkedBlockingQueue[Throwable]()
    val manager = new LifecycleManager(appId, conf) {
      override def onError(cause: Throwable): Unit = {
        if (cause.isInstanceOf[SecurityException]) errors.add(cause)
        else super.onError(cause)
      }
    }
    // Seed the real metadata response without introducing native registration into this policy test.
    val metaField = classOf[LifecycleManager].getDeclaredFields
      .find(_.getType == classOf[ApplicationMeta]).get
    metaField.setAccessible(true)
    metaField.set(manager, ApplicationMeta(appId, "lifecycle-secret"))
    val clients = ArrayBuffer.empty[RpcEnv]
    try {
      val application = connect(manager, conf, clients, "application")
      assert(
        application.askSync[PbApplicationMeta](metadata(appId)).getSecret == "lifecycle-secret")
      val foreign = connect(manager, conf, clients, "foreign")
      // The request claims the caller's allowed app, but the secret belongs to this LM's app.
      denied(foreign, metadata("foreign/claimed-application"), GET_APPLICATION_META)

      val deniedPeer = connect(manager, conf, clients, "denied")
      requests.foreach { case (operation, message) => denied(deniedPeer, message, operation) }
      assert(manager.getShuffleIdMapping.isEmpty)

      val observed = AuthorizationTestBootstrap.requests(serverId)
      val decisions = new util.ArrayList[AuthorizationRequest]()
      observed.drainTo(decisions)
      assert(!decisions.isEmpty)
      val iterator = decisions.iterator()
      while (iterator.hasNext) {
        assert(iterator.next().getApplicationId == appId)
      }

      // Finish prior ask handling before observing the two one-way rejections.
      application.askSync[PbApplicationMeta](metadata(appId))
      errors.clear()
      Seq(RemoveExpiredShuffle, StageEnd(1)).foreach { message =>
        application.send(message)
        val error = errors.poll(5, TimeUnit.SECONDS)
        assert(error != null && error.isInstanceOf[SecurityException])
        assert(error.getMessage.contains("local RPC messages"))
      }
      assert(!manager.commitManager.isStageEnd(1))
      manager.self.send(RemoveExpiredShuffle)
      manager.self.send(StageEnd(1))
      // This endpoint uses one business dispatcher thread, so the reply fences its local sends.
      assert(manager.self.askSync[PbApplicationMeta](metadata(appId)).getAppId == appId)
      assert(manager.commitManager.isStageEnd(1))
    } finally {
      clients.foreach(_.shutdown())
      clients.foreach(_.awaitTermination())
      // Follow the existing asynchronous LM shutdown pattern; onStop awaits its own dispatcher.
      manager.stop()
      master.shutdown()
      master.awaitTermination()
      AuthorizationTestBootstrap.clear(serverId)
    }
  }

  private def requests: Seq[(String, Any)] = Seq(
    REGISTER_SHUFFLE -> RegisterShuffle(1, 1, 1, SerdeVersion.V1),
    REGISTER_MAP_PARTITION_TASK -> PbRegisterMapPartitionTask.newBuilder().build(),
    REVIVE -> Revive(
      1,
      util.Collections.emptyList[Integer](),
      util.Collections.emptyList[ReviveRequest](),
      SerdeVersion.V1),
    PARTITION_SPLIT -> PbPartitionSplit.newBuilder().build(),
    MAPPER_END -> MapperEnd(
      1,
      0,
      0,
      1,
      0,
      util.Collections.emptyMap(),
      1,
      Array.emptyIntArray,
      Array.emptyLongArray,
      SerdeVersion.V1),
    READ_REDUCER_PARTITION_END -> PbReadReducerPartitionEnd.newBuilder()
      .setShuffleId(1).setPartitionId(0).setStartMaxIndex(0).setEndMapIndex(1)
      .setCrc32(0).setBytesWritten(0L).build(),
    GET_REDUCER_FILE_GROUP -> GetReducerFileGroup(1, false, SerdeVersion.V1),
    GET_STAGE_END -> PbGetStageEnd.newBuilder().setShuffleId(1).build(),
    GET_SHUFFLE_ID -> PbGetShuffleId.newBuilder().build(),
    REPORT_SHUFFLE_FETCH_FAILURE -> PbReportShuffleFetchFailure.newBuilder().build(),
    REPORT_BARRIER_STAGE_ATTEMPT_FAILURE -> PbReportBarrierStageAttemptFailure.newBuilder().build(),
    GET_APPLICATION_META -> metadata("foreign/claimed-application"))

  private def metadata(applicationId: String): PbApplicationMetaRequest =
    PbApplicationMetaRequest.newBuilder().setAppId(applicationId).build()

  private def denied(reference: RpcEndpointRef, request: Any, expected: String): Unit = {
    val failure = intercept[Exception] { reference.askSync[Any](request) }
    val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toSeq
    assert(
      causes.exists(cause => Option(cause.getMessage).exists(_.contains(expected))),
      s"Expected authorization rejection containing '$expected', got ${causes.mkString(" -> ")}")
    assert(!causes.exists(_.isInstanceOf[java.util.concurrent.TimeoutException]))
  }

  private def connect(
      manager: LifecycleManager,
      conf: CelebornConf,
      clients: ArrayBuffer[RpcEnv],
      token: String): RpcEndpointRef = {
    val clientConf = conf.clone()
      .set("celeborn.rpc_service.client.bootstrap.classes", plugin)
      .set("celeborn.test.security.token", token)
    val client = RpcEnv.create(
      s"lifecycle-policy-${clients.size}",
      "rpc_service",
      "localhost",
      0,
      clientConf,
      Role.CLIENT,
      None)
    clients += client
    client.setupEndpointRef(manager.rpcEnv.address, RpcNameConstants.LIFECYCLE_MANAGER_EP)
  }
}
