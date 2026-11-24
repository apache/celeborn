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

package org.apache.celeborn.service.deploy.master.scale

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.fabric8.kubernetes.client.{ConfigBuilder, KubernetesClient, KubernetesClientBuilder, KubernetesClientException}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.PbScaleType
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.server.common.service.config.{DynamicConfigServiceFactory, FsConfigServiceImpl}

class OpenKruiseScaleManagerSuite extends AnyFunSuite {

  private val ratioKeys = Seq(
    WorkerStats.MemoryPressureRatio,
    WorkerStats.LastMinuteSystemLoadRatio,
    WorkerStats.DiskUsedRatio)
  private val low = Seq.fill(10)(WorkerStats(ratioKeys.map(_ -> 0.1).toMap))

  test("uses the default three-dimensional count policy in the concrete manager") {
    val conf = new CelebornConf(false).set(SCALE_WORKER_NUMBER_MIN.key, "1")
    withManager(conf) { (manager, _) =>
      assert(manager.target(10, 10, low) == 9)
      assert(manager.target(20, 10, low) == 10)
      assert(manager.target(10, 10, low, 2, 1) == 9)
      assert(manager.target(10, 10, low, 6, 1) == 10)
      ratioKeys.foreach { key =>
        val high = Seq.fill(10)(WorkerStats(low.head.metrics + (key -> 0.9)))
        assert(manager.target(10, 10, high) == 15, key)
      }
    }
  }

  test("a conf-only configured manager uses the shared service and refreshed dynamic policy") {
    val file = Files.createTempFile("openkruise-scale-config", ".yaml")
    val conf = new CelebornConf(false)
      .set(SCALE_WORKER_NUMBER_MIN.key, "1")
      .set(DYNAMIC_CONFIG_STORE_BACKEND.key, classOf[FsConfigServiceImpl].getName)
      .set(DYNAMIC_CONFIG_STORE_FS_PATH.key, file.toString)
      .set(DYNAMIC_CONFIG_REFRESH_INTERVAL.key, "1d")
    def writeConfig(step: Int, percent: Double): Unit = {
      Files.write(
        file,
        ("- level: SYSTEM\n  config:\n" +
          s"    ${SCALE_DOWN_POLICY_STEP_NUMBER.key}: '$step'\n" +
          s"    ${SCALE_DOWN_POLICY_PERCENT.key}: '$percent'\n").getBytes(StandardCharsets.UTF_8))
    }
    try {
      DynamicConfigServiceFactory.reset()
      writeConfig(100, 1.0)
      withManager(conf) { (manager, _) =>
        assert(manager.target(10, 10, low) == 4)
        val sharedService = DynamicConfigServiceFactory.getConfigService(conf)
        writeConfig(2, 0.2)
        sharedService.refreshCache()
        assert(manager.target(10, 10, low) == 8)
        Files.write(file, "- level: SYSTEM\n  config: {}\n".getBytes(StandardCharsets.UTF_8))
        sharedService.refreshCache()
        assert(manager.target(10, 10, low) == 9)
      }
    } finally {
      DynamicConfigServiceFactory.reset()
      Files.deleteIfExists(file)
    }
  }

  test("patches replicas and reserved ordinals together and preserves other StatefulSet fields") {
    withManager() { (manager, api) =>
      manager.clusterInfo()
      val operation = ScaleOperation(3, Seq(1, 4), Seq.empty, Seq.empty, PbScaleType.SCALE_DOWN, 0L)
      assert(manager.scale(operation) == "8")
      assert(api.patches.size == 1)
      val patch = api.patches.head
      assert(patch.path("spec").path("replicas").asInt() == 3)
      assert(patch.path("spec").path("reserveOrdinals").toString == "[1,4]")
      assert(patch.path("metadata").path("resourceVersion").asText() == "70")
      assert(api.resource.path("spec").path("serviceName").asText() == "workers")
      assert(api.resource.path("spec").path("updateStrategy").path("type").asText() == "OnDelete")
      assert(api.lastPatchContentType.takeWhile(_ != ';').trim == "application/merge-patch+json")
      assert(manager.clusterInfo() == ClusterInfo(3, Seq(1, 4), false, "workers-uid", "71"))
      assert(
        manager.scale(
          ScaleOperation(6, Seq.empty, Seq.empty, Seq.empty, PbScaleType.SCALE_UP, 0L)) == "9")
      assert(api.patches.size == 2)
      assert(api.resource.path("spec").path("replicas").asInt() == 6)
      assert(api.resource.path("spec").path("reserveOrdinals").isEmpty)
    }
  }

  test(
    "rejects stale scale plans and can submit a new plan after reading the changed StatefulSet") {
    withManager() { (manager, api) =>
      assert(manager.clusterInfo() == ClusterInfo(5, Seq(1), true, "workers-uid", "70"))
      val metadata = api.resource.get("metadata").asInstanceOf[ObjectNode]
      metadata.put("resourceVersion", "71").put("generation", 8)
      val spec = api.resource.get("spec").asInstanceOf[ObjectNode]
      spec.put("replicas", 7).putArray("reserveOrdinals").add(4)
      api.setStatus(8, 7, 7)

      val stale = ScaleOperation(6, Seq.empty, Seq.empty, Seq.empty, PbScaleType.SCALE_UP, 0L)
      val error = intercept[KubernetesClientException](manager.scale(stale))
      assert(error.getCode == 409)
      assert(manager.rejectedUpdate(error))
      assert(api.patches.head.path("metadata").path("resourceVersion").asText() == "70")
      assert(manager.clusterInfo() == ClusterInfo(7, Seq(4), true, "workers-uid", "71"))

      val fresh = stale.copy(specWorkerCnt = 8, reservedWorkerIndices = Seq(4))
      assert(manager.scale(fresh) == "9")
      assert(api.patches.last.path("metadata").path("resourceVersion").asText() == "71")
      assert(manager.clusterInfo() == ClusterInfo(8, Seq(4), false, "workers-uid", "72"))
    }
  }

  test("a same-name StatefulSet replacement has a new workload identity and rejects the old plan") {
    withManager() { (manager, api) =>
      val original = manager.clusterInfo()
      val metadata = api.resource.get("metadata").asInstanceOf[ObjectNode]
      metadata.put("uid", "replacement-uid").put("resourceVersion", "88")
      val operation = ScaleOperation(6, Seq(1), Seq.empty, Seq.empty, PbScaleType.SCALE_UP, 0L)

      val error = intercept[KubernetesClientException](manager.scale(operation))
      assert(error.getCode == 409)
      assert(api.patches.head.path("metadata").path("resourceVersion").asText() == "70")
      assert(manager.clusterInfo() ==
        original.copy(workloadUid = "replacement-uid", platformVersion = "88"))
    }
  }

  test("waits for observed generation and replica convergence without requiring ready replicas") {
    withManager() { (manager, api) =>
      manager.clusterInfo()
      val operationId =
        manager.scale(ScaleOperation(
          3,
          Seq(1, 4),
          Seq.empty,
          Seq.empty,
          PbScaleType.SCALE_DOWN,
          0L))
      api.setStatus(observedGeneration = 7, replicas = 3, readyReplicas = 3)
      assert(manager.operationResult(operationId) == OperationResult(false, false))
      api.setStatus(observedGeneration = 8, replicas = 5, readyReplicas = 3)
      assert(manager.operationResult(operationId) == OperationResult(false, false))
      api.setStatus(observedGeneration = 8, replicas = 3, readyReplicas = 1)
      assert(manager.operationResult(operationId) == OperationResult(true, true))
      assert(manager.clusterInfo().isInRunningStatus)
    }
  }

  test("a patch response must confirm the requested worker spec") {
    withManager() { (manager, api) =>
      manager.clusterInfo()
      api.ignoreNextPatch = true
      val operation = ScaleOperation(6, Seq(1), Seq.empty, Seq.empty, PbScaleType.SCALE_UP, 0L)
      val failure = intercept[IllegalStateException](manager.scale(operation))
      assert(failure.getMessage.contains("requested worker spec"))
      assert(!manager.rejectedUpdate(failure))
      assert(manager.clusterInfo() == ClusterInfo(5, Seq(1), true, "workers-uid", "70"))
      assert(manager.scale(operation) == "8")
    }
  }

  test("a failed pod creation condition remains pending and can converge after reconciliation") {
    withManager() { (manager, api) =>
      manager.clusterInfo()
      val operationId =
        manager.scale(ScaleOperation(8, Seq.empty, Seq.empty, Seq.empty, PbScaleType.SCALE_UP, 0L))
      api.setStatus(observedGeneration = 8, replicas = 5, readyReplicas = 5)
      val status = api.resource.path("status").asInstanceOf[ObjectNode]
      status.putArray("conditions").addObject()
        .put("type", "FailedCreatePod").put("status", "True")
      assert(manager.operationResult(operationId) == OperationResult(false, false))
      status.remove("conditions")
      api.setStatus(observedGeneration = 8, replicas = 8, readyReplicas = 5)
      assert(manager.operationResult(operationId) == OperationResult(true, true))
    }
  }

  test("propagates an API rejection and reads the cluster after access is restored") {
    withManager() { (manager, api) =>
      api.rejectNextGet = true
      val failure = intercept[KubernetesClientException](manager.clusterInfo())
      assert(failure.getCode == 403)
      assert(manager.rejectedUpdate(failure))
      Seq(408, 429, 500, 503).foreach { status =>
        assert(!manager.rejectedUpdate(new KubernetesClientException(
          "update failed",
          status,
          null)))
      }
      assert(manager.clusterInfo() == ClusterInfo(5, Seq(1), true, "workers-uid", "70"))
    }
  }

  private def withManager(conf: CelebornConf = new CelebornConf(false))(
      testBody: (ConfiguredOpenKruiseScaleManager, KubernetesApi) => Unit): Unit = {
    val api = new KubernetesApi
    api.server.start()
    conf
      .set(CelebornConf.SCALE_KUBERNETES_NAMESPACE.key, "test")
      .set(CelebornConf.SCALE_KUBERNETES_WORKER_STATEFULSET.key, "workers")
      .set("celeborn.test.kubernetes.api", s"http://127.0.0.1:${api.server.getAddress.getPort}/")
    val manager = Utils.instantiateClassWithCelebornConf[ConfiguredOpenKruiseScaleManager](
      classOf[ConfiguredOpenKruiseScaleManager].getName,
      conf)
    try {
      testBody(manager, api)
    } finally {
      manager.stop()
      api.server.stop(0)
    }
  }

  private class KubernetesApi {
    private val mapper = new ObjectMapper
    private val statefulSetPath =
      "/apis/apps.kruise.io/v1beta1/namespaces/test/statefulsets/workers"
    val patches = new ArrayBuffer[JsonNode]
    var lastPatchContentType: String = _
    var rejectNextGet = false
    var ignoreNextPatch = false
    val resource = mapper.readTree("""{
      "apiVersion":"apps.kruise.io/v1beta1","kind":"StatefulSet",
      "metadata":{"name":"workers","namespace":"test","uid":"workers-uid","generation":7,"resourceVersion":"70"},
      "spec":{"replicas":5,"reserveOrdinals":[1],"serviceName":"workers","updateStrategy":{"type":"OnDelete"}},
      "status":{"observedGeneration":7,"replicas":5,"readyReplicas":1}
    }""").asInstanceOf[ObjectNode]
    val server: HttpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = KubernetesApi.this.synchronized {
          (exchange.getRequestMethod, exchange.getRequestURI.getPath) match {
            case ("GET", `statefulSetPath`) if rejectNextGet =>
              rejectNextGet = false
              val body =
                mapper.readTree("""{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","message":"test access denied","code":403}""")
              val bytes = body.toString.getBytes(StandardCharsets.UTF_8)
              exchange.getResponseHeaders.set("Content-Type", "application/json")
              exchange.sendResponseHeaders(403, bytes.length)
              try exchange.getResponseBody.write(bytes)
              finally exchange.close()
            case ("GET", `statefulSetPath`) => respond(exchange, resource)
            case ("PATCH", `statefulSetPath`) =>
              val patch = mapper.readTree(exchange.getRequestBody)
              patches += patch
              lastPatchContentType = exchange.getRequestHeaders.getFirst("Content-Type")
              val metadata = resource.get("metadata").asInstanceOf[ObjectNode]
              val version = patch.path("metadata").path("resourceVersion")
              if (!version.isMissingNode && version != metadata.get("resourceVersion")) {
                val conflict =
                  mapper.readTree("""{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","message":"stale resource version","code":409}""")
                respond(exchange, conflict, 409)
              } else {
                if (ignoreNextPatch) {
                  ignoreNextPatch = false
                } else {
                  val spec = resource.get("spec").asInstanceOf[ObjectNode]
                  spec.set[JsonNode]("replicas", patch.path("spec").get("replicas"))
                  spec.set[JsonNode]("reserveOrdinals", patch.path("spec").get("reserveOrdinals"))
                  metadata.put("generation", metadata.path("generation").asLong() + 1)
                  metadata.put(
                    "resourceVersion",
                    (metadata.path("resourceVersion").asLong() + 1).toString)
                }
                respond(exchange, resource)
              }
            case _ =>
              exchange.sendResponseHeaders(404, -1)
              exchange.close()
          }
        }
      })

    def setStatus(observedGeneration: Long, replicas: Int, readyReplicas: Int): Unit =
      synchronized {
        val status = resource.get("status").asInstanceOf[ObjectNode]
        status.put("observedGeneration", observedGeneration)
        status.put("replicas", replicas)
        status.put("readyReplicas", readyReplicas)
      }

    private def respond(exchange: HttpExchange, value: JsonNode, code: Int = 200): Unit = {
      val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.set("Content-Type", "application/json")
      exchange.sendResponseHeaders(code, bytes.length)
      val output = exchange.getResponseBody
      try {
        output.write(bytes)
      } finally {
        output.close()
        exchange.close()
      }
    }
  }
}

class ConfiguredOpenKruiseScaleManager(conf: CelebornConf) extends OpenKruiseScaleManager(conf) {

  override protected def createClient(): KubernetesClient = {
    new KubernetesClientBuilder().withConfig(new ConfigBuilder()
      .withMasterUrl(conf.get("celeborn.test.kubernetes.api"))
      .withNamespace("test")
      .withAutoConfigure(false)
      .withHttp2Disable(true)
      .build()).build()
  }

  def target(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      exclWorkerCnt: Int = 0,
      unaccountedExclWorkerCnt: Int = 0): Int = {
    calculateScaleTarget(
      registeredWorkerCnt,
      specWorkerCnt,
      workerStats,
      exclWorkerCnt,
      unaccountedExclWorkerCnt).targetSpecWorkerCnt
  }

  def clusterInfo(): ClusterInfo = getCurrentClusterInfo()

  def scale(operation: ScaleOperation): String = doScaleOperation(operation)

  def operationResult(operationId: String): OperationResult = checkOperation(operationId)

  def rejectedUpdate(error: Exception): Boolean = isRejectedPlatformUpdate(error)
}
