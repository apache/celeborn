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

package org.apache.celeborn.server.common.http

import java.net.{HttpURLConnection, URL}

import scala.collection.JavaConverters._
import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.util.CelebornExitKind
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.api.ApiRootResource
import org.apache.celeborn.server.common.service.config.{ConfigService, TenantConfig}

class DynamicConfigQuerySuite extends AnyFunSuite with BeforeAndAfterAll {
  private val configService = mock(classOf[ConfigService])
  private val httpService = new HttpService {
    override def serviceName: String = "master"
    override def conf: CelebornConf = new CelebornConf()
    override def metricsSystem: MetricsSystem = null
    override def configService: ConfigService = DynamicConfigQuerySuite.this.configService
    override def getWorkerInfo: String = ""
    override def getShuffleList: String = ""
    override def getApplicationList: String = ""
  }
  private val mapper = new ObjectMapper()
  private var server: HttpServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tenantConfig = new TenantConfig(
      configService,
      "tenantA",
      null,
      Map("tenant.key" -> "tenant.value").asJava)
    val userConfig = new TenantConfig(
      configService,
      "tenantA",
      "alice",
      Map("user.key" -> "user.value").asJava)
    when(configService.getRawTenantConfigFromCache("tenantA")).thenReturn(tenantConfig)
    when(configService.getRawTenantUserConfigFromCache("tenantA", "alice"))
      .thenReturn(userConfig)
    when(configService.listRawTenantConfigsFromCache()).thenReturn(List(tenantConfig).asJava)
    when(configService.listRawTenantUserConfigsFromCache()).thenReturn(List(userConfig).asJava)
    server = HttpServer(
      "test",
      "127.0.0.1",
      0,
      16,
      1000,
      10000,
      false,
      None,
      None,
      None,
      None,
      Seq.empty,
      Seq.empty)
    server.addHandler(ApiRootResource.getServletHandler(httpService))
    server.start()
  }

  override def afterAll(): Unit = {
    if (server != null) {
      server.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    }
    super.afterAll()
  }

  private def get(path: String): (Int, String) = {
    val connection = new URL(s"http://${server.getServerUri}/$path")
      .openConnection().asInstanceOf[HttpURLConnection]
    try {
      val status = connection.getResponseCode
      val stream = if (status < 400) connection.getInputStream else connection.getErrorStream
      val source = Source.fromInputStream(stream, "UTF-8")
      try {
        (status, source.mkString)
      } finally {
        source.close()
      }
    } finally {
      connection.disconnect()
    }
  }

  test("dynamic configuration queries return empty results for missing entries") {
    Seq(
      "level=TENANT&tenant=missing",
      "level=TENANT_USER&tenant=tenantA&name=missing").foreach { query =>
      val (status, body) = get(s"api/v1/conf/dynamic?$query")
      assert(status == 200, body)
      assert(mapper.readTree(body).path("configs").isArray)
      assert(mapper.readTree(body).path("configs").size() == 0)

      val (legacyStatus, legacyBody) = get(s"listDynamicConfigs?$query")
      assert(legacyStatus == 200, legacyBody)
      assert(!legacyBody.contains("Tenant:"))
    }
  }

  test("dynamic configuration queries still return existing and listed entries") {
    Seq(
      ("level=TENANT&tenant=tenantA", "tenant.key", "tenant.value"),
      ("level=TENANT_USER&tenant=tenantA&name=alice", "user.key", "user.value"),
      ("level=TENANT", "tenant.key", "tenant.value"),
      ("level=TENANT_USER", "user.key", "user.value")).foreach { case (query, key, value) =>
      val (status, body) = get(s"api/v1/conf/dynamic?$query")
      assert(status == 200, body)
      val configs = mapper.readTree(body).path("configs")
      assert(configs.size() == 1)
      assert(configs.get(0).path("configs").get(0).path("name").asText() == key)
      assert(configs.get(0).path("configs").get(0).path("value").asText() == value)

      val (legacyStatus, legacyBody) = get(s"listDynamicConfigs?$query")
      assert(legacyStatus == 200, legacyBody)
      assert(legacyBody.contains(key) && legacyBody.contains(value))
    }
  }
}
