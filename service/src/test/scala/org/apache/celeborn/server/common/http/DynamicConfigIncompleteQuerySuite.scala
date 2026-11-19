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
import org.apache.celeborn.server.common.service.config.{ConfigService, SystemConfig, TenantConfig}

class DynamicConfigIncompleteQuerySuite extends AnyFunSuite with BeforeAndAfterAll {
  private val configService = mock(classOf[ConfigService])
  private val httpService = new HttpService {
    override def serviceName: String = "master"
    override def conf: CelebornConf = new CelebornConf()
    override def metricsSystem: MetricsSystem = null
    override def configService: ConfigService = DynamicConfigIncompleteQuerySuite.this.configService
    override def getWorkerInfo: String = ""
    override def getShuffleList: String = ""
    override def getApplicationList: String = ""
  }
  private val mapper = new ObjectMapper()
  private var server: HttpServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(configService.getSystemConfigFromCache).thenReturn(new SystemConfig(new CelebornConf()))
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

  test("tenant user queries with one missing parameter return no configs") {
    Seq(
      "level=TENANT_USER&tenant=tenantA",
      "level=TENANT_USER&name=alice",
      "level=TENANT_USER&tenant=tenantA&name=",
      "level=TENANT_USER&tenant=&name=alice").foreach { query =>
      val (status, body) = get(s"api/v1/conf/dynamic?$query")
      assert(status == 200, body)
      assert(mapper.readTree(body).path("configs").size() == 0)
    }
  }

  test("queries across all levels retain system and tenant results with only a tenant") {
    val (status, body) = get("api/v1/conf/dynamic?tenant=tenantA")
    assert(status == 200, body)
    val configs = mapper.readTree(body).path("configs")
    assert(configs.size() == 2)
    assert(configs.get(0).path("level").asText() == "SYSTEM")
    assert(configs.get(1).path("level").asText() == "TENANT")
    assert(configs.get(1).path("configs").get(0).path("name").asText() == "tenant.key")
  }

  test("tenant user queries with no parameters or both parameters still work") {
    Seq("level=TENANT_USER", "level=TENANT_USER&tenant=tenantA&name=alice")
      .foreach { query =>
        val (status, body) = get(s"api/v1/conf/dynamic?$query")
        assert(status == 200, body)
        val configs = mapper.readTree(body).path("configs")
        assert(configs.size() == 1)
        assert(configs.get(0).path("configs").get(0).path("name").asText() == "user.key")
      }
  }
}
