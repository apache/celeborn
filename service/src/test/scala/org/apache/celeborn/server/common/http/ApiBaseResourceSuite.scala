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

import javax.ws.rs.core.MediaType

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.TestHelper

abstract class ApiBaseResourceSuite extends HttpTestHelper {
  celebornConf.set(CelebornConf.METRICS_ENABLED.key, "true")
    .set(
      CelebornConf.METRICS_CONF.key,
      TestHelper.getResourceAsAbsolutePath("/metrics-api.properties"))

  test("ping") {
    val response = webTarget.path("ping").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]) == "pong")
  }

  test("conf") {
    val response = webTarget.path("conf").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("listDynamicConfigs") {
    val response = webTarget.path("listDynamicConfigs")
      .queryParam("LEVEL", "TENANT")
      .request(MediaType.TEXT_PLAIN)
      .get()
    assert(200 == response.getStatus)
  }

  test("workerInfo") {
    val response = webTarget.path("workerInfo").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("threadDump") {
    val response = webTarget.path("threadDump").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("shuffle") {
    val response = webTarget.path("shuffle").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("applications") {
    val response = webTarget.path("applications").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("listTopDiskUsedApps") {
    val response = webTarget.path("listTopDiskUsedApps").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("openapi.json") {
    val response = webTarget.path("openapi.json").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("/conf"))
  }

  test("swagger") {
    Seq("swagger", "docs", "help").foreach { path =>
      val response = webTarget.path(path).request(MediaType.TEXT_HTML).get()
      assert(200 == response.getStatus)
      assert(response.readEntity(classOf[String]).contains("swagger-ui"))
    }
  }

  test("metrics/prometheus") {
    val response = webTarget.path("metrics/prometheus").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("metrics_jvm_memory_heap_max_Value"))
  }

  test("metrics/json") {
    val response = webTarget.path("metrics/json").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("\"name\" : \"jvm.memory.heap.max\""))
  }
}
