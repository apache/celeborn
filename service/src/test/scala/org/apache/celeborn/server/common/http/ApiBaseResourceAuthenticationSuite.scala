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

import java.nio.charset.StandardCharsets
import java.util.Base64
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.HttpAuthSchemes
import org.apache.celeborn.server.common.http.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.celeborn.server.common.http.authentication.{UserDefinePasswordAuthenticationProviderImpl, UserDefineTokenAuthenticationProviderImpl}

abstract class ApiBaseResourceAuthenticationSuite extends HttpTestHelper {
  celebornConf
    .set(CelebornConf.METRICS_ENABLED.key, "true")
    .set(
      CelebornConf.METRICS_CONF.key,
      Thread.currentThread().getContextClassLoader.getResource("metrics-api.properties").getFile)
    .set(CelebornConf.MASTER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC", "BEARER"))
    .set(
      CelebornConf.MASTER_HTTP_AUTH_BASIC_PROVIDER,
      classOf[UserDefinePasswordAuthenticationProviderImpl].getName)
    .set(
      CelebornConf.MASTER_HTTP_AUTH_BEARER_PROVIDER,
      classOf[UserDefineTokenAuthenticationProviderImpl].getName)
    .set(CelebornConf.WORKER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC", "BEARER"))
    .set(
      CelebornConf.WORKER_HTTP_AUTH_BASIC_PROVIDER_CLASS,
      classOf[UserDefinePasswordAuthenticationProviderImpl].getName)
    .set(
      CelebornConf.WORKER_HTTP_AUTH_BEARER_PROVIDER_CLASS,
      classOf[UserDefineTokenAuthenticationProviderImpl].getName)

  def basicAuthorizationHeader(user: String, password: String): String =
    HttpAuthSchemes.BASIC + " " + new String(
      Base64.getEncoder.encode(s"$user:$password".getBytes()),
      StandardCharsets.UTF_8)

  def bearerAuthorizationHeader(token: String): String =
    HttpAuthSchemes.BEARER + " " + new String(
      Base64.getEncoder.encode(token.getBytes()),
      StandardCharsets.UTF_8)

  Seq("conf", "listDynamicConfigs", "workerInfo", "shuffle", "applications").foreach { api =>
    test(s"API $api authentication") {
      var response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(
          AUTHORIZATION_HEADER,
          basicAuthorizationHeader(
            "user",
            UserDefinePasswordAuthenticationProviderImpl.VALID_PASSWORD))
        .get()
      assert(HttpServletResponse.SC_OK == response.getStatus)

      response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("user", "invalid"))
        .get()
      assert(HttpServletResponse.SC_FORBIDDEN == response.getStatus)

      response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(
          AUTHORIZATION_HEADER,
          bearerAuthorizationHeader(UserDefineTokenAuthenticationProviderImpl.VALID_TOKEN))
        .get()
      assert(HttpServletResponse.SC_OK == response.getStatus)

      response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(AUTHORIZATION_HEADER, bearerAuthorizationHeader("bad_token"))
        .get()
      assert(HttpServletResponse.SC_FORBIDDEN == response.getStatus)
    }
  }

  test("swagger api do not need authentication") {
    Seq("swagger", "docs", "help").foreach { path =>
      val response = webTarget.path(path).request(MediaType.TEXT_HTML).get()
      assert(HttpServletResponse.SC_OK == response.getStatus)
      assert(response.readEntity(classOf[String]).contains("swagger-ui"))
    }
    Seq(
      "openapi.json" -> MediaType.APPLICATION_JSON,
      "openapi.yaml" -> "application/yaml").foreach { case (path, mediaType) =>
      val response = webTarget.path(path).request(mediaType).get()
      assert(HttpServletResponse.SC_OK == response.getStatus)
      assert(response.readEntity(classOf[String]).contains("/conf"))
    }
  }

  test("metrics api do not need authentication") {
    var response = webTarget.path("metrics/prometheus").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("metrics_jvm_memory_heap_max_Value"))

    response = webTarget.path("metrics/json").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("\"name\" : \"jvm.memory.heap.max\""))
  }
}
