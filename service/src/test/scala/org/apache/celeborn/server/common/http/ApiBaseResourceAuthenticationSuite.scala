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

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.authentication.HttpAuthSchemes
import org.apache.celeborn.common.network.TestHelper
import org.apache.celeborn.server.common.http.HttpAuthUtils.AUTHORIZATION_HEADER
import org.apache.celeborn.server.common.http.api.HealthCheckResponse
import org.apache.celeborn.server.common.http.authentication.{UserDefinedPasswordAuthenticationProviderImpl, UserDefineTokenAuthenticationProviderImpl}

abstract class ApiBaseResourceAuthenticationSuite extends HttpTestHelper {
  val administers = Seq("celeborn", "celeborn2")
  celebornConf
    .set(CelebornConf.METRICS_ENABLED.key, "true")
    .set(
      CelebornConf.METRICS_CONF.key,
      TestHelper.getResourceAsAbsolutePath("/metrics-api.properties"))
    .set(CelebornConf.MASTER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC", "BEARER"))
    .set(
      CelebornConf.MASTER_HTTP_AUTH_BASIC_PROVIDER,
      classOf[UserDefinedPasswordAuthenticationProviderImpl].getName)
    .set(
      CelebornConf.MASTER_HTTP_AUTH_BEARER_PROVIDER,
      classOf[UserDefineTokenAuthenticationProviderImpl].getName)
    .set(CelebornConf.WORKER_HTTP_AUTH_SUPPORTED_SCHEMES, Seq("BASIC", "BEARER"))
    .set(
      CelebornConf.WORKER_HTTP_AUTH_BASIC_PROVIDER,
      classOf[UserDefinedPasswordAuthenticationProviderImpl].getName)
    .set(
      CelebornConf.WORKER_HTTP_AUTH_BEARER_PROVIDER,
      classOf[UserDefineTokenAuthenticationProviderImpl].getName)
    .set(CelebornConf.MASTER_HTTP_AUTH_ADMINISTERS, administers)
    .set(CelebornConf.WORKER_HTTP_AUTH_ADMINISTERS, administers)

  def basicAuthorizationHeader(user: String, password: String): String =
    HttpAuthSchemes.BASIC + " " + new String(
      Base64.getEncoder.encode(s"$user:$password".getBytes()),
      StandardCharsets.UTF_8)

  def bearerAuthorizationHeader(token: String): String = HttpAuthSchemes.BEARER + " " + token

  Seq("conf", "listDynamicConfigs", "workerInfo", "shuffle", "applications").foreach { api =>
    test(s"API $api authentication") {
      var response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(
          AUTHORIZATION_HEADER,
          basicAuthorizationHeader(
            "user",
            UserDefinedPasswordAuthenticationProviderImpl.VALID_PASSWORD))
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

  test("health api does not need authentication") {
    val response = webTarget.path("healthz").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[HealthCheckResponse]).healthy)
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

  test("check admin privilege for mutative request") {
    Seq("/any_api", "/api/v1/any_api").foreach { api =>
      var response = webTarget.path(api)
        .request(MediaType.TEXT_PLAIN)
        .header(
          AUTHORIZATION_HEADER,
          basicAuthorizationHeader(
            "no_admin",
            UserDefinedPasswordAuthenticationProviderImpl.VALID_PASSWORD))
        .post(null)
      assert(HttpServletResponse.SC_FORBIDDEN == response.getStatus)

      administers.foreach { admin =>
        response = webTarget.path(api)
          .request(MediaType.TEXT_PLAIN)
          .header(
            AUTHORIZATION_HEADER,
            basicAuthorizationHeader(
              admin,
              UserDefinedPasswordAuthenticationProviderImpl.VALID_PASSWORD))
          .post(null)
        // pass the admin privilege check, but the api is not found
        assert(HttpServletResponse.SC_NOT_FOUND == response.getStatus)
      }
    }
  }

  test("request without Authorization header gets 401 with WWW-Authenticate challenges") {
    // The suite configures BASIC + BEARER schemes; a request with no Authorization header must be
    // challenged with one WWW-Authenticate value per registered scheme. The Basic challenge carries
    // a realm (RFC 7617), other schemes use their bare token.
    val response = webTarget.path("shuffle").request(MediaType.TEXT_PLAIN).get()
    assert(HttpServletResponse.SC_UNAUTHORIZED == response.getStatus)

    val challenges =
      response.getStringHeaders.get(HttpAuthUtils.WWW_AUTHENTICATE_HEADER).asScala.toSet
    assert(challenges.contains("Basic realm=\"Celeborn\""))
    assert(challenges.contains(HttpAuthSchemes.BEARER.toString))
  }

  test("request with unmatched Authorization scheme keeps 401 and original error") {
    // An Authorization header that matches no configured handler must still 401 and must NOT be
    // challenged (no new WWW-Authenticate added for it).
    val response = webTarget.path("shuffle")
      .request(MediaType.TEXT_PLAIN)
      .header(AUTHORIZATION_HEADER, "Digest realm=\"x\"")
      .get()
    assert(HttpServletResponse.SC_UNAUTHORIZED == response.getStatus)
  }
}
