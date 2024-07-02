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

package org.apache.celeborn.server.common.http.api.v1

import java.net.URI
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.{MediaType, UriBuilder}

import org.apache.celeborn.server.common.http.HttpTestHelper
import org.apache.celeborn.server.common.http.v1.model.{ConfResponse, ThreadStackResponse}

abstract class ApiV1BaseResourceSuite extends HttpTestHelper {
  override protected lazy val baseUri: URI =
    UriBuilder.fromUri(s"http://${httpService.connectionUrl}/api/v1").build()

  test("conf resource") {
    var response = webTarget.path("conf").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(!response.readEntity(classOf[ConfResponse]).getConfigs.isEmpty)

    response = webTarget.path("conf/dynamic").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_SERVICE_UNAVAILABLE == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("Dynamic configuration is disabled."))
  }

  test("thread_dump") {
    val response = webTarget.path("thread_dump").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(!response.readEntity(classOf[ThreadStackResponse]).getThreadStacks.isEmpty)
  }
}
