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
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{MediaType, UriBuilder}

import scala.collection.JavaConverters._

import org.apache.celeborn.rest.v1.model.{ConfResponse, LoggerInfo, LoggerInfos, ThreadStackResponse}
import org.apache.celeborn.server.common.http.HttpTestHelper

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

  test("logger resource") {
    val loggerName = this.getClass.getName

    // set logger level to INFO as initial state
    val response = webTarget.path("loggers").request(MediaType.APPLICATION_JSON).post(Entity.entity(
      new LoggerInfo().name(loggerName).level("INFO"),
      MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response.getStatus)

    // check logger level is INFO
    val response1 = webTarget.path("loggers")
      .queryParam("name", loggerName)
      .request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val loggerInfo = response1.readEntity(classOf[LoggerInfos]).getLoggers.get(0)
    assert(loggerName == loggerInfo.getName)
    assert(loggerInfo.getLevel == "INFO")
    assert(log.isInfoEnabled)
    assert(!log.isDebugEnabled)

    // set logger level to DEBUG
    val response2 =
      webTarget.path("loggers").request(MediaType.APPLICATION_JSON).post(Entity.entity(
        new LoggerInfo().name(loggerName).level("DEBUG"),
        MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response2.getStatus)

    // check logger level is DEBUG
    val response3 = webTarget.path("loggers")
      .queryParam("name", loggerName)
      .request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val loggerInfo2 = response3.readEntity(classOf[LoggerInfos]).getLoggers.get(0)
    assert(loggerName == loggerInfo2.getName)
    assert(loggerInfo2.getLevel == "DEBUG")
    assert(log.isInfoEnabled)
    assert(log.isDebugEnabled)

    // check all loggers
    val response4 = webTarget.path("loggers").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response4.getStatus)
    val loggers = response4.readEntity(classOf[LoggerInfos]).getLoggers.asScala
    assert(loggers.exists(l => l.getName == loggerName && l.getLevel == "DEBUG"))
    // root logger
    assert(loggers.exists(l => l.getName == "" && l.getLevel == "INFO"))

    // update root logger level
    val response5 =
      webTarget.path("loggers").request(MediaType.APPLICATION_JSON).post(Entity.entity(
        new LoggerInfo().name("").level("DEBUG"),
        MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response5.getStatus)

    // check root logger level is DEBUG
    val response6 = webTarget.path("loggers")
      .queryParam("name", "")
      .request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response6.getStatus)
    val loggerInfo3 = response6.readEntity(classOf[LoggerInfos]).getLoggers.get(0)
    assert("" == loggerInfo3.getName)
    assert(loggerInfo3.getLevel == "DEBUG")

    // reset root logger level to INFO
    val response7 =
      webTarget.path("loggers").request(MediaType.APPLICATION_JSON).post(Entity.entity(
        new LoggerInfo().name("").level("INFO"),
        MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response7.getStatus)
  }

  test("thread_dump") {
    val response = webTarget.path("thread_dump").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val threadStacks = response.readEntity(classOf[ThreadStackResponse]).getThreadStacks.asScala
    assert(threadStacks.nonEmpty)
    assert(threadStacks.exists(_.getBlockedByThreadId == null))
    assert(threadStacks.exists(_.getLockName != null))
  }
}
