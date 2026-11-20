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
import java.nio.charset.StandardCharsets

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.common.util.CelebornExitKind
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.api.ApiRootResource

class LoggerResourceSuite extends AnyFunSuite with BeforeAndAfterAll {
  private val loggerName = "org.apache.celeborn.test.LoggerResourceSuite"
  private val logger = LogManager.getLogger(loggerName)
  private val originalLevel = logger.getLevel
  private var server: HttpServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val httpService = new HttpService {
      override def serviceName: String = "master"
      override def conf: CelebornConf = new CelebornConf()
      override def metricsSystem: MetricsSystem = null
      override def getWorkerInfo: String = ""
      override def getShuffleList: String = ""
      override def getApplicationList: String = ""
    }
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
    Configurator.setLevel(loggerName, originalLevel)
    if (server != null) {
      server.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    }
    super.afterAll()
  }

  private def setLevel(levelJson: String): Int = {
    val connection = new URL(s"http://${server.getServerUri}/api/v1/loggers")
      .openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setRequestProperty("Content-Type", "application/json")
      connection.setDoOutput(true)
      val body = s"""{"name":"$loggerName"$levelJson}""".getBytes(StandardCharsets.UTF_8)
      val output = connection.getOutputStream
      try output.write(body)
      finally output.close()
      connection.getResponseCode
    } finally {
      connection.disconnect()
    }
  }

  test("invalid or missing logger levels are rejected without changing the level") {
    Configurator.setLevel(loggerName, Level.INFO)
    Seq(",\"level\":\"INF0\"", ",\"level\":\"\"", "", ",\"level\":null")
      .foreach { level =>
        assert(setLevel(level) == 400)
        assert(logger.getLevel == Level.INFO)
      }
  }

  test("valid logger levels are applied") {
    Configurator.setLevel(loggerName, Level.INFO)
    assert(setLevel(",\"level\":\"debug\"") == 200)
    assert(logger.getLevel == Level.DEBUG)

    val customLevel = Level.forName("CELEBORN_TEST_CUSTOM", 450)
    assert(setLevel(",\"level\":\"CELEBORN_TEST_CUSTOM\"") == 200)
    assert(logger.getLevel == customLevel)
  }
}
