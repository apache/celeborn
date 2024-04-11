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

package org.apache.celeborn.service.deploy.worker.http.api

import javax.servlet.http.HttpServletResponse
import javax.ws.rs.core.MediaType

import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceSuite
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class ApiWorkerResourceSuite extends ApiBaseResourceSuite with MiniClusterFeature {
  private var worker: Worker = _
  override protected def httpService: HttpService = worker

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup celeborn mini cluster")
    val (_, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    worker = w.head
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    logInfo("all test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("listPartitionLocationInfo") {
    val response = webTarget.path("listPartitionLocationInfo").request(MediaType.TEXT_PLAIN).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("unavailablePeers") {
    val response = webTarget.path("unavailablePeers").request(MediaType.TEXT_PLAIN).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("isShutdown") {
    val response = webTarget.path("isShutdown").request(MediaType.TEXT_PLAIN).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("isRegistered") {
    val response = webTarget.path("isRegistered").request(MediaType.TEXT_PLAIN).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
  }

  test("isDecommissioning") {
    val response = webTarget.path("isDecommissioning").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }
}
