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

import org.apache.celeborn.common.meta.WorkerStatus
import org.apache.celeborn.common.protocol.PbWorkerStatus.State
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceSuite
import org.apache.celeborn.server.common.http.api.HealthCheckResponse
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

  test("health reports unavailable during the startup interval before initialization completes") {
    // `initialize()` starts the HTTP server and registers with the master before the push/fetch
    // handlers and the controller endpoint are set up. Clearing the flag reproduces that interval:
    // the worker is registered and in Normal state, but cannot serve traffic yet.
    assert(worker.registered.get())
    assert(worker.registeredInMasterView.get())
    assert(worker.workerStatusManager.getWorkerState() == State.Normal)
    worker.initialized.set(false)
    try {
      val response = webTarget.path("healthz").request(MediaType.APPLICATION_JSON).get()
      assert(HttpServletResponse.SC_SERVICE_UNAVAILABLE == response.getStatus)
      val health = response.readEntity(classOf[HealthCheckResponse])
      assert(!health.healthy)
      assert(health.reason.contains("still initializing"))
    } finally {
      worker.initialized.set(true)
    }
  }

  test("health reports unavailable when the worker is not registered") {
    worker.registered.set(false)
    try {
      val response = webTarget.path("healthz").request(MediaType.APPLICATION_JSON).get()
      assert(HttpServletResponse.SC_SERVICE_UNAVAILABLE == response.getStatus)
      val health = response.readEntity(classOf[HealthCheckResponse])
      assert(!health.healthy)
      assert(health.reason.contains("not registered"))
    } finally {
      worker.registered.set(true)
    }
  }

  test("health reports unavailable when the master no longer knows the worker") {
    worker.registeredInMasterView.set(false)
    try {
      val response = webTarget.path("healthz").request(MediaType.APPLICATION_JSON).get()
      assert(HttpServletResponse.SC_SERVICE_UNAVAILABLE == response.getStatus)
      val health = response.readEntity(classOf[HealthCheckResponse])
      assert(!health.healthy)
      assert(health.reason.contains("not registered"))
    } finally {
      worker.registeredInMasterView.set(true)
    }
  }

  test("health reports unavailable when the worker state is not Normal") {
    worker.workerStatusManager.transitionState(State.InDecommission)
    try {
      assert(worker.workerStatusManager.getWorkerState() == State.InDecommission)
      val response = webTarget.path("healthz").request(MediaType.APPLICATION_JSON).get()
      assert(HttpServletResponse.SC_SERVICE_UNAVAILABLE == response.getStatus)
      val health = response.readEntity(classOf[HealthCheckResponse])
      assert(!health.healthy)
      assert(health.reason.contains(State.InDecommission.toString))
    } finally {
      // InDecommission may only transition to Exit, so restore Normal directly.
      val normal = WorkerStatus.normalWorkerStatus()
      worker.workerStatusManager.currentWorkerStatus = normal
      worker.workerInfo.setWorkerStatus(normal)
    }
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
