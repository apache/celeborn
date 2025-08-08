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

package org.apache.celeborn.service.deploy.master.http.api.v1

import java.util.Collections
import javax.servlet.http.HttpServletResponse
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import org.apache.celeborn.rest.v1.model.{ApplicationsResponse, ExcludeWorkerRequest, HandleResponse, HostnamesResponse, RemoveWorkersUnavailableInfoRequest, SendWorkerEventRequest, ShufflesResponse, TopologyResponse, WorkerEventsResponse, WorkerId, WorkersResponse}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.api.v1.ApiV1BaseResourceSuite
import org.apache.celeborn.service.deploy.master.{Master, MasterClusterFeature}

class ApiV1MasterResourceSuite extends ApiV1BaseResourceSuite with MasterClusterFeature {
  private var master: Master = _

  override protected def httpService: HttpService = master

  override def beforeAll(): Unit = {
    master = setupMasterWithRandomPort(celebornConf.getAll.toMap)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    shutdownMaster()
  }

  test("shuffle resource") {
    val response = webTarget.path("shuffles").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[ShufflesResponse]).getShuffleIds.isEmpty)
  }

  test("application resource") {
    var response = webTarget.path("applications").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[ApplicationsResponse]).getApplications.isEmpty)

    response = webTarget.path("applications/hostnames").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[HostnamesResponse]).getHostnames.isEmpty)
  }

  test("master resource") {
    val response = webTarget.path("masters").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_BAD_REQUEST == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("HA is not enabled"))
  }

  test("worker resource") {
    var response = webTarget.path("workers").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val workersResponse = response.readEntity(classOf[WorkersResponse])
    assert(workersResponse.getWorkers.isEmpty)
    assert(workersResponse.getLostWorkers.isEmpty)
    assert(workersResponse.getExcludedWorkers.isEmpty)
    assert(workersResponse.getShutdownWorkers.isEmpty)
    assert(workersResponse.getDecommissioningWorkers.isEmpty)
    assert(workersResponse.getTaggedWorkers.isEmpty)

    val worker = new WorkerId()
      .host("unknown.celeborn")
      .rpcPort(0)
      .pushPort(0)
      .fetchPort(0)
      .replicatePort(0)
    val excludeWorkerRequest =
      new ExcludeWorkerRequest()
        .add(Collections.singletonList(worker))
        .remove(Collections.singletonList(worker))
    response = webTarget.path("workers/exclude").request(MediaType.APPLICATION_JSON).post(
      Entity.entity(excludeWorkerRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[HandleResponse]).getMessage.contains(
      "Unknown workers Host:unknown.celeborn"))

    val removeWorkersUnavailableInfoRequest = new RemoveWorkersUnavailableInfoRequest()
      .workers(Collections.singletonList(worker))
    response =
      webTarget.path("workers/remove_unavailable").request(MediaType.APPLICATION_JSON).post(
        Entity.entity(removeWorkersUnavailableInfoRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_OK == response.getStatus)

    response = webTarget.path("workers/events").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[WorkerEventsResponse]).getWorkerEvents.isEmpty)

    val sendWorkerEventRequest = new SendWorkerEventRequest()
    response = webTarget.path("workers/events").request(MediaType.APPLICATION_JSON).post(
      Entity.entity(sendWorkerEventRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_BAD_REQUEST == response.getStatus)
    assert(
      response.readEntity(classOf[String]).contains("eventType(null) and workers([]) are required"))
    sendWorkerEventRequest.eventType(SendWorkerEventRequest.EventTypeEnum.NONE)
    response = webTarget.path("workers/events").request(MediaType.APPLICATION_JSON).post(
      Entity.entity(sendWorkerEventRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_BAD_REQUEST == response.getStatus)
    assert(
      response.readEntity(classOf[String]).contains("eventType(NONE) and workers([]) are required"))
    sendWorkerEventRequest.workers(Collections.singletonList(worker))
    response = webTarget.path("workers/events").request(MediaType.APPLICATION_JSON).post(
      Entity.entity(sendWorkerEventRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_BAD_REQUEST == response.getStatus)
    assert(response.readEntity(classOf[String]).contains("None of the workers are known"))

    response = webTarget.path("workers/topology").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    val topologyResponse = response.readEntity(classOf[TopologyResponse])
    assert(topologyResponse.getTopologies.isEmpty)
  }
}
