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

import com.google.common.io.Files

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.rest.v1.model.{AppDiskUsageSnapshotsResponse, ApplicationsResponse, ExcludeWorkerRequest, HandleResponse, HostnamesResponse, RemoveWorkersUnavailableInfoRequest, SendWorkerEventRequest, ShufflesResponse, WorkerEventsResponse, WorkerId, WorkersResponse}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.api.v1.ApiV1BaseResourceSuite
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}

class ApiV1MasterResourceSuite extends ApiV1BaseResourceSuite {
  private var master: Master = _

  override protected def httpService: HttpService = master

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    val randomMasterPort = Utils.selectRandomPort(1024, 65535)
    val randomHttpPort = randomMasterPort + 1
    celebornConf.set(CelebornConf.HA_ENABLED.key, "false")
    celebornConf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    celebornConf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    celebornConf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    celebornConf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, celebornConf)
    master = new Master(celebornConf, masterArgs)
    new Thread() {
      override def run(): Unit = {
        master.initialize()
      }
    }.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    master.rpcEnv.shutdown()
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

    response =
      webTarget.path("applications/top_disk_usages").request(MediaType.APPLICATION_JSON).get()
    assert(HttpServletResponse.SC_OK == response.getStatus)
    assert(response.readEntity(classOf[AppDiskUsageSnapshotsResponse]).getSnapshots.isEmpty)

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
      .eventType(SendWorkerEventRequest.EventTypeEnum.DECOMMISSION)
      .workers(Collections.singletonList(worker))
    response = webTarget.path("workers/events").request(MediaType.APPLICATION_JSON).post(
      Entity.entity(sendWorkerEventRequest, MediaType.APPLICATION_JSON))
    assert(HttpServletResponse.SC_BAD_REQUEST == response.getStatus)
    assert(response.readEntity(classOf[String]).contains(
      "None of the workers are known"))
  }
}
