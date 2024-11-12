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

package org.apache.celeborn.service.deploy.master.http.api

import javax.ws.rs.client.Entity
import javax.ws.rs.core.{Form, MediaType}

import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceSuite
import org.apache.celeborn.service.deploy.master.{Master, MasterClusterFeature}

class ApiMasterResourceSuite extends ApiBaseResourceSuite with MasterClusterFeature {
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

  test("masterGroupInfo") {
    val response = webTarget.path("masterGroupInfo").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("lostWorkers") {
    val response = webTarget.path("lostWorkers").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("excludedWorkers") {
    val response = webTarget.path("excludedWorkers").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("shutdownWorkers") {
    val response = webTarget.path("shutdownWorkers").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("hostnames") {
    val response = webTarget.path("hostnames").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("workerEventInfo") {
    val response = webTarget.path("workerEventInfo").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("exclude") {
    val form = new Form
    form.param("add", "worker1-ip:9095:9096:9097:9098")
    form.param("remove", "worker2-ip:9095:9096:9097:9098")
    val response = webTarget.path("exclude").request(MediaType.TEXT_PLAIN)
      .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    assert(200 == response.getStatus)
  }

  test("sendWorkerEvent") {
    val form = new Form
    form.param("type", "Decommission")
    form.param("workers", "worker1-ip:9095:9096:9097:9098,worker2-ip:9095:9096:9097:9098")
    val response =
      webTarget.path("sendWorkerEvent").request(MediaType.TEXT_PLAIN)
        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    assert(200 == response.getStatus)
  }

  test("decommissionWorkers") {
    val response = webTarget.path("decommissionWorkers").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }
}
