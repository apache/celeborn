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

import java.nio.file.Files
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{Form, MediaType}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, ThreadUtils, Utils}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceSuite
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}

class ApiMasterResourceSuite extends ApiBaseResourceSuite {
  private var master: Master = _

  override protected def httpService: HttpService = master

  def getTmpDir(): String = {
    val tmpDir = Files.createTempDirectory(null).toFile
    tmpDir.deleteOnExit()
    tmpDir.getAbsolutePath
  }

  override def beforeAll(): Unit = {
    val randomMasterPort = Utils.selectRandomInt(1024, 65535)
    val randomHttpPort = randomMasterPort + 1
    celebornConf.set(CelebornConf.HA_ENABLED.key, "false")
    celebornConf.set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, getTmpDir())
    celebornConf.set(CelebornConf.WORKER_STORAGE_DIRS.key, getTmpDir())
    celebornConf.set(CelebornConf.MASTER_HTTP_HOST.key, "127.0.0.1")
    celebornConf.set(CelebornConf.MASTER_HTTP_PORT.key, randomHttpPort.toString)

    val args = Array("-h", "localhost", "-p", randomMasterPort.toString)

    val masterArgs = new MasterArguments(args, celebornConf)
    master = new Master(celebornConf, masterArgs)
    ThreadUtils.newThread(
      new Runnable {
        override def run(): Unit = {
          master.initialize()
        }
      },
      "master-init-thread").start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    master.rpcEnv.shutdown()
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
