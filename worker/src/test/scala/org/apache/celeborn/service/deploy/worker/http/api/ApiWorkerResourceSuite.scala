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

import javax.ws.rs.core.MediaType

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceSuite
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerArguments}

class ApiWorkerResourceSuite extends ApiBaseResourceSuite {
  private var worker: Worker = _
  override protected def httpService: HttpService = worker

  override def beforeAll(): Unit = {
    celebornConf.set(CelebornConf.WORKER_HTTP_PORT.key, s"${Utils.selectRandomPort(1024, 65535)}")
    val workerArgs = new WorkerArguments(Array(), celebornConf)
    worker = new Worker(celebornConf, workerArgs)
    worker.metricsSystem.start()
    worker.startHttpServer()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    worker.metricsSystem.stop()
    worker.rpcEnv.shutdown()
    worker.stop(CelebornExitKind.EXIT_IMMEDIATELY)
  }

  test("listPartitionLocationInfo") {
    val response = webTarget.path("listPartitionLocationInfo").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("unavailablePeers") {
    val response = webTarget.path("unavailablePeers").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("isShutdown") {
    val response = webTarget.path("isShutdown").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }

  test("isRegistered") {
    val response = webTarget.path("isRegistered").request(MediaType.TEXT_PLAIN).get()
    assert(200 == response.getStatus)
  }
}
