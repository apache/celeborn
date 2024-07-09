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

package org.apache.celeborn.service.deploy.worker.http.api.v1

import javax.servlet.http.HttpServletResponse

import org.apache.celeborn.rest.v1.worker._
import org.apache.celeborn.rest.v1.worker.invoker.{ApiClient, ApiException}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.HttpTestHelper
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class ApiV1WorkerOpenapiClientSuite extends HttpTestHelper with MiniClusterFeature {
  private var worker: Worker = _
  override protected def httpService: HttpService = worker
  private var apiClient: ApiClient = _

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup celeborn mini cluster")
    val (m, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    worker = w.head
    super.beforeAll()
    apiClient = new ApiClient().setBasePath(s"http://${httpService.connectionUrl}")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    logInfo("all test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("default api") {
    val api = new DefaultApi(apiClient)
    assert(!api.getThreadDump.getThreadStacks.isEmpty)
  }

  test("conf api") {
    val api = new ConfApi(apiClient)
    assert(!api.getConf.getConfigs.isEmpty)
    val e = intercept[ApiException](api.getDynamicConf("", "", ""))
    assert(e.getCode == HttpServletResponse.SC_SERVICE_UNAVAILABLE)
    assert(e.getMessage.contains("Dynamic configuration is disabled"))
  }

  test("application api") {
    val api = new ApplicationApi(apiClient)
    assert(api.getApplicationList.getApplications.isEmpty)
    assert(api.getApplicationsDiskUsage.getAppDiskUsages.isEmpty)
  }

  test("shuffle api") {
    val api = new ShuffleApi(apiClient)
    assert(api.getShuffles.getShuffleIds.isEmpty)
    assert(api.getShufflePartitions.getPrimaryPartitions.isEmpty)
    assert(api.getShufflePartitions.getReplicaPartitions.isEmpty)
  }

  test("worker api") {
    val api = new WorkerApi(apiClient)
    val workerInfo = api.getWorkerInfo
    assert(!workerInfo.getIsShutdown)
    assert(workerInfo.getIsRegistered)
    assert(!workerInfo.getIsDecommissioning)

    // assert(api.workerExit(new WorkerExitRequest()).getSuccess)
  }
}
