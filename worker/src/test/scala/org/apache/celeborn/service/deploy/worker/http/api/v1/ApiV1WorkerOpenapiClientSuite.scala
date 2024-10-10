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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.rest.v1.worker._
import org.apache.celeborn.rest.v1.worker.invoker.{ApiClient, ApiException}
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.master.Master
import org.apache.celeborn.service.deploy.worker.Worker

abstract class ApiV1WorkerOpenapiClientSuite extends CelebornFunSuite with MiniClusterFeature {
  private val celebornConf = new CelebornConf()
  protected var master: Master = _
  protected var worker: Worker = _
  private var workerApiClient: ApiClient = _

  override def beforeAll(): Unit = {
    logInfo("test initialized, setup celeborn mini cluster")
    val (m, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    master = m
    worker = w.head
    super.beforeAll()
    workerApiClient = new ApiClient().setBasePath(s"http://${worker.connectionUrl}")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    logInfo("all test complete, stop celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("worker: default api") {
    val api = new DefaultApi(workerApiClient)
    assert(!api.getThreadDump.getThreadStacks.isEmpty)
    assert(
      api.getContainerInfo.getContainerAddress.nonEmpty && api.getContainerInfo.getContainerAddress.nonEmpty)
  }

  test("worker: conf api") {
    val api = new ConfApi(workerApiClient)
    assert(!api.getConf.getConfigs.isEmpty)
    val e = intercept[ApiException](api.getDynamicConf("", "", ""))
    assert(e.getCode == HttpServletResponse.SC_SERVICE_UNAVAILABLE)
    assert(e.getMessage.contains("Dynamic configuration is disabled"))
  }

  test("worker: application api") {
    val api = new ApplicationApi(workerApiClient)
    assert(api.getApplicationList.getApplications.isEmpty)
    assert(api.getApplicationsDiskUsage.getAppDiskUsages.isEmpty)
  }

  test("worker: shuffle api") {
    val api = new ShuffleApi(workerApiClient)
    assert(api.getShuffles.getShuffleIds.isEmpty)
    assert(api.getShufflePartitions.getPrimaryPartitions.isEmpty)
    assert(api.getShufflePartitions.getReplicaPartitions.isEmpty)
  }

  test("worker: worker api") {
    val api = new WorkerApi(workerApiClient)
    val workerInfo = api.getWorkerInfo
    assert(!workerInfo.getIsShutdown)
    assert(workerInfo.getIsRegistered)
    assert(!workerInfo.getIsDecommissioning)

    assert(api.unavailablePeers().getPeers.isEmpty)

    // assert(api.workerExit(new WorkerExitRequest()).getSuccess)
  }
}
