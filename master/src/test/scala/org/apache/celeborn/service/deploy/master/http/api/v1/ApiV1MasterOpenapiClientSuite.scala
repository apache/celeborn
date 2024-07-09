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

import javax.servlet.http.HttpServletResponse

import com.google.common.io.Files

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.rest.v1.master._
import org.apache.celeborn.rest.v1.master.invoker._
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.HttpTestHelper
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}

class ApiV1MasterOpenapiClientSuite extends HttpTestHelper {
  private var master: Master = _
  override protected def httpService: HttpService = master
  private var apiClient: ApiClient = _

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
    apiClient = new ApiClient().setBasePath(s"http://${httpService.connectionUrl}")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    master.rpcEnv.shutdown()
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
    assert(api.getApplications.getApplications.isEmpty)
    assert(api.getApplicationHostNames.getHostnames.isEmpty)
    assert(api.getApplicationsDiskUsageSnapshots.getSnapshots.isEmpty)
  }

  test("master api") {
    val api = new MasterApi(apiClient)
    val e = intercept[ApiException](api.getMasterGroupInfo)
    assert(e.getCode == HttpServletResponse.SC_BAD_REQUEST)
    assert(e.getMessage.contains("HA is not enabled"))
  }

  test("shuffle api") {
    val api = new ShuffleApi(apiClient)
    assert(api.getShuffles.getShuffleIds.isEmpty)
  }

  test("worker api") {
    val api = new WorkerApi(apiClient)
    val workersResponse = api.getWorkers
    assert(workersResponse.getWorkers.isEmpty)
    assert(workersResponse.getLostWorkers.isEmpty)
    assert(workersResponse.getExcludedWorkers.isEmpty)
    assert(workersResponse.getManualExcludedWorkers.isEmpty)
    assert(workersResponse.getShutdownWorkers.isEmpty)
    assert(workersResponse.getDecommissioningWorkers.isEmpty)

    assert(api.getWorkerEvents.getWorkerEvents.isEmpty)
  }
}
