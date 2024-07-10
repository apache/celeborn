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

package org.apache.celeborn.service.deploy.web.http.api

import javax.ws.rs.core.MediaType

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.util.{CelebornExitKind, Utils}
import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.HttpTestHelper
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.web.{Web, WebArguments}
import org.apache.celeborn.service.deploy.web.http.api.dto.ClusterSummary

class ApiWebResourceSuite extends HttpTestHelper with MiniClusterFeature {

  private var masterEndpoint: collection.Set[String] = _
  private var web: Web = _

  override protected def httpService: HttpService = web

  override def beforeAll(): Unit = {
    val masters = setupMiniClusterWithRandomPorts()
    masterEndpoint = masters.map(master => s"${master.masterArgs.host}:${master.masterArgs.port}")
    val port = Utils.selectRandomPort(1024, 65535)
    celebornConf.set(CelebornConf.WEB_HTTP_HOST, "127.0.0.1")
      .set(CelebornConf.WEB_HTTP_PORT, port + 1)
      .set(CelebornConf.MASTER_ENDPOINTS, masterEndpoint.toSeq)
    web = new Web(
      celebornConf,
      new WebArguments(Array("-h", "localhost", "-p", port.toString), celebornConf))
    new Thread() {
      override def run(): Unit = {
        web.initialize()
      }
    }.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    web.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    web.rpcEnv.shutdown()
    shutdownMiniCluster()
  }

  test("cluster/overview") {
    val response = webTarget.path("cluster/overview").request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val clusterSummary = response.readEntity(classOf[ClusterSummary])
    assert(masterEndpoint.mkString(",") == clusterSummary.getEndpoint)
    assert(clusterSummary.getLeader != null)
  }
}
