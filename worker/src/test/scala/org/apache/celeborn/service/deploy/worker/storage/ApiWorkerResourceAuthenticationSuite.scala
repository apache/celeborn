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

package org.apache.celeborn.service.deploy.worker.storage

import org.apache.celeborn.server.common.HttpService
import org.apache.celeborn.server.common.http.ApiBaseResourceAuthenticationSuite
import org.apache.celeborn.service.deploy.MiniClusterFeature
import org.apache.celeborn.service.deploy.worker.Worker

class ApiWorkerResourceAuthenticationSuite extends ApiBaseResourceAuthenticationSuite
  with MiniClusterFeature {
  private var worker: Worker = _
  override protected def httpService: HttpService = worker

  override def beforeAll(): Unit = {
    val (_, w) =
      setupMiniClusterWithRandomPorts(workerConf = celebornConf.getAll.toMap, workerNum = 1)
    worker = w.head
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    shutdownMiniCluster()
  }
}
