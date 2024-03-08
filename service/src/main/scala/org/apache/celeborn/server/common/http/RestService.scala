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

package org.apache.celeborn.server.common.http

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.Service

abstract class RestService extends Service with Logging {
  private var httpServer: JettyServer = _

  private def httpHost(): String = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpHost
      case Service.WORKER =>
        conf.workerHttpHost
    }
  }

  private def httpPort(): Int = {
    serviceName match {
      case Service.MASTER =>
        conf.masterHttpPort
      case Service.WORKER =>
        conf.workerHttpPort
    }
  }

  def startHttpServer(): Unit = {
    httpServer = JettyServer(
      serviceName,
      httpHost(),
      httpPort(),
      999
    )
    startInternal()
    // block until the HTTP server is started, otherwise, we may get
    // the wrong HTTP server port -1
    while (httpServer.getState != "STARTED") {
      logInfo(s"Waiting for $serviceName's HTTP server getting started")
      Thread.sleep(1000)
    }
  }

  protected def startInternal(): Unit = {

  }



}
