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

package org.apache.celeborn.server.common

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.{HttpRequestHandler, HttpServer, HttpServerInitializer}

abstract class HttpService extends Service with Logging {

  private var httpServer: HttpServer = _

  def getConf: String = {
    val sb = new StringBuilder
    sb.append("=========================== Configuration ============================\n")
    if (conf.getAll.length > 0) {
      val maxKeyLength = conf.getAll.toMap.keys.map(_.length).max
      conf.getAll.foreach { case (key, value) =>
        sb.append(s"${key.padTo(maxKeyLength + 10, " ").mkString}$value\n")
      }
    }
    sb.toString()
  }

  def getWorkerInfo: String

  def getThreadDump: String

  def getShuffleList: String

  def getApplicationList: String

  def listTopDiskUseApps: String

  def getMasterGroupInfo: String = throw new UnsupportedOperationException()

  def getLostWorkers: String = throw new UnsupportedOperationException()

  def getShutdownWorkers: String = throw new UnsupportedOperationException()

  def getExcludedWorkers: String = throw new UnsupportedOperationException()

  def getHostnameList: String = throw new UnsupportedOperationException()

  def exclude(addWorkers: String, removeWorkers: String): String =
    throw new UnsupportedOperationException()

  def listPartitionLocationInfo: String = throw new UnsupportedOperationException()

  def getUnavailablePeers: String = throw new UnsupportedOperationException()

  def isShutdown: String = throw new UnsupportedOperationException()

  def isRegistered: String = throw new UnsupportedOperationException()

  def exit(exitType: String): String = throw new UnsupportedOperationException()

  def startHttpServer(): Unit = {
    val handlers =
      if (metricsSystem.running) {
        new HttpRequestHandler(this, metricsSystem.getServletHandlers)
      } else {
        new HttpRequestHandler(this, null)
      }
    httpServer = new HttpServer(
      serviceName,
      httpHost(),
      httpPort(),
      new HttpServerInitializer(handlers))
    httpServer.start()
  }

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

  override def initialize(): Unit = {
    super.initialize()
    startHttpServer()
  }

  override def stop(exitKind: Int): Unit = {
    // may be null when running the unit test
    if (null != httpServer) {
      httpServer.stop(exitKind)
    }
    super.stop(exitKind)
  }
}
