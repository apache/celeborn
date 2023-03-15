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

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.{HttpRequestHandler, HttpServer, HttpServerInitializer}

abstract class HttpService extends Service with Logging {

  private var httpServer: HttpServer = _

  def getConf: String = {
    val sb = new StringBuilder
    val maxKeyLength = conf.getAll.toMap.keys.map(_.length).max
    sb.append("==============Configurations==============\n")
    conf.getAll.foreach { case (key, value) =>
      sb.append(key.padTo(maxKeyLength + 10, " ") + value + "\n")
    }
    sb.toString()
  }

  def getWorkerInfo: String

  def getLostWorkers: String

  def getBlacklistedWorkers: String

  def getThreadDump: String

  def getHostnameList: String

  def getApplicationList: String

  def getShuffleList: String

  def listTopDiskUseApps: String

  def listPartitionLocationInfo: String

  def getUnavailablePeers: String

  def isShutdown: String

  def isRegistered: String

  def startHttpServer(): Unit = {
    val handlers =
      if (metricsSystem.running) {
        new HttpRequestHandler(this, metricsSystem.getPrometheusHandler)
      } else {
        new HttpRequestHandler(this, null)
      }
    httpServer = new HttpServer(
      serviceName,
      prometheusHost(),
      prometheusPort(),
      new HttpServerInitializer(handlers))
    httpServer.start()
  }

  private def prometheusHost(): String = {
    serviceName match {
      case Service.MASTER =>
        conf.masterPrometheusMetricHost
      case Service.WORKER =>
        conf.workerPrometheusMetricHost
    }
  }

  private def prometheusPort(): Int = {
    serviceName match {
      case Service.MASTER =>
        conf.masterPrometheusMetricPort
      case Service.WORKER =>
        conf.workerPrometheusMetricPort
    }
  }

  override def initialize(): Unit = {
    super.initialize()
    startHttpServer()
  }

  override def close(): Unit = {
    httpServer.stop()
    super.close()
  }
}
