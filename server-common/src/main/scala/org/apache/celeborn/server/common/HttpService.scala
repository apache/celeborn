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

import io.netty.channel.ChannelFuture

import org.apache.celeborn.common.RssConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.server.common.http.{HttpRequestHandler, HttpServer, HttpServerInitializer}

abstract class HttpService extends Service with Logging {

  def getWorkerInfo: String

  def getThreadDump: String

  def getHostnameList: String

  def getApplicationList: String

  def getShuffleList: String

  def startHttpServer(): ChannelFuture = {
    val handlers =
      if (metricsSystem.running) {
        new HttpRequestHandler(this, metricsSystem.getPrometheusHandler)
      } else {
        new HttpRequestHandler(this, null)
      }
    val httpServer = new HttpServer(
      serviceName,
      prometheusHost(),
      prometheusPort(),
      new HttpServerInitializer(handlers))
    httpServer.start()
  }

  private def prometheusHost(): String = {
    serviceName match {
      case Service.MASTER =>
        RssConf.masterPrometheusMetricHost(conf)
      case Service.WORKER =>
        RssConf.workerPrometheusMetricHost(conf)
    }
  }

  private def prometheusPort(): Int = {
    serviceName match {
      case Service.MASTER =>
        RssConf.masterPrometheusMetricPort(conf)
      case Service.WORKER =>
        RssConf.workerPrometheusMetricPort(conf)
    }
  }

  override def initialize(): Unit = {
    super.initialize()
    startHttpServer()
  }
}
