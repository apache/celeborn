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

package com.aliyun.emr.rss.server.common

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.server.common.http.{HttpRequestHandler, HttpServer, HttpServerInitializer}
import io.netty.channel.ChannelFuture

abstract class HttpService extends Service with Logging {

  def getWorkerInfo: String = ""

  def getThreadDump: String = ""

  def getHostnameList: String = ""

  def getApplicationList: String = ""

  def getShuffleList: String = ""

  def startHttpServer(service: Service): ChannelFuture = {
    val handlers =
      if (RssConf.metricsSystemEnable(service.conf)) {
        logInfo(s"Metrics system enabled.")
        service.metricsSystem.start()
        new HttpRequestHandler(service, service.metricsSystem.getPrometheusHandler)
      } else {
        new HttpRequestHandler(service, null)
      }

    val httpServer = new HttpServer(
      "master",
      RssConf.masterPrometheusMetricHost(service.conf),
      RssConf.masterPrometheusMetricPort(service.conf),
      new HttpServerInitializer(handlers))
    httpServer.start()
  }
}
