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

import io.netty.channel.ChannelFuture

import com.aliyun.emr.rss.common.RssConf
import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.metrics.MetricsSystem
import com.aliyun.emr.rss.server.common.http.{HttpRequestHandler, HttpServer, HttpServerInitializer, HttpServiceInterface}

abstract class Service extends Logging with HttpServiceInterface {
  def serviceName: String
  def conf: RssConf
  def metricsSystem: MetricsSystem

  def start(): Unit

  def startHttpServer(): ChannelFuture = {
    val handlers =
      if (RssConf.metricsSystemEnable(conf)) {
        logInfo(s"Metrics system enabled.")
        metricsSystem.start()
        new HttpRequestHandler(this, metricsSystem.getPrometheusHandler)
      } else {
        new HttpRequestHandler(this, null)
      }

    val httpServer = new HttpServer(
      "master",
      RssConf.masterPrometheusMetricHost(conf),
      RssConf.masterPrometheusMetricPort(conf),
      new HttpServerInitializer(handlers))
    httpServer.start()
  }
}

object Service {
  val MASTER = "master"
  val WORKER = "worker"
}
