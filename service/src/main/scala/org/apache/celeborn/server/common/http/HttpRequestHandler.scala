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

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil

import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.sink.PrometheusHttpRequestHandler
import org.apache.celeborn.server.common.{HttpService, Service}

/**
 * A handler for the REST API that defines how to handle the HTTP request given a message.
 *
 * @param service The service of HTTP server.
 * @param uri The uri of HTTP request.
 */
@Sharable
class HttpRequestHandler(
    service: HttpService,
    prometheusHttpRequestHandler: PrometheusHttpRequestHandler)
  extends SimpleChannelInboundHandler[FullHttpRequest] with Logging {

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.uri()
    val msg = handleRequest(uri)
    val response = msg match {
      case "invalid" =>
        if (prometheusHttpRequestHandler != null) {
          prometheusHttpRequestHandler.handleRequest(uri)
        } else {
          s"invalid uri $uri"
        }
      case _ => msg
    }

    val res = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer(response, CharsetUtil.UTF_8))
    res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8")
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE)
  }

  private def handleRequest(uri: String): String = {
    if (service.serviceName == Service.MASTER) {
      new MasterRequestHandler(service, uri).handle()
    } else {
      new WorkerRequestHandler(service, uri).handle()
    }
  }
}

/**
 * A basic handler for the REST API that defines how to handle the HTTP request.
 *
 * @param service The service of HTTP server.
 * @param uri The uri of HTTP request.
 */
class BaseRequestHandler(service: HttpService, uri: String) extends Logging {

  val (path, parameters) = HttpUtils.parseUrl(uri)

  def handle(): String = {
    path match {
      case "/conf" =>
        service.getConf
      case "/workerInfo" =>
        service.getWorkerInfo
      case "/threadDump" =>
        service.getThreadDump
      case "/shuffles" =>
        service.getShuffleList
      case "/listTopDiskUsedApps" =>
        service.listTopDiskUseApps
      case _ => "invalid"
    }
  }
}

/**
 * A handler for the REST API that defines how to handle the HTTP request from master.
 *
 * @param service The service of HTTP server.
 * @param uri The uri of HTTP request from master.
 */
class MasterRequestHandler(service: HttpService, uri: String)
  extends BaseRequestHandler(service, uri) with Logging {

  override def handle(): String = {
    path match {
      case "/masterGroupInfo" =>
        service.getMasterGroupInfo
      case "/lostWorkers" =>
        service.getLostWorkers
      case "/excludedWorkers" =>
        service.getExcludedWorkers
      case "/shutdownWorkers" =>
        service.getShutdownWorkers
      case "/hostnames" =>
        service.getHostnameList
      case "/applications" =>
        service.getApplicationList
      case _ => super.handle()
    }
  }
}

/**
 * A handler for the REST API that defines how to handle the HTTP request from worker.
 *
 * @param service The service of HTTP server.
 * @param uri The uri of HTTP request from worker.
 */
class WorkerRequestHandler(service: HttpService, uri: String)
  extends BaseRequestHandler(service, uri) with Logging {

  override def handle(): String = {
    path match {
      case "/listPartitionLocationInfo" =>
        service.listPartitionLocationInfo
      case "/unavailablePeers" =>
        service.getUnavailablePeers
      case "/isShutdown" =>
        service.isShutdown
      case "/isRegistered" =>
        service.isRegistered
      case "/exit" =>
        service.exit(parameters.getOrElse("TYPE", ""))
      case _ => super.handle()
    }
  }
}
