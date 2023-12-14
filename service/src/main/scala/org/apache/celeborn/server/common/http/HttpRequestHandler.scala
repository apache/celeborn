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
import org.apache.celeborn.common.metrics.sink.{JsonHttpRequestHandler, ServletHttpRequestHandler}
import org.apache.celeborn.server.common.HttpService

/**
 * A handler for the REST API that defines how to handle the HTTP request given a message.
 *
 * @param service The service of HTTP server.
 * @param uri The uri of HTTP request.
 */
@Sharable
class HttpRequestHandler(
    service: HttpService,
    servletHttpRequestHandlers: Array[ServletHttpRequestHandler])
  extends SimpleChannelInboundHandler[FullHttpRequest] with Logging {

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.uri()
    val (path, parameters) = HttpUtils.parseUri(uri)
    val msg = HttpUtils.handleRequest(service, path, parameters)
    val textType = "text/plain; charset=UTF-8"
    val jsonType = "application/json"
    val (response, contentType) = msg match {
      case Invalid.invalid =>
        if (servletHttpRequestHandlers != null) {
          servletHttpRequestHandlers.find(servlet =>
            uri == servlet.getServletPath()).map {
            case jsonHandler: JsonHttpRequestHandler =>
              (jsonHandler.handleRequest(uri), jsonType)
            case handler: ServletHttpRequestHandler =>
              (handler.handleRequest(uri), textType)
          }.getOrElse((s"Unknown path $uri!", textType))
        } else {
          (
            s"${Invalid.description(service.serviceName)} ${HttpUtils.help(service.serviceName)}",
            textType)
        }
      case _ => (msg, textType)
    }

    val res = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer(response, CharsetUtil.UTF_8))
    res.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType)
    ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE)
  }
}
