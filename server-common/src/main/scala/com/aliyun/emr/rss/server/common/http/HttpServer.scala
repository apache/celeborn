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

package com.aliyun.emr.rss.server.common.http

import java.net.InetSocketAddress

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer}
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.logging.LoggingHandler
import io.netty.handler.logging.LogLevel

import com.aliyun.emr.rss.common.internal.Logging
import com.aliyun.emr.rss.common.network.util.{IOMode, NettyUtils}

class HttpServer(
    role: String,
    host: String,
    port: Int,
    channelInitializer: ChannelInitializer[_]) extends Logging {
  @throws[Exception]
  def start(): ChannelFuture = {
    val bootstrap = new ServerBootstrap
    val boss = NettyUtils.createEventLoop(IOMode.NIO, 1, role + "-http-boss")
    val worker = NettyUtils.createEventLoop(IOMode.NIO, 2, role + "-http-worker")

    bootstrap
      .group(boss, worker)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .channel(classOf[NioServerSocketChannel])
      .childHandler(channelInitializer)

    val f = bootstrap.bind(new InetSocketAddress(host, port)).sync
    logInfo(s"HttpServer started on port $port.")
    f.syncUninterruptibly()
    f
  }
}
