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

package org.apache.celeborn.common.network.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.util.TransportConf;

@ChannelHandler.Sharable
public class ReconnectHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(ReconnectHandler.class);

  private final int maxReconnectRetries;
  private final int reconnectRetryWaitTimeMs;
  private final Bootstrap bootstrap;

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicInteger reconnectRetries = new AtomicInteger(0);

  public ReconnectHandler(TransportConf conf, Bootstrap bootstrap) {
    this.maxReconnectRetries = conf.maxReconnectRetries();
    this.reconnectRetryWaitTimeMs = conf.reconnectRetryWaitTimeMs();
    this.bootstrap = bootstrap;
  }

  @Override
  public void channelActive(ChannelHandlerContext context) throws Exception {
    reconnectRetries.set(0);
    super.channelActive(context);
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    if (stopped.get()) {
      super.channelInactive(context);
    } else {
      scheduleReconnect(context);
    }
  }

  private void scheduleReconnect(ChannelHandlerContext context) throws Exception {
    if (reconnectRetries.incrementAndGet() <= maxReconnectRetries) {
      LOG.warn(
          "Reconnect to {} {}/{} times.",
          context.channel().remoteAddress(),
          reconnectRetries,
          maxReconnectRetries);
      context
          .channel()
          .eventLoop()
          .schedule(
              () -> {
                bootstrap
                    .connect()
                    .addListener(
                        (ChannelFuture future) -> {
                          if (future.isSuccess()) {
                            reconnectRetries.set(0);
                          } else {
                            scheduleReconnect(context);
                          }
                        });
              },
              reconnectRetryWaitTimeMs,
              TimeUnit.MILLISECONDS);
    } else {
      super.channelInactive(context);
    }
  }

  public void stopReconnect() {
    stopped.set(true);
  }
}
