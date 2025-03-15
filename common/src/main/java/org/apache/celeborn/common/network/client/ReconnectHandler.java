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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.celeborn.common.network.util.TransportConf;

public class ReconnectHandler extends ChannelInboundHandlerAdapter {

  private final int maxReconnectRetries;
  private final int reconnectRetryWaitTimeMs;
  private final Bootstrap bootstrap;

  private volatile boolean shouldReconnect = true;
  private int reconnectRetries = 0;

  public ReconnectHandler(TransportConf conf, Bootstrap bootstrap) {
    this.maxReconnectRetries = conf.maxReconnectRetries();
    this.reconnectRetryWaitTimeMs = conf.reconnectRetryWaitTimeMs();
    this.bootstrap = bootstrap;
  }

  @Override
  public void channelActive(ChannelHandlerContext context) throws Exception {
    reconnectRetries = 0;
    super.channelActive(context);
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    if (shouldReconnect && reconnectRetries < maxReconnectRetries) {
      scheduleReconnect(context);
    } else {
      super.channelInactive(context);
    }
  }

  private void scheduleReconnect(ChannelHandlerContext context) {
    reconnectRetries++;
    context
        .channel()
        .eventLoop()
        .schedule(
            () -> {
              bootstrap
                  .connect()
                  .addListener(
                      (ChannelFuture future) -> {
                        System.out.println(future.isSuccess());
                        if (!future.isSuccess()) {
                          scheduleReconnect(context);
                        }
                      });
            },
            reconnectRetryWaitTimeMs,
            TimeUnit.MILLISECONDS);
  }

  public void stopReconnect() {
    shouldReconnect = false;
  }
}
