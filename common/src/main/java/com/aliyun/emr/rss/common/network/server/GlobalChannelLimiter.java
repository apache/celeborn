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

package com.aliyun.emr.rss.common.network.server;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class GlobalChannelLimiter extends ChannelDuplexHandler
  implements MemoryTracker.MemoryTrackerListener {
  private static final long DEFAULT_CHECK_INTERVAL = 10;
  private static Logger logger = LoggerFactory.getLogger(GlobalChannelLimiter.class);
  private ConcurrentSet<Channel> channels = new ConcurrentSet<>();
  private ScheduledExecutorService checkExecutor = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setDaemon(true)
    .setNameFormat("GlobalChannelLimiter-check-thread").build());
  private MemoryTracker memoryTracker = MemoryTracker.instance();
  private static GlobalChannelLimiter globalChannelLimiter = new GlobalChannelLimiter();

  public static GlobalChannelLimiter globalChannelLimiter() {
    return globalChannelLimiter;
  }

  private GlobalChannelLimiter() {
    checkExecutor.scheduleAtFixedRate(() -> {
      if (channels.isEmpty()) {
        return;
      }
      if (memoryTracker.directMemoryCritical()) {
        pauseAllChannels();
      } else {
        resumeAllChannels();
      }
    }, DEFAULT_CHECK_INTERVAL, DEFAULT_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

    memoryTracker.registerMemoryListener(this);
  }

  private void pauseAllChannels() {
    channels.stream().forEach(c -> {
      if (c.config().isAutoRead()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Worker memory level is critical," +
                         " channel : {} stop receive data.", c);
        }
        c.config().setAutoRead(false);
      }
    });
  }

  private void resumeAllChannels() {
    channels.stream().forEach(c -> {
      if (!c.config().isAutoRead()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Worker memory level is normal," +
                         " channel : {} start receive data.", c);
        }
        c.config().setAutoRead(true);
      }
    });
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    channels.add(ctx.channel());
    super.handlerAdded(ctx);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (!ctx.channel().config().isAutoRead()) {
      ctx.channel().config().setAutoRead(true);
    }
    channels.remove(ctx.channel());
    super.handlerRemoved(ctx);
  }

  @Override
  public void onMemoryCritical() {
    logger.info("Channel limiter execute rate control action");
    pauseAllChannels();
  }
}
