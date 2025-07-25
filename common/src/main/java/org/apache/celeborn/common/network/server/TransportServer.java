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

package org.apache.celeborn.common.network.server;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.util.*;
import org.apache.celeborn.common.util.CelebornExitKind;
import org.apache.celeborn.common.util.JavaUtils;

/** Server for the efficient, low-level streaming service. */
public class TransportServer implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final BaseMessageHandler appMessageHandler;
  private final List<TransportServerBootstrap> bootstraps;
  private ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private AbstractSource source;
  private int port = -1;

  public TransportServer(
      TransportContext context,
      String hostToBind,
      int portToBind,
      List<TransportServerBootstrap> bootstraps) {
    this.context = context;
    this.conf = context.getConf();
    this.source = context.getSource();
    this.appMessageHandler = Preconditions.checkNotNull(context.getMsgHandler());
    this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

    boolean shouldClose = true;
    try {
      init(hostToBind, portToBind);
      shouldClose = false;
    } finally {
      if (shouldClose) {
        JavaUtils.closeQuietly(this);
      }
    }
  }

  public int getPort() {
    if (port == -1) {
      throw new IllegalStateException("Server not initialized");
    }
    return port;
  }

  private void init(String hostToBind, int portToBind) {

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup =
        NettyUtils.createEventLoop(ioMode, 1, conf.getModuleName() + "-boss");
    EventLoopGroup workerGroup =
        NettyUtils.createEventLoop(ioMode, conf.serverThreads(), conf.getModuleName() + "-server");

    ByteBufAllocator allocator =
        NettyUtils.getByteBufAllocator(conf, source, true, conf.serverThreads());

    bootstrap =
        new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NettyUtils.getServerChannelClass(ioMode))
            .option(ChannelOption.ALLOCATOR, allocator)
            .option(ChannelOption.SO_REUSEADDR, !SystemUtils.IS_OS_WINDOWS)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.ALLOCATOR, allocator);

    if (conf.backlog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, conf.backlog());
    }

    if (conf.receiveBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    initializeChannel(bootstrap);

    InetSocketAddress address =
        hostToBind == null
            ? new InetSocketAddress(portToBind)
            : new InetSocketAddress(hostToBind, portToBind);
    channelFuture = bootstrap.bind(address);
    channelFuture.syncUninterruptibly();

    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    logger.debug("Shuffle server started on {}:{}", address.getHostString(), port);
  }

  protected void initializeChannel(ServerBootstrap bootstrap) {
    bootstrap.childHandler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            BaseMessageHandler baseHandler = appMessageHandler;
            logger.debug("number of bootstraps {}", bootstraps.size());
            for (TransportServerBootstrap bootstrap : bootstraps) {
              logger.debug(
                  "Adding bootstrap to TransportServer {}.", bootstrap.getClass().getName());
              baseHandler = bootstrap.doBootstrap(ch, baseHandler);
            }
            context.initializePipeline(ch, baseHandler, false);
          }
        });
  }

  @Override
  public void close() {
    shutdown(CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN());
  }

  public void shutdown(int exitKind) {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (bootstrap != null && bootstrap.config().group() != null) {
      if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN()) {
        bootstrap.config().group().shutdownGracefully();
      } else {
        bootstrap.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS);
      }
    }
    if (bootstrap != null && bootstrap.config().childGroup() != null) {
      if (exitKind == CelebornExitKind.WORKER_GRACEFUL_SHUTDOWN()) {
        bootstrap.config().childGroup().shutdownGracefully();
      } else {
        bootstrap.config().childGroup().shutdownGracefully(0, 0, TimeUnit.SECONDS);
      }
    }
    bootstrap = null;
  }
}
