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

package org.apache.celeborn.common.network;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.MessageEncoder;
import org.apache.celeborn.common.network.protocol.SslMessageEncoder;
import org.apache.celeborn.common.network.server.*;
import org.apache.celeborn.common.network.ssl.SSLFactory;
import org.apache.celeborn.common.network.util.FrameDecoder;
import org.apache.celeborn.common.network.util.NettyLogger;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a {@link TransportChannelHandler}.
 *
 * <p>There are two communication protocols that the TransportClient provides, control-plane RPCs
 * and data-plane "chunk fetching". The handling of the RPCs is performed outside the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * <p>The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class TransportContext implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private static final NettyLogger nettyLogger = new NettyLogger();
  private final TransportConf conf;
  private final BaseMessageHandler msgHandler;
  private final ChannelDuplexHandler channelsLimiter;
  private final boolean closeIdleConnections;
  // Non-null if SSL is enabled, null otherwise.
  @Nullable private final SSLFactory sslFactory;
  private final boolean enableHeartbeat;
  private final AbstractSource source;

  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  private static final SslMessageEncoder SSL_ENCODER = SslMessageEncoder.INSTANCE;

  public TransportContext(
      TransportConf conf,
      BaseMessageHandler msgHandler,
      boolean closeIdleConnections,
      ChannelDuplexHandler channelsLimiter,
      boolean enableHeartbeat,
      AbstractSource source) {
    this.conf = conf;
    this.msgHandler = msgHandler;
    this.closeIdleConnections = closeIdleConnections;
    this.sslFactory = SSLFactory.createSslFactory(conf);
    this.channelsLimiter = channelsLimiter;
    this.enableHeartbeat = enableHeartbeat;
    this.source = source;

    if (null != this.sslFactory) {
      logger.info(
          "SSL factory created for module {}, has keys ? {}",
          conf.getModuleName(),
          this.sslFactory.hasKeyManagers());
    } else {
      logger.info("SSL not enabled for module = {}", conf.getModuleName());
    }
  }

  public TransportContext(
      TransportConf conf,
      BaseMessageHandler msgHandler,
      boolean closeIdleConnections,
      boolean enableHeartbeat,
      AbstractSource source) {
    this(conf, msgHandler, closeIdleConnections, null, enableHeartbeat, source);
  }

  public TransportContext(
      TransportConf conf,
      BaseMessageHandler msgHandler,
      boolean closeIdleConnections,
      boolean enableHeartbeat,
      AbstractSource source,
      boolean collectFetchChunkDetailMetrics) {
    this(conf, msgHandler, closeIdleConnections, null, enableHeartbeat, source);
    if (collectFetchChunkDetailMetrics) {
      ENCODER.setSource(source);
    }
  }

  public TransportContext(
      TransportConf conf, BaseMessageHandler msgHandler, boolean closeIdleConnections) {
    this(conf, msgHandler, closeIdleConnections, null, false, null);
  }

  public TransportContext(TransportConf conf, BaseMessageHandler msgHandler) {
    this(conf, msgHandler, false, false, null);
  }

  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(Collections.emptyList());
  }

  /** Create a server which will attempt to bind to a specific host and port. */
  public TransportServer createServer(String host, int port) {
    return new TransportServer(this, host, port, Collections.emptyList());
  }

  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, bootstraps);
  }

  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(null, 0, bootstraps);
  }

  public TransportServer createServer(int port) {
    return createServer(null, port, Collections.emptyList());
  }

  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return createServer(null, port, bootstraps);
  }

  /** For Suite only */
  public TransportServer createServer() {
    return createServer(null, 0, Collections.emptyList());
  }

  public boolean sslEncryptionEnabled() {
    return this.sslFactory != null;
  }

  public TransportChannelHandler initializePipeline(
      SocketChannel channel, ChannelInboundHandlerAdapter decoder, boolean isClient) {
    return initializePipeline(channel, decoder, msgHandler, isClient);
  }

  public TransportChannelHandler initializePipeline(
      SocketChannel channel, BaseMessageHandler resolvedMsgHandler, boolean isClient) {
    return initializePipeline(channel, new TransportFrameDecoder(), resolvedMsgHandler, isClient);
  }

  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      ChannelInboundHandlerAdapter decoder,
      BaseMessageHandler resolvedMsgHandler,
      boolean isClient) {
    try {
      ChannelPipeline pipeline = channel.pipeline();
      if (nettyLogger.getLoggingHandler() != null) {
        pipeline.addLast("loggingHandler", nettyLogger.getLoggingHandler());
      }
      if (sslEncryptionEnabled()) {
        if (!isClient && !sslFactory.hasKeyManagers()) {
          throw new IllegalStateException("Not a client connection and no keys configured");
        }

        SslHandler sslHandler;
        try {
          sslHandler = new SslHandler(sslFactory.createSSLEngine(isClient, channel.alloc()));
          sslHandler.setHandshakeTimeoutMillis(conf.sslHandshakeTimeoutMs());
        } catch (Exception e) {
          throw new IllegalStateException("Error creating Netty SslHandler", e);
        }
        pipeline.addFirst("NettySslEncryptionHandler", sslHandler);
        // Cannot use zero-copy with HTTPS, so we add in our ChunkedWriteHandler just before the
        // MessageEncoder
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
      }

      if (channelsLimiter != null) {
        pipeline.addLast("limiter", channelsLimiter);
      }
      TransportChannelHandler channelHandler = createChannelHandler(channel, resolvedMsgHandler);
      pipeline
          .addLast("encoder", sslEncryptionEnabled() ? SSL_ENCODER : ENCODER)
          .addLast(FrameDecoder.HANDLER_NAME, decoder)
          .addLast(
              "idleStateHandler",
              enableHeartbeat
                  ? new IdleStateHandler(conf.connectionTimeoutMs() / 1000, 0, 0)
                  : new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
          .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  private TransportChannelHandler createChannelHandler(
      Channel channel, BaseMessageHandler msgHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(conf, channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler =
        new TransportRequestHandler(channel, client, msgHandler);
    return new TransportChannelHandler(
        client,
        responseHandler,
        requestHandler,
        conf.connectionTimeoutMs(),
        closeIdleConnections,
        enableHeartbeat,
        conf.clientHeartbeatInterval(),
        this);
  }

  public TransportConf getConf() {
    return conf;
  }

  public BaseMessageHandler getMsgHandler() {
    return msgHandler;
  }

  public AbstractSource getSource() {
    return source;
  }

  @Override
  public void close() {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }
}
