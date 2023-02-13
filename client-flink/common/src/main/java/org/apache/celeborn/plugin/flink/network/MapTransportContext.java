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

package org.apache.celeborn.plugin.flink.network;

import java.util.function.Supplier;

import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportChannelHandler;
import org.apache.celeborn.common.network.server.TransportRequestHandler;
import org.apache.celeborn.common.network.util.FrameDecoder;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.plugin.flink.utils.Utils;

public class MapTransportContext extends TransportContext {

  private static final Logger logger = LoggerFactory.getLogger(MapTransportContext.class);

  public MapTransportContext(
      TransportConf conf, BaseMessageHandler msgHandler, boolean closeIdleConnections) {
    super(conf, msgHandler, closeIdleConnections);
  }

  public TransportChannelHandler createChannelHandler(
      Channel channel, BaseMessageHandler msgHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler =
        new TransportRequestHandler(channel, client, msgHandler);
    return new TransportChannelHandler(
        client, responseHandler, requestHandler, conf.connectionTimeoutMs(), closeIdleConnections);
  }

  public TransportChannelHandler initializePipeline(
      SocketChannel channel, Supplier<ByteBuf> bufSupplier) {
    try {
      if (channelsLimiter != null) {
        channel.pipeline().addLast("limiter", channelsLimiter);
      }
      TransportChannelHandler channelHandler = createChannelHandler(channel, msgHandler);
      channel
          .pipeline()
          .addLast("encoder", ENCODER)
          .addLast(
              FrameDecoder.HANDLER_NAME, Utils.createFrameDecoderWithBufferSupplier(bufSupplier))
          .addLast(
              "idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
          .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }
}
