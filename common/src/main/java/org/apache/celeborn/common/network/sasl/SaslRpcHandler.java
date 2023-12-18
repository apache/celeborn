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

package org.apache.celeborn.common.network.sasl;

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.server.AbstractAuthRpcHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.PbSaslRequest;

/**
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler. The
 * delegate will only receive messages if the given connection has been successfully authenticated.
 * A connection may be authenticated at most once.
 *
 * <p>Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 */
public class SaslRpcHandler extends AbstractAuthRpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretRegistry secretRegistry;

  private CelebornSaslServer saslServer;

  public SaslRpcHandler(
      TransportConf conf,
      Channel channel,
      BaseMessageHandler delegate,
      SecretRegistry secretRegistry) {
    super(delegate);
    this.conf = conf;
    this.channel = channel;
    this.secretRegistry = secretRegistry;
    this.saslServer = null;
  }

  @Override
  public boolean checkRegistered() {
    return delegate.checkRegistered();
  }

  @Override
  public boolean doAuthChallenge(
      TransportClient client, RequestMessage message, RpcResponseCallback callback) {
    if (saslServer == null || !saslServer.isComplete()) {
      RpcRequest rpcRequest = (RpcRequest) message;
      PbSaslRequest saslMessage;
      try {
        TransportMessage pbMsg = TransportMessage.fromByteBuffer(message.body().nioByteBuffer());
        saslMessage = pbMsg.getParsedPayload();
      } catch (IOException e) {
        logger.error("Error while parsing Sasl Message with RPC id {}", rpcRequest.requestId, e);
        callback.onFailure(e);
        return false;
      }
      if (saslServer == null) {
        saslServer =
            new CelebornSaslServer(
                DIGEST_MD5,
                DEFAULT_SASL_SERVER_PROPS,
                new CelebornSaslServer.DigestCallbackHandler(secretRegistry));
      }
      byte[] response = saslServer.response(saslMessage.getPayload().toByteArray());
      callback.onSuccess(ByteBuffer.wrap(response));
    }
    if (saslServer.isComplete()) {
      logger.debug("SASL authentication successful for channel {}", client);
      complete();
      return true;
    }
    return false;
  }

  @Override
  public void channelInactive(TransportClient client) {
    super.channelInactive(client);
    cleanup();
  }

  private void complete() {
    cleanup();
  }

  private void cleanup() {
    if (null != saslServer) {
      try {
        saslServer.dispose();
      } catch (RuntimeException e) {
        logger.error("Error while disposing SASL server", e);
      } finally {
        saslServer = null;
      }
    }
  }
}
