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

package org.apache.celeborn.common.network.registration.anonymous;

import static org.apache.celeborn.common.protocol.MessageType.REGISTER_APPLICATION_REQUEST_VALUE;
import static org.apache.celeborn.common.protocol.MessageType.REGISTER_APPLICATION_RESPONSE;

import java.io.IOException;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.PbRegisterApplicationRequest;
import org.apache.celeborn.common.protocol.PbRegisterApplicationResponse;

public class RegistrationAnonymousRpcHandler extends BaseMessageHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RegistrationAnonymousRpcHandler.class);

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  private final BaseMessageHandler delegate;

  private RegistrationState registrationState = RegistrationState.NONE;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretRegistry secretRegistry;

  public RegistrationAnonymousRpcHandler(
      TransportConf conf,
      Channel channel,
      BaseMessageHandler delegate,
      SecretRegistry secretRegistry) {
    this.conf = conf;
    this.channel = channel;
    this.secretRegistry = secretRegistry;
    this.delegate = delegate;
  }

  @Override
  public boolean checkRegistered() {
    return delegate.checkRegistered();
  }

  @Override
  public void receive(
      TransportClient client, RequestMessage message, RpcResponseCallback callback) {
    if (registrationState == RegistrationState.REGISTERED) {
      LOG.trace("Already authenticated. Delegating {}", client.getClientId());
      delegate.receive(client, message, callback);
    } else {
      RpcRequest rpcRequest = (RpcRequest) message;
      try {
        processRpcMessage(client, rpcRequest, callback);
      } catch (Exception e) {
        LOG.error("Error while invoking RpcHandler#receive() on RPC id " + rpcRequest.requestId, e);
        registrationState = RegistrationState.FAILED;
        client
            .getChannel()
            .writeAndFlush(
                new RpcFailure(rpcRequest.requestId, Throwables.getStackTraceAsString(e)));
      }
    }
  }

  @Override
  public void receive(TransportClient client, RequestMessage message) {
    if (registrationState == RegistrationState.REGISTERED) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Already authenticated. Delegating {}", client.getClientId());
      }
      delegate.receive(client, message);
    } else {
      throw new SecurityException("Unauthenticated call to receive().");
    }
  }

  private void processRpcMessage(
      TransportClient client, RpcRequest message, RpcResponseCallback callback) throws IOException {
    TransportMessage pbMsg = TransportMessage.fromByteBuffer(message.body().nioByteBuffer());
    switch (pbMsg.getMessageTypeValue()) {
      case REGISTER_APPLICATION_REQUEST_VALUE:
        PbRegisterApplicationRequest registerApplicationRequest = pbMsg.getParsedPayload();
        LOG.trace("Application registration started {}", registerApplicationRequest.getId());
        processRegisterApplicationRequest(registerApplicationRequest, callback);
        registrationState = RegistrationState.REGISTERED;
        LOG.info(
            "Application registered: appId {} rpcId {}",
            registerApplicationRequest.getId(),
            message.requestId);
        break;
    }
  }

  private void processRegisterApplicationRequest(
      PbRegisterApplicationRequest registerApplicationRequest, RpcResponseCallback callback) {
    if (secretRegistry.isRegistered(registerApplicationRequest.getId())) {
      // Re-registration is not allowed.
      throw new IllegalStateException(
          "Application is already registered " + registerApplicationRequest.getId());
    }
    secretRegistry.register(
        registerApplicationRequest.getId(), registerApplicationRequest.getSecret());
    PbRegisterApplicationResponse response =
        PbRegisterApplicationResponse.newBuilder().setStatus(true).build();
    TransportMessage message =
        new TransportMessage(REGISTER_APPLICATION_RESPONSE, response.toByteArray());
    callback.onSuccess(message.toByteBuffer());
  }

  @Override
  public void channelInactive(TransportClient client) {
    delegate.channelInactive(client);
    cleanup();
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    delegate.exceptionCaught(cause, client);
  }

  private void complete() {
    cleanup();
  }

  private void cleanup() {}

  private enum RegistrationState {
    NONE,
    REGISTERED,
    FAILED
  }
}
