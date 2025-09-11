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

package org.apache.celeborn.common.network.sasl.registration;

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;
import static org.apache.celeborn.common.protocol.MessageType.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcFailure;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.sasl.CelebornSaslServer;
import org.apache.celeborn.common.network.sasl.SaslRpcHandler;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.PbAuthType;
import org.apache.celeborn.common.protocol.PbAuthenticationInitiationRequest;
import org.apache.celeborn.common.protocol.PbAuthenticationInitiationResponse;
import org.apache.celeborn.common.protocol.PbRegisterApplicationRequest;
import org.apache.celeborn.common.protocol.PbRegisterApplicationResponse;
import org.apache.celeborn.common.protocol.PbSaslMechanism;
import org.apache.celeborn.common.protocol.PbSaslRequest;

/**
 * RPC Handler which registers an application. If an application is registered or the connection is
 * authenticated, subsequent messages are delegated to a child RPC handler.
 */
public class RegistrationRpcHandler extends BaseMessageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RegistrationRpcHandler.class);

  private static final String VERSION = "1.0";

  /**
   * TODO: This should be made configurable. For now, we only support ANONYMOUS for client-auth and
   * DIGEST-MD5 for connect-auth.
   */
  private static final List<PbSaslMechanism> SASL_MECHANISMS =
      Lists.newArrayList(
          PbSaslMechanism.newBuilder()
              .setMechanism(ANONYMOUS)
              .addAuthTypes(PbAuthType.CLIENT_AUTH)
              .build(),
          PbSaslMechanism.newBuilder()
              .setMechanism(DIGEST_MD5)
              .addAuthTypes(PbAuthType.CONNECTION_AUTH)
              .build());

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  private final BaseMessageHandler delegate;

  private RegistrationState registrationState = RegistrationState.NONE;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretRegistry secretRegistry;

  private SaslRpcHandler saslHandler;

  /** Used for client authentication. */
  private CelebornSaslServer saslServer = null;

  public RegistrationRpcHandler(
      TransportConf conf,
      Channel channel,
      BaseMessageHandler delegate,
      SecretRegistry secretRegistry) {
    this.conf = conf;
    this.channel = channel;
    this.secretRegistry = secretRegistry;
    this.delegate = delegate;
    this.saslHandler = new SaslRpcHandler(conf, channel, delegate, secretRegistry);
  }

  @Override
  public boolean checkRegistered() {
    return delegate.checkRegistered();
  }

  @Override
  public final void receive(
      TransportClient client, RequestMessage message, RpcResponseCallback callback) {
    // The message is delegated either if the client is already authenticated or if the connection
    // is authenticated.
    if (registrationState == RegistrationState.REGISTERED || saslHandler.isAuthenticated()) {
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
  public final void receive(TransportClient client, RequestMessage message) {
    if (registrationState == RegistrationState.REGISTERED || saslHandler.isAuthenticated()) {
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
      case AUTHENTICATION_INITIATION_REQUEST_VALUE:
        // TODO: not validating the auth init request. Should we?
        PbAuthenticationInitiationRequest authInitRequest = pbMsg.getParsedPayload();
        checkRequestAllowed(RegistrationState.NONE);
        respondToAuthInitialization(callback);
        registrationState = RegistrationState.INIT;
        LOG.trace("Authentication initialization completed: rpcId {}", message.requestId);
        break;
      case SASL_REQUEST_VALUE:
        PbSaslRequest saslRequest = pbMsg.getParsedPayload();
        if (saslRequest.getAuthType().equals(PbAuthType.CLIENT_AUTH)) {
          LOG.trace("Received Sasl Message for client authentication");
          checkRequestAllowed(RegistrationState.INIT);
          authenticateClient(saslRequest, callback);
          if (saslServer.isComplete()) {
            LOG.debug("SASL authentication successful for channel {}", client);
            complete();
            registrationState = RegistrationState.AUTHENTICATED;
            LOG.trace("Client authenticated: rpcId {}", message.requestId);
          }
        } else {
          // It is a SASL message to authenticate the connection. If the application hasn't
          // registered, then
          // saslHandler will throw an exception that the app hasn't registered.
          LOG.trace("Delegating to sasl handler: rpcId {}", message.requestId);
          saslHandler.receive(client, message, callback);
        }
        break;
      case REGISTER_APPLICATION_REQUEST_VALUE:
        PbRegisterApplicationRequest registerApplicationRequest = pbMsg.getParsedPayload();
        checkRequestAllowed(RegistrationState.AUTHENTICATED);
        LOG.trace("Application registration started {}", registerApplicationRequest.getId());

        if (!secretRegistry.registrationEnabled()) {
          throw new IOException(
              "Application " + registerApplicationRequest.getId() + " failed to register.");
        }

        processRegisterApplicationRequest(registerApplicationRequest, callback);
        registrationState = RegistrationState.REGISTERED;
        client.setClientId(registerApplicationRequest.getId());
        LOG.info(
            "Application registered: appId {} rpcId {}",
            registerApplicationRequest.getId(),
            message.requestId);
        break;
      default:
        throw new SecurityException(
            "The app is not registered and the connection is not authenticated "
                + message.requestId);
    }
  }

  private void checkRequestAllowed(RegistrationState expectedState) {
    if (registrationState != expectedState) {
      throw new IllegalStateException(
          "Invalid registration state. Expected: "
              + expectedState
              + ", Actual: "
              + registrationState);
    }
  }

  private void respondToAuthInitialization(RpcResponseCallback callback) {
    PbAuthenticationInitiationResponse response =
        PbAuthenticationInitiationResponse.newBuilder()
            .setAuthEnabled(conf.authEnabled())
            .setVersion(VERSION)
            .addAllSaslMechanisms(SASL_MECHANISMS)
            .build();
    TransportMessage message =
        new TransportMessage(AUTHENTICATION_INITIATION_RESPONSE, response.toByteArray());
    callback.onSuccess(message.toByteBuffer());
  }

  private void authenticateClient(PbSaslRequest saslMessage, RpcResponseCallback callback) {
    if (saslServer == null || !saslServer.isComplete()) {
      if (saslServer == null) {
        saslServer = new CelebornSaslServer(ANONYMOUS, null, null);
      }
      byte[] response = saslServer.response(saslMessage.getPayload().toByteArray());
      callback.onSuccess(ByteBuffer.wrap(response));
    } else {
      throw new IllegalArgumentException("Unexpected message type " + saslMessage.toString());
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

  private void cleanup() {
    if (null != saslServer) {
      try {
        saslServer.dispose();
      } catch (RuntimeException e) {
        LOG.error("Error while disposing SASL server", e);
      } finally {
        saslServer = null;
      }
    }
    saslHandler.cleanup();
  }

  private enum RegistrationState {
    NONE,
    INIT,
    AUTHENTICATED,
    REGISTERED,
    FAILED
  }
}
