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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RequestMessage;

/**
 * RPC Handler which performs authentication, and when it's successful, delegates further calls to
 * another RPC handler. The authentication handshake itself should be implemented by subclasses.
 */
public abstract class AbstractAuthRpcHandler extends BaseMessageHandler {
  /** RpcHandler we will delegate to for authenticated connections. */
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAuthRpcHandler.class);

  protected final BaseMessageHandler delegate;

  private boolean isAuthenticated;

  protected AbstractAuthRpcHandler(BaseMessageHandler delegate) {
    this.delegate = delegate;
  }

  /**
   * Responds to an authentication challenge.
   *
   * @return Whether the client is authenticated.
   */
  protected abstract boolean doAuthChallenge(
      TransportClient client, RequestMessage message, RpcResponseCallback callback);

  @Override
  public final void receive(
      TransportClient client, RequestMessage message, RpcResponseCallback callback) {
    if (isAuthenticated) {
      LOG.trace("Already authenticated. Delegating {}", client.getClientId());
      delegate.receive(client, message, callback);
    } else {
      isAuthenticated = doAuthChallenge(client, message, callback);
    }
  }

  @Override
  public final void receive(TransportClient client, RequestMessage message) {
    if (isAuthenticated) {
      delegate.receive(client, message);
    } else {
      throw new SecurityException("Unauthenticated call to receive().");
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    delegate.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    delegate.channelInactive(client);
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    delegate.exceptionCaught(cause, client);
  }

  public boolean isAuthenticated() {
    return isAuthenticated;
  }
}
