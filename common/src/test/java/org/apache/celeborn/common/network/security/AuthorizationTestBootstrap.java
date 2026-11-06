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

package org.apache.celeborn.common.network.security;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.channel.Channel;

import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.sasl.SaslClientBootstrap;
import org.apache.celeborn.common.network.sasl.SaslCredentials;
import org.apache.celeborn.common.network.sasl.SaslServerBootstrap;
import org.apache.celeborn.common.network.sasl.SecretRegistryImpl;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;

/** Adapts existing SASL authentication to a connection policy for transport integration tests. */
public class AuthorizationTestBootstrap
    implements TransportClientBootstrap, TransportServerBootstrap {
  private static final String SECRET = "test-policy-secret";
  private static final ConcurrentHashMap<String, BlockingQueue<AuthorizationRequest>> REQUESTS =
      new ConcurrentHashMap<>();
  private final TransportConf conf;

  public AuthorizationTestBootstrap(TransportConf conf) {
    this.conf = conf;
  }

  public static BlockingQueue<AuthorizationRequest> requests(String serverId) {
    return REQUESTS.computeIfAbsent(serverId, ignored -> new LinkedBlockingQueue<>());
  }

  public static void clear(String serverId) {
    REQUESTS.remove(serverId);
  }

  @Override
  public void doBootstrap(TransportClient client) {
    String identity = conf.getCelebornConf().get("celeborn.test.security.token");
    new SaslClientBootstrap(conf, identity, new SaslCredentials(identity, SECRET))
        .doBootstrap(client);
  }

  @Override
  public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler delegate) {
    SecretRegistryImpl registry = new SecretRegistryImpl();
    for (String identity : Arrays.asList("application", "service", "foreign", "denied")) {
      registry.register(identity, SECRET);
    }
    BaseMessageHandler policyHandler =
        new BaseMessageHandler() {
          private boolean installed;

          private void bind(TransportClient client) {
            if (installed) {
              return;
            }
            // Only the completed SASL layer forwards to this handler. Snapshot its verified
            // subject.
            String identity = client.getClientId();
            String prefix = identity.equals("foreign") ? "foreign/" : "tenant/";
            String serverId = conf.getCelebornConf().get("celeborn.test.security.serverId");
            Set<String> deniedOperations =
                new HashSet<>(
                    Arrays.asList(
                        conf.getCelebornConf()
                            .get("celeborn.test.security.deniedOperations", "")
                            .split(",")));
            client.setSecurityContext(
                request -> {
                  requests(serverId).add(request);
                  boolean allowed =
                      !identity.equals("denied")
                          && !deniedOperations.contains(request.getOperation());
                  if (request.getScope() == AuthorizationRequest.Scope.SERVICE) {
                    allowed &= identity.equals("service");
                  } else {
                    allowed &=
                        !identity.equals("service")
                            && (request.getApplicationId() == null
                                || request.getApplicationId().startsWith(prefix));
                  }
                  if (!allowed) {
                    throw new SecurityException("Denied " + request.getOperation());
                  }
                });
            installed = true;
            delegate.channelActive(client);
          }

          @Override
          public boolean checkRegistered() {
            return delegate.checkRegistered();
          }

          @Override
          public void receive(
              TransportClient client, RequestMessage message, RpcResponseCallback callback) {
            bind(client);
            delegate.receive(client, message, callback);
          }

          @Override
          public void receive(TransportClient client, RequestMessage message) {
            bind(client);
            delegate.receive(client, message);
          }

          @Override
          public void channelInactive(TransportClient client) {
            delegate.channelInactive(client);
          }

          @Override
          public void exceptionCaught(Throwable cause, TransportClient client) {
            delegate.exceptionCaught(cause, client);
          }
        };
    return new SaslServerBootstrap(conf, registry).doBootstrap(channel, policyHandler);
  }
}
