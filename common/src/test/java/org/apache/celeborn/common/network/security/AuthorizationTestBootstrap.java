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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.channel.Channel;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.AbstractAuthRpcHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

/** Test identities and policy decisions for real-transport authorization integration tests. */
public class AuthorizationTestBootstrap
    implements TransportClientBootstrap, TransportServerBootstrap {
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
    try {
      String token = conf.getCelebornConf().get("celeborn.test.security.token");
      String result =
          JavaUtils.bytesToString(client.sendRpcSync(JavaUtils.stringToBytes(token), 10000));
      if (!"OK".equals(result)) {
        throw new SecurityException("Test authentication failed");
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler delegate) {
    return new AbstractAuthRpcHandler(delegate) {
      @Override
      public boolean checkRegistered() {
        return delegate.checkRegistered();
      }

      @Override
      public void channelActive(TransportClient client) {
        // Test handshake completion activates the delegate; inherited inactive/error hooks remain.
      }

      @Override
      protected boolean doAuthChallenge(
          TransportClient client, RequestMessage message, RpcResponseCallback callback) {
        try {
          String token = JavaUtils.bytesToString(message.body().nioByteBuffer());
          if (!Arrays.asList("application", "service", "foreign", "missing-user", "denied")
              .contains(token)) {
            callback.onFailure(new SecurityException("Unknown test identity"));
            return false;
          }
          String prefix = token.equals("foreign") ? "foreign/" : "tenant/";
          UserIdentifier effectiveUser =
              token.equals("missing-user")
                  ? null
                  : new UserIdentifier(token.equals("foreign") ? "foreign" : "tenant", "alice");
          String serverId = conf.getCelebornConf().get("celeborn.test.security.serverId");
          Set<String> deniedOperations =
              new HashSet<>(
                  Arrays.asList(
                      conf.getCelebornConf()
                          .get("celeborn.test.security.deniedOperations", "")
                          .split(",")));
          // The native binding deliberately differs from permitted tenant application IDs.
          String nativeApp =
              conf.getCelebornConf()
                  .get("celeborn.test.security.nativeApplicationId", "native-" + token);
          if (!nativeApp.isEmpty()) {
            client.setClientId(nativeApp);
          }
          client.setSecurityContext(
              new ConnectionSecurityContext() {
                @Override
                public UserIdentifier resolveUserIdentifier(
                    String applicationId, UserIdentifier claimed) {
                  return effectiveUser;
                }

                @Override
                public void authorize(AuthorizationRequest request) {
                  requests(serverId).add(request);
                  boolean allowed =
                      !token.equals("denied") && !deniedOperations.contains(request.getOperation());
                  if (request.getScope() == AuthorizationRequest.Scope.SERVICE) {
                    allowed &= token.equals("service");
                  } else {
                    allowed &=
                        !token.equals("service")
                            && (request.getApplicationId() == null
                                || request.getApplicationId().startsWith(prefix));
                  }
                  if (!allowed) {
                    throw new SecurityException("Denied " + request.getOperation());
                  }
                }
              });
          delegate.channelActive(client);
          callback.onSuccess(JavaUtils.stringToBytes("OK"));
          return true;
        } catch (IOException e) {
          callback.onFailure(e);
          return false;
        }
      }
    };
  }

  @Override
  public void close() {}
}
