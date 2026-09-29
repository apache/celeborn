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

package example.security;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import io.netty.channel.Channel;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.security.AuthorizationRequest;
import org.apache.celeborn.common.network.security.ConnectionSecurityContext;
import org.apache.celeborn.common.network.security.SecurityOperation;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.AbstractAuthRpcHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

/** Test-only token verifier with a private connection identity and authorization policy. */
public class TokenBootstrap implements TransportClientBootstrap, TransportServerBootstrap {
  private final TransportConf conf;
  private final String challenge;
  private final boolean contextOwner;

  public TokenBootstrap(TransportConf conf) {
    this(conf, "TOKEN", true);
  }

  private TokenBootstrap(TransportConf conf, String challenge, boolean contextOwner) {
    this.conf = conf;
    this.challenge = challenge;
    this.contextOwner = contextOwner;
  }

  public static class First extends TokenBootstrap {
    public First(TransportConf conf) {
      super(conf, "FIRST", true);
    }
  }

  public static class Second extends TokenBootstrap {
    public Second(TransportConf conf) {
      super(conf, "SECOND", false);
    }
  }

  private void record(String side) {
    String key = "celeborn.test.token." + side + ".completed";
    String previous = conf.getCelebornConf().get(key, "");
    conf.getCelebornConf().set(key, previous.isEmpty() ? challenge : previous + "," + challenge);
  }

  @Override
  public void doBootstrap(TransportClient client) {
    try {
      // Credentials may rotate between connections; never cache their contents in the bootstrap.
      String token =
          new String(
                  Files.readAllBytes(
                      Paths.get(conf.getCelebornConf().get("celeborn.test.token.file"))),
                  StandardCharsets.UTF_8)
              .trim();
      String response =
          JavaUtils.bytesToString(
              client.sendRpcSync(JavaUtils.stringToBytes(challenge + " " + token), 10000));
      if (!"OK".equals(response)) {
        throw new SecurityException("Token authentication failed");
      }
      record("client");
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
        // Delay activation until authentication. Inactive/exception delegation stays inherited.
      }

      @Override
      protected boolean doAuthChallenge(
          TransportClient client, RequestMessage message, RpcResponseCallback callback) {
        try {
          String request = JavaUtils.bytesToString(message.body().nioByteBuffer());
          String prefix = challenge + " ";
          if (!request.startsWith(prefix)) {
            callback.onFailure(new SecurityException("Invalid token"));
            return false;
          }
          String key = "celeborn.test.token." + request.substring(prefix.length());
          String appId = conf.getCelebornConf().get(key + ".appId", "");
          String tenant = conf.getCelebornConf().get(key + ".tenant", "");
          String principal = conf.getCelebornConf().get(key + ".principal", "");
          if (appId.isEmpty() || tenant.isEmpty() || principal.isEmpty()) {
            callback.onFailure(new SecurityException("Invalid token"));
            return false;
          }
          // Retain verified identity and grants in our context; clientId belongs to native auth.
          TokenSecurityContext verified =
              new TokenSecurityContext(
                  client,
                  appId,
                  new UserIdentifier(tenant, principal),
                  conf.getCelebornConf().get("celeborn.test.token.policy", "application"));
          if (contextOwner) {
            client.setSecurityContext(verified);
          } else {
            // FIRST owns the context. SECOND verifies the same identity without replacing it.
            ConnectionSecurityContext installed = client.getSecurityContext();
            if (!(installed instanceof TokenSecurityContext)
                || !((TokenSecurityContext) installed).sameIdentity(verified)) {
              callback.onFailure(new SecurityException("Conflicting token identity or missing owner"));
              return false;
            }
          }
          delegate.channelActive(client);
          record("server");
          callback.onSuccess(JavaUtils.stringToBytes("OK"));
          return true;
        } catch (IOException e) {
          callback.onFailure(e);
          return false;
        }
      }
    };
  }

  private static final class TokenSecurityContext implements ConnectionSecurityContext {
    private final TransportClient client;
    private final String applicationId;
    private final UserIdentifier user;
    private final String policy;

    private TokenSecurityContext(
        TransportClient client, String applicationId, UserIdentifier user, String policy) {
      if (!"application".equals(policy)
          && !"tenant".equals(policy)
          && !"deny-registration".equals(policy)) {
        throw new IllegalArgumentException("Unknown token policy: " + policy);
      }
      this.client = client;
      this.applicationId = applicationId;
      this.user = user;
      this.policy = policy;
    }

    private boolean sameIdentity(TokenSecurityContext other) {
      return user.equals(other.user)
          && applicationId.equals(other.applicationId)
          && policy.equals(other.policy);
    }

    @Override
    public UserIdentifier resolveUserIdentifier(String target, UserIdentifier claimed) {
      return user;
    }

    @Override
    public void authorize(AuthorizationRequest request) {
      boolean registration = SecurityOperation.REGISTER_APPLICATION.equals(request.getOperation());
      // Authentication handlers gate business traffic. A non-null native ID alone proves nothing.
      String nativeApplication = client.getClientId();
      if (request.getScope() != AuthorizationRequest.Scope.APPLICATION
          || !(registration || SecurityOperation.APPLICATION_ACCESS.equals(request.getOperation()))
          || (registration && "deny-registration".equals(policy))
          || !allowsApplication(request.getApplicationId())
          || (nativeApplication != null && !allowsApplication(nativeApplication))
          || request.getUserIdentifier() != user) {
        throw new SecurityException("Denied by token policy");
      }
    }

    private boolean allowsApplication(String target) {
      return target != null
          && ("tenant".equals(policy)
              ? target.startsWith(user.tenantId() + "/")
              : applicationId.equals(target));
    }
  }

  @Override
  public void close() {}
}
