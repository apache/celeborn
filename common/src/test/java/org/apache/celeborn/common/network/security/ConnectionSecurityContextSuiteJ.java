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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;
import org.apache.celeborn.common.network.protocol.OneWayMessage;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.server.AbstractAuthRpcHandler;
import org.apache.celeborn.common.network.server.BaseMessageHandler;

public class ConnectionSecurityContextSuiteJ {
  private final EmbeddedChannel channel = new EmbeddedChannel();
  private final TransportClient client =
      new TransportClient(channel, mock(TransportResponseHandler.class));

  @After
  public void close() {
    channel.finishAndReleaseAll();
  }

  @Test
  public void nativePolicyPreservesApplicationIsolationAndInternalServiceAccess() {
    AuthorizationRequest service =
        AuthorizationRequest.forService(SecurityOperation.REGISTER_WORKER);
    client.authorize(service);
    client.setClientId("tenant/application");
    client.authorize(
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/application"));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(
                    SecurityOperation.PUSH_DATA, "other/application")));
    assertThrows(SecurityException.class, () -> client.authorize(service));
    client.authorize(
        AuthorizationRequest.forService(
            SecurityOperation.GET_APPLICATION_META, "tenant/application"));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forService(
                    SecurityOperation.GET_APPLICATION_META, "other/application")));
  }

  @Test
  public void tenantPolicyCanReplaceNativeApplicationEquality() {
    client.setClientId("tenant/first");
    client.setSecurityContext(
        request -> {
          if (request.getScope() != AuthorizationRequest.Scope.APPLICATION
              || !SecurityOperation.PUSH_DATA.equals(request.getOperation())
              || request.getApplicationId() == null
              || !request.getApplicationId().startsWith("tenant/")) {
            throw new SecurityException("Denied by tenant policy");
          }
        });
    client.authorize(
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/second"));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "different/app")));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication("FUTURE_OPERATION", "tenant/second")));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forService(SecurityOperation.PUSH_DATA, "tenant/first")));
  }

  @Test
  public void pluginCanExplicitlyComposeNativeAuthorization() {
    client.setClientId("tenant/first");
    AtomicInteger allowed = new AtomicInteger();
    client.setSecurityContext(
        request -> {
          client.checkNativeAuthorization(request);
          allowed.incrementAndGet();
        });
    client.authorize(
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/first"));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/second")));
    assertEquals(1, allowed.get());
  }

  @Test
  public void effectiveUserIsResolvedOnceAndSharedWithPolicyAndCaller() {
    UserIdentifier claimed = new UserIdentifier("untrusted", "claim");
    UserIdentifier effective = new UserIdentifier("tenant", "authenticated-user");
    AtomicInteger resolutions = new AtomicInteger();
    AtomicReference<AuthorizationRequest> checked = new AtomicReference<>();
    client.setSecurityContext(
        new ConnectionSecurityContext() {
          @Override
          public UserIdentifier resolveUserIdentifier(String applicationId, UserIdentifier user) {
            assertEquals("tenant/app", applicationId);
            assertSame(claimed, user);
            resolutions.incrementAndGet();
            return effective;
          }

          @Override
          public void authorize(AuthorizationRequest request) {
            checked.set(request);
          }
        });
    AuthorizationRequest request =
        AuthorizationRequest.forApplication(SecurityOperation.CHECK_QUOTA, "tenant/app", claimed);
    assertSame(effective, client.authorize(request));
    assertSame(effective, checked.get().getUserIdentifier());
    assertSame(claimed, request.getUserIdentifier());
    assertEquals(1, resolutions.get());
  }

  @Test
  public void nativeApplicationBindingDoesNotReplacePluginIdentityOrPolicy() {
    UserIdentifier externalUser = new UserIdentifier("tenant", "alice");
    ConnectionSecurityContext context =
        new ConnectionSecurityContext() {
          @Override
          public UserIdentifier resolveUserIdentifier(
              String applicationId, UserIdentifier claimed) {
            return externalUser;
          }

          @Override
          public void authorize(AuthorizationRequest request) {
            assertSame(externalUser, request.getUserIdentifier());
            if (!"tenant/resource".equals(request.getApplicationId())) {
              throw new SecurityException("Outside the plugin's grant");
            }
          }
        };
    AuthorizationRequest request =
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/resource");
    client.setSecurityContext(context);
    assertNull(client.getClientId());
    assertSame(externalUser, client.authorize(request));

    client.setClientId("native/application");
    assertSame(context, client.getSecurityContext());
    assertEquals("native/application", client.getClientId());
    assertSame(externalUser, client.authorize(request));
    // Native equality would reject this target. The plugin policy is selected explicitly instead.
    assertThrows(SecurityException.class, () -> client.checkNativeAuthorization(request));
    assertThrows(IllegalStateException.class, () -> client.setClientId("different/application"));
    assertSame(context, client.getSecurityContext());
    assertSame(externalUser, client.authorize(request));
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(
                    SecurityOperation.PUSH_DATA, "other/resource")));
  }

  @Test
  public void boundIdentitiesDoNotCompleteAuthenticationLayers() {
    client.setClientId("native/application");
    client.setSecurityContext(request -> {});
    BaseMessageHandler delegate = mock(BaseMessageHandler.class);
    AtomicBoolean outerComplete = new AtomicBoolean();
    AtomicBoolean innerComplete = new AtomicBoolean();
    AbstractAuthRpcHandler inner = authHandler(delegate, innerComplete);
    AbstractAuthRpcHandler outer = authHandler(inner, outerComplete);
    RpcResponseCallback callback = mock(RpcResponseCallback.class);
    NioManagedBuffer empty = new NioManagedBuffer(ByteBuffer.allocate(0));
    RpcRequest handshake = new RpcRequest(1L, empty);
    RpcRequest business = new RpcRequest(2L, empty);
    OneWayMessage oneWay = new OneWayMessage(empty);

    outer.receive(client, handshake, callback);
    assertFalse(outer.isAuthenticated());
    assertThrows(SecurityException.class, () -> outer.receive(client, oneWay));
    verifyNoInteractions(delegate);

    outerComplete.set(true);
    outer.receive(client, handshake, callback);
    assertTrue(outer.isAuthenticated());
    assertFalse(inner.isAuthenticated());
    outer.receive(client, handshake, callback);
    assertThrows(SecurityException.class, () -> outer.receive(client, oneWay));
    verifyNoInteractions(delegate);

    innerComplete.set(true);
    outer.receive(client, handshake, callback);
    assertTrue(inner.isAuthenticated());
    // Completing a handshake consumes that frame; only subsequent business frames are delegated.
    verifyNoInteractions(delegate);
    outer.receive(client, business, callback);
    outer.receive(client, oneWay);
    verify(delegate).receive(client, business, callback);
    verify(delegate).receive(client, oneWay);
    verifyNoMoreInteractions(delegate);
  }

  private static AbstractAuthRpcHandler authHandler(
      BaseMessageHandler delegate, AtomicBoolean complete) {
    return new AbstractAuthRpcHandler(delegate) {
      @Override
      protected boolean doAuthChallenge(
          TransportClient client, RequestMessage message, RpcResponseCallback callback) {
        callback.onSuccess(ByteBuffer.allocate(0));
        return complete.get();
      }
    };
  }

  @Test
  public void missingResolvedUserCannotFallBackToUntrustedClaim() {
    AtomicInteger checks = new AtomicInteger();
    client.setSecurityContext(
        new ConnectionSecurityContext() {
          @Override
          public UserIdentifier resolveUserIdentifier(String applicationId, UserIdentifier user) {
            return null;
          }

          @Override
          public void authorize(AuthorizationRequest request) {
            checks.incrementAndGet();
          }
        });
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(
                    SecurityOperation.CHECK_QUOTA, null, new UserIdentifier("claim", "user"))));
    assertEquals(0, checks.get());
  }

  @Test
  public void connectionIdentityCannotBeReplacedAfterPublication() {
    ConnectionSecurityContext identity = request -> {};
    assertThrows(NullPointerException.class, () -> client.setSecurityContext(null));
    client.setSecurityContext(identity);
    assertSame(identity, client.getSecurityContext());
    assertThrows(IllegalStateException.class, () -> client.setSecurityContext(identity));
    assertThrows(IllegalStateException.class, () -> client.setSecurityContext(request -> {}));
    assertSame(identity, client.getSecurityContext());
  }
}
