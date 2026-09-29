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
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportResponseHandler;

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
