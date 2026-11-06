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
  public void policyReceivesOriginalResourceAndUserClaims() {
    UserIdentifier claimed = new UserIdentifier("untrusted", "claim");
    AtomicReference<AuthorizationRequest> checked = new AtomicReference<>();
    client.setSecurityContext(checked::set);
    AuthorizationRequest request =
        AuthorizationRequest.forApplication(SecurityOperation.CHECK_QUOTA, null, claimed);
    client.authorize(request);
    assertSame(request, checked.get());
    assertNull(checked.get().getApplicationId());
    assertSame(claimed, checked.get().getUserIdentifier());
  }

  @Test
  public void nativeApplicationBindingDoesNotReplacePluginIdentityOrPolicy() {
    ConnectionSecurityContext context =
        request -> {
          if (!"tenant/resource".equals(request.getApplicationId())) {
            throw new SecurityException("Outside the plugin's grant");
          }
        };
    AuthorizationRequest request =
        AuthorizationRequest.forApplication(SecurityOperation.PUSH_DATA, "tenant/resource");
    client.setSecurityContext(context);
    assertNull(client.getClientId());
    client.authorize(request);

    client.setClientId("native/application");
    assertSame(context, client.getSecurityContext());
    assertEquals("native/application", client.getClientId());
    client.authorize(request);
    // Native equality would reject this target. The plugin policy is selected explicitly instead.
    assertThrows(SecurityException.class, () -> client.checkNativeAuthorization(request));
    assertThrows(IllegalStateException.class, () -> client.setClientId("different/application"));
    assertSame(context, client.getSecurityContext());
    client.authorize(request);
    assertThrows(
        SecurityException.class,
        () ->
            client.authorize(
                AuthorizationRequest.forApplication(
                    SecurityOperation.PUSH_DATA, "other/resource")));
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
