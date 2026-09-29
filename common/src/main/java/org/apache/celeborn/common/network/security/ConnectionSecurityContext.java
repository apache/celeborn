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

import javax.annotation.Nullable;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.TransportClient;

/**
 * A connection's mechanism-specific authenticated identity and authorization policy, supplied by
 * its owning bootstrap.
 *
 * <p>Keep the mechanism's verified principal or session in this implementation's private state.
 * Celeborn does not prescribe that identity's type or require it to equal the native application ID
 * returned by {@link TransportClient#getClientId()}. The resolved {@link UserIdentifier} is the
 * identity projection used by Celeborn for authorization, ownership and quota.
 *
 * <p>Install this context once, after its owner authenticates the peer and before forwarding
 * business traffic. Implementations must support concurrent calls and must not change the bound
 * identity after publication. Multiple authentication layers designate one context owner and
 * coordinate their evidence through that owner; they must not replace an installed context.
 * Installing a context does not complete another layer's handshake, including native SASL.
 *
 * <p>This context replaces the native authorization policy. The framework does not implicitly
 * require equal principal strings or combine authorization policies. Enforce any required
 * association between the mechanism's identity and a native application binding in the plugin's
 * policy before protected operations execute. A plugin can explicitly invoke {@link
 * TransportClient#checkNativeAuthorization(AuthorizationRequest)} when it also requires the native
 * authorization rules.
 */
public interface ConnectionSecurityContext {
  /** Reject unauthorized or unrecognized operations with SecurityException. */
  void authorize(AuthorizationRequest request);

  /**
   * Resolves the user used for authorization, ownership and quota from the bound session and the
   * request's claim. Celeborn calls this once, then authorizes and executes with that same user.
   * The default preserves native behavior, which does not authenticate the claimed quota user.
   * Mechanisms which bind a user override this method. The returned fields are only as trustworthy
   * as the checks performed by that mechanism; they are not universally verified identity claims.
   * applicationId and claimed may be null when the request carries no corresponding resource.
   */
  @Nullable
  default UserIdentifier resolveUserIdentifier(
      @Nullable String applicationId, @Nullable UserIdentifier claimed) {
    return claimed;
  }
}
