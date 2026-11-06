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

import org.apache.celeborn.common.network.client.TransportClient;

/**
 * A connection's mechanism-specific authenticated identity and authorization policy, supplied by
 * its owning bootstrap.
 *
 * <p>Keep the mechanism's verified principal or session in this implementation's private state.
 * Celeborn does not prescribe that identity's type or require it to equal the native application ID
 * returned by {@link TransportClient#getClientId()}. A request's user identifier is a client claim;
 * this interface does not resolve or replace the user used for resource ownership and quota.
 *
 * <p>Install this context once, after its owner authenticates the peer and before forwarding
 * business traffic. Implementations must support concurrent calls and must not change the bound
 * identity after publication. Multiple authentication layers designate one context owner and
 * coordinate their evidence through that owner; they must not replace an installed context.
 * Installing a context does not complete another layer's handshake, including native SASL.
 *
 * <p>This context replaces the native connection policy used by {@link
 * TransportClient#authorize(AuthorizationRequest)}. It does not replace host-enforced RPC
 * entry-point restrictions or the resource facts supplied by receiving handlers. With native
 * authentication enabled, the Master's external RPC allowlist applies before this policy; a plugin
 * cannot permit service operations outside that allowlist through the external listener.
 *
 * <p>The framework does not implicitly require equal principal strings or combine authorization
 * policies. Enforce any required association between the mechanism's identity and a native
 * application binding in the plugin's policy before protected operations execute. A plugin can
 * explicitly invoke {@link TransportClient#checkNativeAuthorization(AuthorizationRequest)} when it
 * also requires the native authorization rules.
 */
public interface ConnectionSecurityContext {
  /** Reject unauthorized or unrecognized operations with SecurityException. */
  void authorize(AuthorizationRequest request);
}
