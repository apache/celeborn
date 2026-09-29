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

/**
 * A connection's authenticated identity and authorization policy, supplied by its bootstrap.
 * Capture the identity established by the mechanism in this object and install it before forwarding
 * business traffic. Implementations must support concurrent calls and must not change the bound
 * identity after publication. This context replaces the native authorization policy; authentication
 * layers, including native SASL when enabled, still have to finish independently.
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
