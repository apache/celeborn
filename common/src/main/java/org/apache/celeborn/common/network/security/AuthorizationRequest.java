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

import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.celeborn.common.identity.UserIdentifier;

/** A server-derived operation and its target, independent of the request's reply semantics. */
public final class AuthorizationRequest {
  public enum Scope {
    APPLICATION,
    SERVICE
  }

  private final String operation;
  private final Scope scope;
  @Nullable private final String applicationId;
  @Nullable private final UserIdentifier userIdentifier;

  private AuthorizationRequest(
      String operation, Scope scope, String applicationId, UserIdentifier userIdentifier) {
    this.operation = Objects.requireNonNull(operation, "operation");
    if (operation.isEmpty()) {
      throw new IllegalArgumentException("An authorization operation must not be empty");
    }
    this.scope = Objects.requireNonNull(scope, "scope");
    this.applicationId = applicationId;
    this.userIdentifier = userIdentifier;
  }

  public static AuthorizationRequest forApplication(String operation, String applicationId) {
    return forApplication(operation, applicationId, null);
  }

  /**
   * applicationId may be null for legacy requests, such as quota queries, without an app target.
   */
  public static AuthorizationRequest forApplication(
      String operation, String applicationId, UserIdentifier userIdentifier) {
    return new AuthorizationRequest(operation, Scope.APPLICATION, applicationId, userIdentifier);
  }

  public static AuthorizationRequest forService(String operation) {
    return forService(operation, null);
  }

  public static AuthorizationRequest forService(String operation, String applicationId) {
    return new AuthorizationRequest(operation, Scope.SERVICE, applicationId, null);
  }

  public String getOperation() {
    return operation;
  }

  public Scope getScope() {
    return scope;
  }

  @Nullable
  public String getApplicationId() {
    return applicationId;
  }

  @Nullable
  public UserIdentifier getUserIdentifier() {
    return userIdentifier;
  }

  public AuthorizationRequest withUserIdentifier(UserIdentifier user) {
    return new AuthorizationRequest(operation, scope, applicationId, user);
  }
}
