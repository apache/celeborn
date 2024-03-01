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

package org.apache.celeborn.common.network.sasl;

import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.meta.ApplicationRegistration;

import java.util.concurrent.ConcurrentHashMap;

/** A simple implementation of {@link ApplicationRegistry} that stores secrets in memory. */
public class ApplicationRegistryImpl implements ApplicationRegistry {

  private final ConcurrentHashMap<String, ApplicationRegistration> appRegistrations =
          new ConcurrentHashMap<>();

  public ConcurrentHashMap<String, ApplicationRegistration> getAppRegistrations() {
    return appRegistrations;
  }

  @Override
  public void register(String appId, UserIdentifier userIdentifier, String secret) {
    // TODO: Persist the secret in ratis. See https://issues.apache.org/jira/browse/CELEBORN-1234
    appRegistrations.compute(
        appId,
        (id, oldVal) -> {
          if (oldVal != null) {
            throw new IllegalArgumentException("AppId " + appId + " is already registered.");
          }
          return new ApplicationRegistration(userIdentifier, secret);
        });
  }

  @Override
  public void unregister(String appId) {
    appRegistrations.remove(appId);
  }

  @Override
  public boolean isRegistered(String appId) {
    return appRegistrations.containsKey(appId);
  }

  /** Gets an appropriate SASL secret key for the given appId. */
  @Override
  public String getSecretKey(String appId) {
    ApplicationRegistration registration = appRegistrations.get(appId);
    return registration == null ? null : registration.secret();
  }

  @Override
  public UserIdentifier getUserIdentifier(String appId) {
    ApplicationRegistration registration = appRegistrations.get(appId);
    return registration == null ? null : registration.userIdentifier();
  }
}
