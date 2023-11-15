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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Lists;

public class SecretRegistryImpl implements SecretRegistry {

  private static final SecretRegistryImpl INSTANCE = new SecretRegistryImpl();

  public static SecretRegistryImpl getInstance() {
    return INSTANCE;
  }

  private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();

  public void registerApplication(String appId, String secret) {
    secrets.put(appId, secret);
  }

  public void unregisterApplication(String appId) {
    secrets.remove(appId);
  }

  public String getSecret(String appId) {
    return secrets.get(appId);
  }

  public boolean isRegistered(String appId) {
    return secrets.containsKey(appId);
  }

  public List<String> getAllRegisteredApps() {
    return Lists.newArrayList(secrets.keySet());
  }

  /**
   * Gets an appropriate SASL User for the given appId.
   *
   * @throws IllegalArgumentException if the given appId is not associated with a SASL user.
   */
  @Override
  public String getSaslUser(String appId) {
    return appId;
  }

  /**
   * Gets an appropriate SASL secret key for the given appId.
   *
   * @throws IllegalArgumentException if the given appId is not associated with a SASL secret key.
   */
  @Override
  public String getSecretKey(String appId) {
    return secrets.get(appId);
  }
}
