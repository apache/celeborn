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

import java.util.concurrent.ConcurrentHashMap;

/**
 * A simple implementation of {@link SecretRegistry} that stores secrets in memory. It is designed
 * as a singleton.
 */
public class SecretRegistryImpl implements SecretRegistry {

  private static final SecretRegistryImpl INSTANCE = new SecretRegistryImpl();

  public static SecretRegistryImpl getInstance() {
    return INSTANCE;
  }

  private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();

  public void register(String appId, String secret) {
    secrets.put(appId, secret);
  }

  public void unregister(String appId) {
    secrets.remove(appId);
  }

  @Override
  public boolean isRegistered(String appId) {
    return secrets.containsKey(appId);
  }

  /** Gets an appropriate SASL secret key for the given appId. */
  @Override
  public String getSecretKey(String appId) {
    return secrets.get(appId);
  }
}
