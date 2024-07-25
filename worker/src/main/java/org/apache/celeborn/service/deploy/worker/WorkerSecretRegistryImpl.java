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

package org.apache.celeborn.service.deploy.worker;

import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.protocol.PbApplicationMeta;
import org.apache.celeborn.common.protocol.PbApplicationMetaRequest;

/** A secret registry that fetches the secret from the master if it is not found locally. */
public class WorkerSecretRegistryImpl implements SecretRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerSecretRegistryImpl.class);

  private final int maxCacheSize;
  // MasterClient is created in Worker after the secret registry is created and this order currently
  // cannot be changed.
  // So, we need to set the masterClient after the secret registry is created.
  private MasterClient masterClient;
  private LoadingCache<String, String> secretCache;

  public WorkerSecretRegistryImpl(int maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
  }

  public void initialize(MasterClient masterClient) {
    this.masterClient = Preconditions.checkNotNull(masterClient);
    CacheLoader<String, String> cacheLoader =
        new CacheLoader<String, String>() {
          @Override
          public String load(String appId) throws Exception {
            LOG.debug("Missing the secret for {}; fetching it from the master", appId);
            PbApplicationMetaRequest pbApplicationMetaRequest =
                PbApplicationMetaRequest.newBuilder().setAppId(appId).build();
            try {
              PbApplicationMeta pbApplicationMeta =
                  masterClient.askSync(pbApplicationMetaRequest, PbApplicationMeta.class);
              LOG.debug(
                  "Successfully fetched the application meta info for "
                      + appId
                      + " from the master");
              return pbApplicationMeta.getSecret();
            } catch (Throwable e) {
              // We catch Throwable here because masterClient.askSync declares it in its definition.
              LOG.error(
                  "Failed to fetch the application meta info for {} from the master", appId, e);
              throw new Exception(e);
            }
          }
        };
    secretCache = CacheBuilder.newBuilder().maximumSize(maxCacheSize).build(cacheLoader);
  }

  /** Gets an appropriate SASL secret key for the given appId. */
  @Override
  public String getSecretKey(String appId) {
    try {
      return secretCache.get(appId);
    } catch (ExecutionException ee) {
      LOG.error("Failed to retrieve the the application meta info for {} ", appId, ee);
    }
    return null;
  }

  @Override
  public boolean isRegistered(String appId) {
    return secretCache.getIfPresent(appId) != null;
  }

  @Override
  public void register(String appId, String secret) {
    String existingSecret = secretCache.getIfPresent(appId);
    if (existingSecret != null && !existingSecret.equals(secret)) {
      throw new IllegalArgumentException(
          "AppId " + appId + " is already registered. Cannot re-register.");
    }
    if (existingSecret == null) {
      secretCache.put(appId, secret);
    }
  }

  @Override
  public void unregister(String appId) {
    secretCache.invalidate(appId);
  }
}
