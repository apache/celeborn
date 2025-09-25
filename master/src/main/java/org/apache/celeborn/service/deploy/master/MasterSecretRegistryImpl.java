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

package org.apache.celeborn.service.deploy.master;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAHelper;

/**
 * A simple implementation of {@link SecretRegistry} that stores secrets in Ratis. This persists an
 * application secret in Ratis but the deletion of that secret happens when ApplicationLost is
 * triggered.
 */
public class MasterSecretRegistryImpl implements SecretRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(MasterSecretRegistryImpl.class);
  private final boolean bindPreferIP;
  private AbstractMetaManager statusSystem;

  public MasterSecretRegistryImpl(boolean bindPreferIP) {
    this.bindPreferIP = bindPreferIP;
  }

  @Override
  public void register(String appId, String secret) {
    LOG.info("Persisting metadata for appId: {}", appId);
    statusSystem.handleApplicationMeta(new ApplicationMeta(appId, secret));
  }

  @Override
  public void unregister(String appId) {
    LOG.info("Removing metadata for appId: {}", appId);
    statusSystem.removeApplicationMeta(appId);
  }

  @Override
  public String getSecretKey(String appId) {
    String secret = null;
    LOG.debug("Fetching secret from metadata manager for appId: {}", appId);
    ApplicationMeta applicationMeta = statusSystem.applicationMetas.get(appId);
    if (applicationMeta != null) {
      secret = applicationMeta.secret();
    }
    return secret;
  }

  @Override
  public boolean isRegistered(String appId) {
    LOG.info("Fetching registration status from metadata manager for appId: {}", appId);
    return statusSystem.applicationMetas.containsKey(appId);
  }

  @Override
  public boolean registrationEnabled() {
    return registrationEnabledImpl(statusSystem, bindPreferIP);
  }

  public static boolean registrationEnabledImpl(
      AbstractMetaManager statusSystem, boolean bindPreferIP) {
    try {
      return HAHelper.checkShouldProcess(
          ex -> {
            throw ex;
          },
          statusSystem,
          bindPreferIP);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void setMetadataHandler(AbstractMetaManager statusSystem) {
    this.statusSystem = statusSystem;
  }
}
