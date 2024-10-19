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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.ApplicationMeta;
import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.sasl.SecretRegistryImpl;
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager;

/**
 * A simple implementation of {@link SecretRegistry} that stores secrets in memory and Ratis. This
 * persists an application secret in Ratis but the deletion of that secret happens when
 * ApplicationLost is triggered.
 */
public class MasterSecretRegistryImpl extends SecretRegistryImpl {

  private static final Logger LOG = LoggerFactory.getLogger(MasterSecretRegistryImpl.class);
  private AbstractMetaManager metadataHandler;

  @Override
  public void register(String appId, String secret) {
    super.register(appId, secret);
    if (metadataHandler != null) {
      LOG.info("Persisting metadata for appId: {}", appId);
      metadataHandler.handleApplicationMeta(new ApplicationMeta(appId, secret));
    }
  }

  @Override
  public String getSecretKey(String appId) {
    String secret = super.getSecretKey(appId);
    if (secret == null && metadataHandler != null) {
      LOG.info("Fetching secret from metadata manager for appId: {}", appId);
      ApplicationMeta applicationMeta = metadataHandler.applicationMetas.get(appId);
      if (applicationMeta != null) {
        secret = applicationMeta.secret();
        LOG.debug("Found Secret from metadata manager for appId: {}, register secret", appId);
        super.register(appId, secret);
      } else {
        LOG.warn("Secret not found from metadata manager for appId: {}", appId);
      }
    }
    return secret;
  }

  @Override
  public boolean isRegistered(String appId) {
    boolean isRegistered = super.isRegistered(appId);
    if (!isRegistered && metadataHandler != null) {
      LOG.info("Fetching registration status from metadata manager for appId: {}", appId);
      isRegistered = metadataHandler.applicationMetas.containsKey(appId);
      if (isRegistered) {
        LOG.debug("Found Secret from metadata manager for appId: {}, register secret", appId);
        super.register(appId, metadataHandler.applicationMetas.get(appId).secret());
      }
    }
    return isRegistered;
  }

  void setMetadataHandler(AbstractMetaManager metadataHandler) {
    this.metadataHandler = metadataHandler;
  }
}
