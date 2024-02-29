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

import org.apache.celeborn.common.network.sasl.SecretRegistry;
import org.apache.celeborn.common.network.sasl.SecretRegistryImpl;
import org.apache.celeborn.service.deploy.master.clustermeta.IMetadataHandler;

/**
 * A simple implementation of {@link SecretRegistry} that stores secrets in memory and Ratis. This
 * persists an application secret in Ratis but the deletion of that secret happens when
 * ApplicationLost is triggered.
 */
public class MasterSecretRegistryImpl extends SecretRegistryImpl {

  private static final Logger LOG = LoggerFactory.getLogger(MasterSecretRegistryImpl.class);
  private IMetadataHandler metadataHandler;

  @Override
  public void register(String appId, String secret) {
    super.register(appId, secret);
    if (metadataHandler != null) {
      LOG.info("Persisting metadata for appId: {}", appId);
      metadataHandler.handleApplicationMeta(appId, secret);
    }
  }

  void setMetadataHandler(IMetadataHandler metadataHandler) {
    this.metadataHandler = metadataHandler;
  }
}
