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

package org.apache.celeborn.server.common.service.config;

import java.io.IOException;
import java.util.List;

import org.apache.celeborn.common.CelebornConf;

/**
 * Config service provides the configuration management service with cache for the static and
 * dynamic configuration(system level/tenant level/tenant user level).
 */
public interface ConfigService {

  /**
   * Gets the static configuration {@link CelebornConf}.
   *
   * @return The static configuration.
   */
  CelebornConf getCelebornConf();

  /**
   * Gets the system level dynamic configuration {@link SystemConfig} from cache.
   *
   * @return The system level dynamic configuration.
   */
  SystemConfig getSystemConfigFromCache();

  /**
   * Lists the raw tenant level dynamic configurations {@link TenantConfig} from cache.
   *
   * @return The raw tenant level dynamic configurations.
   */
  List<TenantConfig> listRawTenantConfigsFromCache();

  /**
   * Gets the raw tenant level dynamic configuration {@link TenantConfig} from cache.
   *
   * @return The raw tenant level dynamic configuration.
   */
  TenantConfig getRawTenantConfigFromCache(String tenantId);

  /**
   * Gets the tenant level dynamic configuration {@link DynamicConfig} from cache. When the tenant
   * level config is null or empty, fallback to the system level config.
   *
   * @return The tenant level dynamic configuration.
   */
  default DynamicConfig getTenantConfigFromCache(String tenantId) {
    TenantConfig tenantConfig = getRawTenantConfigFromCache(tenantId);
    if (tenantConfig == null || tenantConfig.getConfigs().isEmpty()) {
      return getSystemConfigFromCache();
    } else {
      return tenantConfig;
    }
  }

  /**
   * Lists the raw tenant user level dynamic configurations {@link TenantConfig} from cache.
   *
   * @return The raw tenant user level dynamic configurations.
   */
  List<TenantConfig> listRawTenantUserConfigsFromCache();

  /**
   * Gets the raw tenant user level dynamic configuration {@link TenantConfig} from cache.
   *
   * @return The raw tenant user level dynamic configuration.
   */
  TenantConfig getRawTenantUserConfigFromCache(String tenantId, String userId);

  /**
   * Gets the tenant user level dynamic configuration {@link DynamicConfig} from cache. When the
   * tenant user level config is null or empty, fallback to the tenant level config. When the tenant
   * level config is null or empty, fallback to the system level config again.
   *
   * @return The tenant user level dynamic configuration.
   */
  default DynamicConfig getTenantUserConfigFromCache(String tenantId, String userId) {
    TenantConfig tenantConfig = getRawTenantUserConfigFromCache(tenantId, userId);
    if (tenantConfig == null || tenantConfig.getConfigs().isEmpty()) {
      return getTenantConfigFromCache(tenantId);
    } else {
      return tenantConfig;
    }
  }

  /**
   * Refreshes cache of the dynamic configuration(system level/tenant level/tenant user level).
   *
   * @throws IOException If refresh fails with exception.
   */
  void refreshCache() throws IOException;

  /** Shutdowns configuration management service. */
  void shutdown();
}
