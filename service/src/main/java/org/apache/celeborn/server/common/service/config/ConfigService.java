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

import org.apache.celeborn.common.CelebornConf;

public interface ConfigService {

  CelebornConf getCelebornConf();

  SystemConfig getSystemConfigFromCache();

  TenantConfig getRawTenantConfigFromCache(String tenantId);

  default DynamicConfig getTenantConfigFromCache(String tenantId) {
    TenantConfig tenantConfig = getRawTenantConfigFromCache(tenantId);
    if (tenantConfig == null || tenantConfig.getConfigs().isEmpty()) {
      return getSystemConfigFromCache();
    } else {
      return tenantConfig;
    }
  }

  TenantConfig getRawTenantUserConfig(String tenantId, String userId);

  default DynamicConfig getTenantUserConfig(String tenantId, String userId) {
    TenantConfig tenantConfig = getRawTenantUserConfig(tenantId, userId);
    if (tenantConfig == null) {
      return getTenantConfigFromCache(tenantId);
    } else {
      return tenantConfig;
    }
  }

  void refreshAllCache() throws IOException;

  void shutdown();
}
