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

import java.util.List;
import java.util.Map;

import org.apache.celeborn.server.common.service.model.ClusterTenantConfig;

public class TenantConfig extends DynamicConfig {
  private final String tenantId;
  private final ConfigService configService;
  private final String name;

  public TenantConfig(
      ConfigService configService, String tenantId, String name, Map<String, String> configs) {
    this.configService = configService;
    this.tenantId = tenantId;
    this.name = name;
    this.configs.putAll(configs);
  }

  public TenantConfig(
      ConfigService configService,
      String tenantId,
      String name,
      List<ClusterTenantConfig> tenantConfigs) {
    this.configService = configService;
    this.tenantId = tenantId;
    this.name = name;
    tenantConfigs.forEach(t -> configs.put(t.getConfigKey(), t.getConfigValue()));
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getName() {
    return name;
  }

  @Override
  public DynamicConfig getParentLevelConfig() {
    if (name == null) {
      return configService.getSystemConfigFromCache();
    } else {
      return configService.getTenantConfigFromCache(tenantId);
    }
  }
}
