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

package org.apache.celeborn.server.common.service.store;

import java.util.List;
import java.util.Map;

import org.apache.celeborn.server.common.service.config.ConfigLevel;
import org.apache.celeborn.server.common.service.config.TenantConfig;
import org.apache.celeborn.server.common.service.model.ClusterInfo;
import org.apache.celeborn.server.common.service.model.ClusterSystemConfig;
import org.apache.celeborn.server.common.service.model.ClusterTag;

public interface IServiceManager {

  int createCluster(ClusterInfo clusterInfo);

  ClusterInfo getClusterInfo(String clusterName);

  List<TenantConfig> getAllTenantConfigs();

  List<TenantConfig> getAllTenantUserConfigs();

  List<ClusterSystemConfig> getSystemConfig();

  List<ClusterTag> getClusterTags();

  void upsertSystemConfig(Map<String, String> systemConfigs);

  void upsertTenantConfig(
      ConfigLevel configLevel, String tenantId, String name, Map<String, String> tenantConfigs);

  void deleteSystemConfigByKeys(List<String> configKeys);

  void deleteTenantConfigByKeys(
      ConfigLevel configLevel, String tenantId, String name, List<String> configKeys);
}
