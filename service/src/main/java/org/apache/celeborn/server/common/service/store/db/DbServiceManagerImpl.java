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

package org.apache.celeborn.server.common.service.store.db;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.config.ConfigLevel;
import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.server.common.service.config.TenantConfig;
import org.apache.celeborn.server.common.service.model.ClusterInfo;
import org.apache.celeborn.server.common.service.model.ClusterSystemConfig;
import org.apache.celeborn.server.common.service.model.ClusterTag;
import org.apache.celeborn.server.common.service.model.ClusterTenantConfig;
import org.apache.celeborn.server.common.service.store.IServiceManager;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterInfoMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterSystemConfigMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterTagsMapper;
import org.apache.celeborn.server.common.service.store.db.mapper.ClusterTenantConfigMapper;
import org.apache.celeborn.server.common.service.utils.JsonUtils;

public class DbServiceManagerImpl implements IServiceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DbServiceManagerImpl.class);
  private final CelebornConf celebornConf;
  private final ConfigService configService;
  private final SqlSessionFactory sqlSessionFactory;
  private final int clusterId;
  private final int pageSize;

  public DbServiceManagerImpl(CelebornConf celebornConf, ConfigService configServer)
      throws IOException {
    this.celebornConf = celebornConf;
    this.sqlSessionFactory = DBSessionFactory.get(celebornConf);
    this.configService = configServer;
    this.pageSize = celebornConf.dynamicConfigStoreDbFetchPageSize();
    this.clusterId = createCluster(getClusterInfoFromEnv());
  }

  @Override
  public int createCluster(ClusterInfo clusterInfo) {
    ClusterInfo clusterInfoFromDB = getClusterInfo(clusterInfo.getName());
    if (clusterInfoFromDB == null) {
      try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
        ClusterInfoMapper mapper = sqlSession.getMapper(ClusterInfoMapper.class);
        Instant now = Instant.now();
        clusterInfo.setGmtCreate(now);
        clusterInfo.setGmtModify(now);
        mapper.insert(clusterInfo);
        LOG.info("Create cluster {} successfully.", JsonUtils.toJson(clusterInfo));
      } catch (Exception e) {
        LOG.warn("Create cluster {} failed: {}.", JsonUtils.toJson(clusterInfo), e.getMessage(), e);
      }
      clusterInfoFromDB = getClusterInfo(clusterInfo.getName());
      if (clusterInfoFromDB == null) {
        throw new RuntimeException("Could not get cluster info of " + clusterInfo.getName() + ".");
      }
    }
    return clusterInfoFromDB.getId();
  }

  @Override
  public ClusterInfo getClusterInfo(String clusterName) {
    try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
      ClusterInfoMapper mapper = sqlSession.getMapper(ClusterInfoMapper.class);
      return mapper.getClusterInfo(clusterName);
    }
  }

  @Override
  public List<TenantConfig> getAllTenantConfigs() {
    try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
      ClusterTenantConfigMapper mapper = sqlSession.getMapper(ClusterTenantConfigMapper.class);
      int totalNum = mapper.getClusterTenantConfigsNum(clusterId, ConfigLevel.TENANT.name());
      int offset = 0;
      List<ClusterTenantConfig> clusterAllTenantConfigs = new ArrayList<>();
      while (offset < totalNum) {
        List<ClusterTenantConfig> clusterTenantConfigs =
            mapper.getClusterTenantConfigs(clusterId, ConfigLevel.TENANT.name(), offset, pageSize);
        clusterAllTenantConfigs.addAll(clusterTenantConfigs);
        offset = offset + pageSize;
      }

      Map<String, List<ClusterTenantConfig>> tenantConfigMaps =
          clusterAllTenantConfigs.stream()
              .collect(
                  Collectors.groupingBy(clusterTenantConfig -> clusterTenantConfig.getTenantId()));
      return tenantConfigMaps.entrySet().stream()
          .map(t -> new TenantConfig(configService, t.getKey(), null, t.getValue()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<TenantConfig> getAllTenantUserConfigs() {
    try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
      ClusterTenantConfigMapper mapper = sqlSession.getMapper(ClusterTenantConfigMapper.class);
      int totalNum = mapper.getClusterTenantConfigsNum(clusterId, ConfigLevel.TENANT_USER.name());
      int offset = 0;
      List<ClusterTenantConfig> clusterAllTenantConfigs = new ArrayList<>();
      while (offset < totalNum) {
        List<ClusterTenantConfig> clusterTenantConfigs =
            mapper.getClusterTenantConfigs(
                clusterId, ConfigLevel.TENANT_USER.name(), offset, pageSize);
        clusterAllTenantConfigs.addAll(clusterTenantConfigs);
        offset = offset + pageSize;
      }

      Map<Pair<String, String>, List<ClusterTenantConfig>> tenantConfigMaps =
          clusterAllTenantConfigs.stream()
              .collect(Collectors.groupingBy(ClusterTenantConfig::getTenantInfo));
      return tenantConfigMaps.entrySet().stream()
          .map(
              t ->
                  new TenantConfig(
                      configService, t.getKey().getKey(), t.getKey().getValue(), t.getValue()))
          .collect(Collectors.toList());
    }
  }

  @Override
  public List<ClusterSystemConfig> getSystemConfig() {
    try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
      ClusterSystemConfigMapper mapper = sqlSession.getMapper(ClusterSystemConfigMapper.class);
      return mapper.getClusterSystemConfig(clusterId);
    }
  }

  public ClusterInfo getClusterInfoFromEnv() {
    Map<String, String> env = System.getenv();
    String clusterName = env.getOrDefault("CELEBORN_CLUSTER_NAME", celebornConf.clusterName());
    String namespace = env.getOrDefault("CELEBORN_CLUSTER_NAMESPACE", "");
    String endpoint = env.getOrDefault("CELEBORN_CLUSTER_ENDPOINT", "");

    ClusterInfo clusterInfo = new ClusterInfo();
    clusterInfo.setName(clusterName);
    clusterInfo.setNamespace(namespace);
    clusterInfo.setEndpoint(endpoint);

    return clusterInfo;
  }

  public List<ClusterTag> getClusterTags() {
    try (SqlSession sqlSession = sqlSessionFactory.openSession()) {
      ClusterTagsMapper mapper = sqlSession.getMapper(ClusterTagsMapper.class);
      return mapper.getClusterTags(clusterId);
    }
  }

  @Override
  public void upsertSystemConfig(Map<String, String> systemConfigs) {
    try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
      ClusterSystemConfigMapper mapper = sqlSession.getMapper(ClusterSystemConfigMapper.class);
      for (Entry<String, String> systemConfig : systemConfigs.entrySet()) {
        ClusterSystemConfig config = new ClusterSystemConfig();
        Instant now = Instant.now();
        config.setClusterId(clusterId);
        config.setConfigKey(systemConfig.getKey());
        config.setConfigValue(systemConfig.getValue());
        config.setGmtCreate(now);
        config.setGmtModify(now);
        int updated = mapper.update(config);
        if (updated == 0) {
          mapper.insert(config);
        }
      }
    }
  }

  @Override
  public void upsertTenantConfig(
      ConfigLevel configLevel, String tenantId, String name, Map<String, String> tenantConfigs) {
    try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
      ClusterTenantConfigMapper mapper = sqlSession.getMapper(ClusterTenantConfigMapper.class);
      for (Entry<String, String> systemConfig : tenantConfigs.entrySet()) {
        ClusterTenantConfig config = new ClusterTenantConfig();
        Instant now = Instant.now();
        config.setClusterId(clusterId);
        config.setLevel(configLevel.name());
        config.setTenantId(tenantId);
        config.setName(name);
        config.setConfigKey(systemConfig.getKey());
        config.setConfigValue(systemConfig.getValue());
        config.setGmtCreate(now);
        config.setGmtModify(now);
        int updated =
            ConfigLevel.TENANT.equals(configLevel)
                ? mapper.updateConfig(config)
                : mapper.updateUserConfig(config);
        if (updated == 0) {
          mapper.insert(config);
        }
      }
    }
  }

  @Override
  public void deleteSystemConfigByKeys(List<String> configKeys) {
    try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
      ClusterSystemConfigMapper mapper = sqlSession.getMapper(ClusterSystemConfigMapper.class);
      for (String configKey : configKeys) {
        ClusterSystemConfig config = new ClusterSystemConfig();
        config.setClusterId(clusterId);
        config.setConfigKey(configKey);
        mapper.delete(config);
      }
    }
  }

  @Override
  public void deleteTenantConfigByKeys(
      ConfigLevel configLevel, String tenantId, String name, List<String> configKeys) {
    try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
      ClusterTenantConfigMapper mapper = sqlSession.getMapper(ClusterTenantConfigMapper.class);
      for (String configKey : configKeys) {
        ClusterTenantConfig config = new ClusterTenantConfig();
        config.setClusterId(clusterId);
        config.setLevel(configLevel.name());
        config.setTenantId(tenantId);
        config.setName(name);
        config.setConfigKey(configKey);
        if (ConfigLevel.TENANT.equals(configLevel)) {
          mapper.deleteConfig(config);
        } else {
          mapper.deleteUserConfig(config);
        }
      }
    }
  }
}
