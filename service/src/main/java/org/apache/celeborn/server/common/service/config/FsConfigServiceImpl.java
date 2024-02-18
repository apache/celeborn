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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import org.apache.celeborn.common.CelebornConf;

public class FsConfigServiceImpl extends BaseConfigServiceImpl implements ConfigService {
  private static final Logger LOG = LoggerFactory.getLogger(FsConfigServiceImpl.class);
  private static final String CONF_TENANT_ID = "tenantId";
  private static final String CONF_TENANT_USERS = "users";
  private static final String CONF_TENANT_NAME = "name";
  private static final String CONF_LEVEL = "level";
  private static final String CONF_CONFIG = "config";

  public FsConfigServiceImpl(CelebornConf celebornConf) throws IOException {
    super(celebornConf);
  }

  @Override
  public synchronized void refreshAllCache() {
    File configurationFile = getConfigurationFile(System.getenv());
    if (!configurationFile.exists()) {
      return;
    }

    SystemConfig systemConfig = null;
    Map<String, TenantConfig> tenantConfs = new HashMap<>();
    Map<Pair<String, String>, TenantConfig> tenantUserConfs = new HashMap<>();
    try (FileInputStream fileInputStream = new FileInputStream(configurationFile)) {
      Yaml yaml = new Yaml();
      List<Map<String, Object>> dynamicConfigs = yaml.load(fileInputStream);
      for (Map<String, Object> settings : dynamicConfigs) {
        String level = (String) settings.get(CONF_LEVEL);
        if (ConfigLevel.TENANT.name().equals(level)) {
          if (settings.containsKey(CONF_TENANT_ID)) {
            String tenantId = (String) settings.get(CONF_TENANT_ID);
            if (settings.containsKey(CONF_CONFIG)) {
              Map<String, String> config = extractConfig(settings);
              TenantConfig tenantConfig = new TenantConfig(this, tenantId, null, config);
              tenantConfs.put(tenantId, tenantConfig);
            }
            if (settings.containsKey(CONF_TENANT_USERS)) {
              List<Map<String, Object>> users =
                  (List<Map<String, Object>>) settings.get(CONF_TENANT_USERS);
              for (Map<String, Object> userSetting : users) {
                if (userSetting.containsKey(CONF_TENANT_NAME)
                    && userSetting.containsKey(CONF_CONFIG)) {
                  String name = (String) userSetting.get(CONF_TENANT_NAME);
                  Map<String, String> userConfig = extractConfig(userSetting);
                  TenantConfig tenantUserConfig =
                      new TenantConfig(this, tenantId, name, userConfig);
                  tenantUserConfs.put(Pair.of(tenantId, name), tenantUserConfig);
                }
              }
            }
          }
        } else {
          if (settings.containsKey(CONF_CONFIG)) {
            Map<String, String> config = extractConfig(settings);
            systemConfig = new SystemConfig(celebornConf, config);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Refresh dynamic config error: {}", e.getMessage(), e);
      return;
    }

    tenantUserConfigAtomicReference.set(tenantUserConfs);
    tenantConfigAtomicReference.set(tenantConfs);
    if (systemConfig != null) {
      systemConfigAtomicReference.set(systemConfig);
    }
  }

  private Map<String, String> extractConfig(Map<String, Object> setting) {
    Map<String, String> config =
        ((Map<String, Object>) setting.get(CONF_CONFIG))
            .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, a -> a.getValue().toString()));
    return config;
  }

  private File getConfigurationFile(Map<String, String> env) {
    if (!this.celebornConf.quotaConfigurationPath().isEmpty()) {
      return new File(this.celebornConf.quotaConfigurationPath().get());
    } else {
      String dynamicConfPath =
          Optional.ofNullable(env.get("CELEBORN_CONF_DIR"))
              .orElse(env.getOrDefault("CELEBORN_HOME", ".") + File.separator + "conf");
      return new File(dynamicConfPath + File.separator + "dynamicConfig.yaml");
    }
  }
}
