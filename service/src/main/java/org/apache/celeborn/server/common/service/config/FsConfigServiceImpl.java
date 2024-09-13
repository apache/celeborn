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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.yaml.snakeyaml.Yaml;

import org.apache.celeborn.common.CelebornConf;

public class FsConfigServiceImpl extends BaseConfigServiceImpl implements ConfigService {
  private static final String CONF_TENANT_ID = "tenantId";
  private static final String CONF_TENANT_USERS = "users";
  private static final String CONF_TENANT_NAME = "name";
  private static final String CONF_LEVEL = "level";
  private static final String CONF_CONFIG = "config";

  public FsConfigServiceImpl(CelebornConf celebornConf) throws IOException {
    super(celebornConf);
  }

  @Override
  public synchronized void refreshCache() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(getConfigFile(System.getenv()))) {
      Map<String, TenantConfig> tenantConfigs = new HashMap<>();
      Map<Pair<String, String>, TenantConfig> tenantUserConfigs = new HashMap<>();
      List<Map<String, Object>> configs = new Yaml().load(fileInputStream);
      for (Map<String, Object> configMap : configs) {
        if (ConfigLevel.SYSTEM.name().equals(configMap.get(CONF_LEVEL))) {
          if (configMap.containsKey(CONF_CONFIG)) {
            systemConfigAtomicReference.set(new SystemConfig(celebornConf, getConfigs(configMap)));
          }
        } else {
          if (configMap.containsKey(CONF_TENANT_ID)) {
            String tenantId = (String) configMap.get(CONF_TENANT_ID);
            if (configMap.containsKey(CONF_CONFIG)) {
              tenantConfigs.put(
                  tenantId, new TenantConfig(this, tenantId, null, getConfigs(configMap)));
            }
            if (configMap.containsKey(CONF_TENANT_USERS)) {
              for (Map<String, Object> userConfigMap :
                  (List<Map<String, Object>>) configMap.get(CONF_TENANT_USERS)) {
                if (userConfigMap.containsKey(CONF_TENANT_NAME)
                    && userConfigMap.containsKey(CONF_CONFIG)) {
                  String name = (String) userConfigMap.get(CONF_TENANT_NAME);
                  tenantUserConfigs.put(
                      Pair.of(tenantId, name),
                      new TenantConfig(this, tenantId, name, getConfigs(userConfigMap)));
                }
              }
            }
          }
        }
        tenantConfigAtomicReference.set(tenantConfigs);
        tenantUserConfigAtomicReference.set(tenantUserConfigs);
      }
    }
  }

  private Map<String, String> getConfigs(Map<String, Object> configMap) {
    Map<String, Object> configs = (Map<String, Object>) configMap.get(CONF_CONFIG);
    if (configs == null) return Collections.emptyMap();
    return configs.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                config -> Optional.ofNullable(config.getValue()).map(Object::toString).orElse("")));
  }

  private File getConfigFile(Map<String, String> env) throws IOException {
    File configFile =
        celebornConf.dynamicConfigStoreFsPath().isEmpty()
            ? new File(
                env.getOrDefault(
                        "CELEBORN_CONF_DIR",
                        env.getOrDefault("CELEBORN_HOME", ".") + File.separator + "conf")
                    + File.separator
                    + "dynamicConfig.yaml")
            : new File(this.celebornConf.dynamicConfigStoreFsPath().get());
    String configPath = configFile.getPath();
    if (!configFile.exists()) {
      throw new IOException(String.format("Dynamic config file %s does not exist", configPath));
    } else if (!configFile.isFile()) {
      throw new IOException(
          String.format("Dynamic config file %s is not a normal file", configPath));
    }
    return configFile;
  }
}
