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

import java.util.HashMap;
import java.util.Map;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.internal.config.ConfigEntry;

public class SystemConfig extends DynamicConfig {
  private final CelebornConf celebornConf;

  public SystemConfig(CelebornConf celebornConf, Map<String, String> configs) {
    this.celebornConf = celebornConf;
    this.configs.putAll(configs);
  }

  public SystemConfig(CelebornConf celebornConf) {
    this.celebornConf = celebornConf;
    this.configs = new HashMap<>();
  }

  @Override
  public DynamicConfig getParentLevelConfig() {
    return null;
  }

  @Override
  public <T> T getValue(
      String configKey,
      ConfigEntry<Object> configEntry,
      Class<T> finalType,
      ConfigType configType) {
    String configValue = configs.get(configKey);
    T formatValue =
        configValue != null ? formatValue(configKey, configValue, finalType, configType) : null;
    if (formatValue == null && configEntry != null) {
      return convert(finalType, celebornConf.get(configEntry).toString());
    } else {
      return formatValue;
    }
  }
}
