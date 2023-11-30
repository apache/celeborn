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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.internal.config.ConfigEntry;
import org.apache.celeborn.common.util.Utils;

/**
 * Dynamic configuration is a type of configuration that can be changed at runtime as needed. It can
 * be used at system level/tenant level. When applying dynamic configuration, the priority order is
 * as follows: tenant level overrides system level, which in turn overrides static
 * configuration(CelebornConf). This means that if a configuration is defined at the tenant level,
 * it will be used instead of the system level or static configuration(CelebornConf). If the
 * tenant-level configuration is missing, the system-level configuration will be used. If the
 * system-level configuration is also missing, CelebornConf will be used as the default value.
 */
public abstract class DynamicConfig {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicConfig.class);
  protected Map<String, String> configs = new HashMap<>();

  public abstract DynamicConfig getParentLevelConfig();

  public <T> T getWithDefaultValue(
      String configKey, T defaultValue, Class<T> finalType, ConfigType configType) {
    String configValue = configs.get(configKey);
    T formatValue =
        configValue != null ? formatValue(configKey, configValue, finalType, configType) : null;
    if (formatValue == null) {
      return defaultValue;
    } else {
      return formatValue;
    }
  }

  public <T> T getValue(
      String configKey,
      ConfigEntry<Object> configEntry,
      Class<T> finalType,
      ConfigType configType) {
    String configValue = configs.get(configKey);
    T formatValue =
        configValue != null ? formatValue(configKey, configValue, finalType, configType) : null;
    if (formatValue == null) {
      DynamicConfig parentLevelConfig = getParentLevelConfig();
      return parentLevelConfig != null
          ? parentLevelConfig.getValue(configKey, configEntry, finalType, configType)
          : null;
    } else {
      return formatValue;
    }
  }

  public <T> T formatValue(
      String configKey, String configValue, Class<T> finalType, ConfigType configType) {
    try {
      if (configValue != null) {
        if (ConfigType.BYTES == configType) {
          return convert(finalType, String.valueOf(Utils.byteStringAsBytes(configValue)));
        } else if (ConfigType.TIME_MS == configType) {
          return convert(finalType, String.valueOf(Utils.timeStringAsMs(configValue)));
        } else {
          return convert(finalType, configValue);
        }
      }
    } catch (Exception e) {
      LOG.warn("Config {} value format is not valid, refer to parent if exist", configKey, e);
    }
    return null;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DynamicConfig{");
    sb.append("configs=").append(configs);
    sb.append('}');
    return sb.toString();
  }

  public enum ConfigType {
    BYTES,
    STRING,
    TIME_MS,
  }

  public static <T> T convert(Class<T> clazz, String value) {
    if (Boolean.TYPE == clazz) {
      return (T) Boolean.valueOf(value);
    } else if (Byte.TYPE == clazz) {
      return (T) Byte.valueOf(value);
    } else if (Short.TYPE == clazz) {
      return (T) Short.valueOf(value);
    } else if (Integer.TYPE == clazz) {
      return (T) Integer.valueOf(value);
    } else if (Long.TYPE == clazz) {
      return (T) Long.valueOf(value);
    } else if (Float.TYPE == clazz) {
      return (T) Float.valueOf(value);
    } else if (Double.TYPE == clazz) {
      return (T) Double.valueOf(value);
    }
    return (T) value;
  }
}
