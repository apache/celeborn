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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;

public class DbConfigServiceImpl extends BaseConfigServiceImpl {
  private static final Logger LOG = LoggerFactory.getLogger(DbConfigServiceImpl.class);
  protected static final String CONFIG_TABLE = "dynamic_configs";
  protected static final String CONFIG_COLUMN_CONF_KEY = "conf_key";
  protected static final String CONFIG_COLUMN_CONF_VALUE = "conf_value";
  protected static final String CONFIG_COLUMN_LEVEL = "level";
  protected static final String CONFIG_COLUMN_TENANT_ID = "tenant_id";
  protected static final String CREATE_CONFIG_TABLE =
      String.format(
          "CREATE TABLE %s "
              + "(%s VARCHAR(255) not null, "
              + " %s VARCHAR(255), "
              + " %s VARCHAR(255) not null, "
              + " %s VARCHAR(255))",
          CONFIG_TABLE,
          CONFIG_COLUMN_CONF_KEY,
          CONFIG_COLUMN_CONF_VALUE,
          CONFIG_COLUMN_LEVEL,
          CONFIG_COLUMN_TENANT_ID);

  private Connection connection;
  private volatile PreparedStatement preparedStatement;
  private volatile ResultSet resultSets;
  private volatile boolean tableExists;

  public DbConfigServiceImpl(CelebornConf celebornConf) {
    super(celebornConf);
  }

  protected void init() {
    Option<String> jdbcDriverOpt = celebornConf.dynamicConfigStoreDbJdbcDriver();
    Option<String> jdbcUrlOpt = celebornConf.dynamicConfigStoreDbJdbcUrl();
    Option<String> jdbcUsernameOpt = celebornConf.dynamicConfigStoreDbJdbcUsername();
    Option<String> jdbcPasswordOpt = celebornConf.dynamicConfigStoreDbJdbcPassword();
    if (jdbcDriverOpt.isEmpty()
        || jdbcUrlOpt.isEmpty()
        || jdbcUsernameOpt.isEmpty()
        || jdbcPasswordOpt.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Some of the jdbc configs do not configure. Please check configs: %s, %s, %s, %s",
              CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_DRIVER().key(),
              CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_URL().key(),
              CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_USERNAME().key(),
              CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_PASSWORD().key()));
    }
    try {
      Class.forName(jdbcDriverOpt.get());
      connection =
          DriverManager.getConnection(
              jdbcUrlOpt.get(), jdbcUsernameOpt.get(), jdbcPasswordOpt.get());
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to create jdbc connection: Driver: %s, Url: %s, Username: %s, Password: %s.",
              jdbcDriverOpt.get(), jdbcUrlOpt.get(), jdbcUsernameOpt.get(), jdbcPasswordOpt.get()));
    }
  }

  protected synchronized void refresh() {
    List<Map<String, Object>> dynamicConfigs;
    try {
      checkTableExists();
      dynamicConfigs = loadDynamicConfigs();
    } catch (Exception e) {
      LOG.error("Refresh dynamic config error: {}.", e.getMessage(), e);
      return;
    }
    refreshConfig(dynamicConfigs);
  }

  @Override
  public void close() {
    super.close();
    if (resultSets != null) {
      try {
        resultSets.close();
      } catch (SQLException ignored) {
      }
    }
    if (preparedStatement != null) {
      try {
        preparedStatement.close();
      } catch (SQLException ignored) {
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ignored) {
      }
    }
  }

  private void checkTableExists() throws SQLException {
    preparedStatement = connection.prepareStatement("SHOW TABLES");
    resultSets = preparedStatement.executeQuery();
    while (resultSets.next()) {
      if (CONFIG_TABLE.equalsIgnoreCase(resultSets.getString(1))) {
        tableExists = true;
        break;
      }
    }
    if (!tableExists) {
      preparedStatement = connection.prepareStatement(CREATE_CONFIG_TABLE);
      preparedStatement.executeUpdate();
      tableExists = true;
    }
  }

  private List<Map<String, Object>> loadDynamicConfigs() throws SQLException {
    preparedStatement =
        connection.prepareStatement(
            String.format(
                "SELECT %s, %s, %s, %s FROM %s",
                CONFIG_COLUMN_CONF_KEY,
                CONFIG_COLUMN_CONF_VALUE,
                CONFIG_COLUMN_LEVEL,
                CONFIG_COLUMN_TENANT_ID,
                CONFIG_TABLE));
    resultSets = preparedStatement.executeQuery();
    Map<String, Object> systemConfig = null;
    Map<String, Map<String, Object>> tenantConfigs = new HashMap<>();
    while (resultSets.next()) {
      String key = resultSets.getString(CONFIG_COLUMN_CONF_KEY);
      String value = resultSets.getString(CONFIG_COLUMN_CONF_VALUE);
      String level = resultSets.getString(CONFIG_COLUMN_LEVEL);
      if (ConfigLevel.SYSTEM.name().equals(level)) {
        if (systemConfig == null) {
          systemConfig = new HashMap<>();
          systemConfig.put(CONF_LEVEL, level);
          systemConfig.put(CONF_CONFIG, buildConfConfigs(key, value));
        } else {
          ((Map<String, Object>) systemConfig.get(CONF_CONFIG)).put(key, value);
        }
      } else if (ConfigLevel.TENANT.name().equals(level)) {
        {
          String tenantId = resultSets.getString(CONFIG_COLUMN_TENANT_ID);
          if (tenantConfigs.containsKey(tenantId)) {
            ((Map<String, Object>) tenantConfigs.get(tenantId).get(CONF_CONFIG)).put(key, value);
          } else {
            Map<String, Object> tenantConfig = new HashMap<>();
            tenantConfig.put(CONF_LEVEL, level);
            tenantConfig.put(CONF_TENANT_ID, tenantId);
            tenantConfig.put(CONF_CONFIG, buildConfConfigs(key, value));
            tenantConfigs.put(tenantId, tenantConfig);
          }
        }
      }
    }
    List<Map<String, Object>> dynamicConfigs = new ArrayList<>();
    if (systemConfig != null) {
      dynamicConfigs.add(systemConfig);
    }
    dynamicConfigs.addAll(tenantConfigs.values());
    return dynamicConfigs;
  }

  private Map<String, Object> buildConfConfigs(String key, String value) {
    Map<String, Object> confConfigs = new HashMap<>();
    confConfigs.put(key, value);
    return confConfigs;
  }
}
