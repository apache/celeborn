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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.config.DynamicConfig.ConfigType;

@RunWith(value = Parameterized.class)
public class ConfigServiceSuiteJ {

  @Parameter public String storeBackend;

  private Connection connection;
  private PreparedStatement preparedStatement;

  private static final String CLIENT_PUSH_BUFFER_INITIAL_SIZE_ONLY =
      "celeborn.client.push.buffer.initial.size.only";
  private static final String CELEBORN_TEST_TIMEOUTMS_ONLY = "celeborn.test.timeoutMs.only";
  private static final String CELEBORN_TEST_ENABLED_ONLY = "celeborn.test.enabled.only";
  private static final String CELEBORN_TEST_INT_ONLY = "celeborn.test.int.only";
  private static final String CELEBORN_TEST_TENANT_TIMEOUTMS_ONLY =
      "celeborn.test.tenant.timeoutMs.only";
  private static final String CELEBORN_TEST_TENANT_ENABLED_ONLY =
      "celeborn.test.tenant.enabled.only";
  private static final String CELEBORN_TEST_TENANT_INT_ONLY = "celeborn.test.tenant.int.only";

  @After
  public void tearDown() {
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

  @Parameters(name = "config service: {0}")
  public static Collection<String> data() {
    return Arrays.asList(ConfigStoreBackend.FS.name(), ConfigStoreBackend.DB.name());
  }

  @Test
  public void testConfigService() throws Exception {
    CelebornConf celebornConf = buildConfig(storeBackend);
    try (ConfigService configService = DynamicConfigServiceFactory.getConfigService(celebornConf)) {
      verifyConfig(configService);
      // change -> refresh config
      refreshConfig(celebornConf, storeBackend);
      verifyChange(configService);
    }
  }

  private CelebornConf buildConfig(String storeBackend) throws Exception {
    CelebornConf celebornConf = new CelebornConf();
    if (ConfigStoreBackend.FS.name().equalsIgnoreCase(storeBackend)) {
      celebornConf
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND(), ConfigStoreBackend.FS.name())
          .set(CelebornConf.DYNAMIC_CONFIG_REFRESH_TIME(), 5L)
          .set(
              CelebornConf.QUOTA_CONFIGURATION_PATH(),
              getClass().getResource("/dynamicConfig.yaml").getFile());
    } else if (ConfigStoreBackend.DB.name().equalsIgnoreCase(storeBackend)) {
      String jdbcDriver = "org.h2.Driver";
      String jdbcUrl = "jdbc:h2:mem:celeborn";
      String jdbcUsername = "root";
      String jdbcPassword = "root";
      celebornConf
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_BACKEND(), ConfigStoreBackend.DB.name())
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_DRIVER(), jdbcDriver)
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_URL(), jdbcUrl)
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_USERNAME(), jdbcUsername)
          .set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_JDBC_PASSWORD(), jdbcPassword);
      initConfigTable(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);
    }
    return celebornConf;
  }

  private void refreshConfig(CelebornConf celebornConf, String storeBackend) throws SQLException {
    if (ConfigStoreBackend.FS.name().equalsIgnoreCase(storeBackend)) {
      celebornConf.set(
          CelebornConf.QUOTA_CONFIGURATION_PATH(),
          getClass().getResource("/dynamicConfig_2.yaml").getFile());
    } else {
      updateSystemConfig(CELEBORN_TEST_INT_ONLY, "100");
    }
  }

  private void verifyConfig(ConfigService configService) {
    // ------------- Verify SystemConfig ----------------- //
    SystemConfig systemConfig = configService.getSystemConfig();
    // verify systemConfig's bytesConf -- use systemConfig
    Long value =
        systemConfig.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 102400);

    // verify systemConfig's bytesConf -- defer to celebornConf
    value =
        systemConfig.getValue(
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD().key(),
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1073741824);

    // verify systemConfig's bytesConf only -- use systemConfig
    value =
        systemConfig.getValue(
            "celeborn.client.push.buffer.initial.size.only", null, Long.TYPE, ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 10240);

    // verify systemConfig's bytesConf with none
    value =
        systemConfig.getValue(
            "celeborn.client.push.buffer.initial.size.only.none",
            null,
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertNull(value);

    // verify systemConfig's timesConf
    value =
        systemConfig.getValue(CELEBORN_TEST_TIMEOUTMS_ONLY, null, Long.TYPE, ConfigType.TIME_MS);
    Assert.assertEquals(value.longValue(), 100000);

    // verify systemConfig's BooleanConf
    Boolean booleanConfValue =
        systemConfig.getValue(CELEBORN_TEST_TIMEOUTMS_ONLY, null, Boolean.TYPE, ConfigType.STRING);
    Assert.assertFalse(booleanConfValue);

    // verify systemConfig's intConf
    Integer intConfValue =
        systemConfig.getValue("celeborn.test.int.only", null, Integer.TYPE, ConfigType.STRING);
    Assert.assertEquals(intConfValue.intValue(), 10);

    // ------------- Verify TenantConfig ----------------- //
    DynamicConfig tenantConfig = configService.getTenantConfig("tenant_id");
    // verify tenantConfig's bytesConf -- use tenantConf
    value =
        tenantConfig.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 10240);

    // verify tenantConfig's bytesConf -- defer to systemConf
    value =
        tenantConfig.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024000);

    // verify tenantConfig's bytesConf -- defer to celebornConf
    value =
        tenantConfig.getValue(
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD().key(),
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1073741824);

    // verify tenantConfig's bytesConf only -- use tenantConf
    value =
        tenantConfig.getValue(
            "celeborn.client.push.buffer.initial.size.only", null, Long.TYPE, ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 102400);

    // verify tenantConfig's bytesConf with none
    value =
        tenantConfig.getValue(
            "celeborn.client.push.buffer.initial.size.only.none",
            null,
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertNull(value);

    DynamicConfig tenantConfigNone = configService.getTenantConfig("tenant_id_none");
    // verify tenantConfig's bytesConf -- defer to systemConf
    value =
        tenantConfigNone.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024000);

    // ------------- Verify with defaultValue ----------------- //
    value =
        tenantConfig.getWithDefaultValue(
            "celeborn.client.push.buffer.initial.size.only", 100L, Long.TYPE, ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 102400);

    Long withDefaultValue =
        tenantConfigNone.getWithDefaultValue("none", 10L, Long.TYPE, ConfigType.STRING);
    Assert.assertEquals(withDefaultValue.longValue(), 10);
  }

  private void verifyChange(ConfigService configService) {
    configService.refreshAllCache();
    SystemConfig systemConfig = configService.getSystemConfig();

    // verify systemConfig's intConf
    Integer intConfValue =
        systemConfig.getValue("celeborn.test.int.only", null, Integer.TYPE, ConfigType.STRING);
    Assert.assertEquals(intConfValue.intValue(), 100);

    // verify systemConfig's bytesConf -- defer to celebornConf
    Long value =
        systemConfig.getValue(
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD().key(),
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1073741824);
  }

  private void initConfigTable(
      String jdbcDriver, String jdbcUrl, String jdbcUsername, String jdbcPassword)
      throws Exception {
    Class.forName(jdbcDriver);
    connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
    preparedStatement = connection.prepareStatement(DbConfigServiceImpl.CREATE_CONFIG_TABLE);
    preparedStatement.executeUpdate();
    insertSystemConfig(CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(), "100k");
    insertSystemConfig(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "1000k");
    insertSystemConfig(CelebornConf.WORKER_FETCH_HEARTBEAT_ENABLED().key(), "true");
    insertSystemConfig(CLIENT_PUSH_BUFFER_INITIAL_SIZE_ONLY, "10k");
    insertSystemConfig(CELEBORN_TEST_TIMEOUTMS_ONLY, "100s");
    insertSystemConfig(CELEBORN_TEST_ENABLED_ONLY, "false");
    insertSystemConfig(CELEBORN_TEST_INT_ONLY, "10");
    insertTenantConfig(CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(), "10k");
    insertTenantConfig(CelebornConf.WORKER_FETCH_HEARTBEAT_ENABLED().key(), "false");
    insertTenantConfig(CLIENT_PUSH_BUFFER_INITIAL_SIZE_ONLY, "100k");
    insertTenantConfig(CELEBORN_TEST_TENANT_TIMEOUTMS_ONLY, "100s");
    insertTenantConfig(CELEBORN_TEST_TENANT_ENABLED_ONLY, "false");
    insertTenantConfig(CELEBORN_TEST_TENANT_INT_ONLY, "10");
  }

  private void insertSystemConfig(String confKey, String confValue) throws SQLException {
    insertDynamicConfig(confKey, confValue, ConfigLevel.SYSTEM.name(), null);
  }

  private void insertTenantConfig(String confKey, String confValue) throws SQLException {
    insertDynamicConfig(confKey, confValue, ConfigLevel.TENANT.name(), "tenant_id");
  }

  private void insertDynamicConfig(String confKey, String confValue, String level, String tenantId)
      throws SQLException {
    preparedStatement =
        connection.prepareStatement(
            String.format(
                "INSERT INTO %s (%s, %s, %s ,%s) VALUES ('%s', '%s', '%s', '%s')",
                DbConfigServiceImpl.CONFIG_TABLE,
                DbConfigServiceImpl.CONFIG_COLUMN_CONF_KEY,
                DbConfigServiceImpl.CONFIG_COLUMN_CONF_VALUE,
                DbConfigServiceImpl.CONFIG_COLUMN_LEVEL,
                DbConfigServiceImpl.CONFIG_COLUMN_TENANT_ID,
                confKey,
                confValue,
                level,
                tenantId));
    preparedStatement.executeUpdate();
  }

  private void updateSystemConfig(String confKey, String confValue) throws SQLException {
    updateDynamicConfig(confKey, confValue, ConfigLevel.SYSTEM.name());
  }

  private void updateDynamicConfig(String confKey, String confValue, String level)
      throws SQLException {
    preparedStatement =
        connection.prepareStatement(
            String.format(
                "UPDATE %s SET %s = '%s' WHERE %s = '%s' AND %s = '%s'",
                DbConfigServiceImpl.CONFIG_TABLE,
                DbConfigServiceImpl.CONFIG_COLUMN_CONF_VALUE,
                confValue,
                DbConfigServiceImpl.CONFIG_COLUMN_CONF_KEY,
                confKey,
                DbConfigServiceImpl.CONFIG_COLUMN_LEVEL,
                level));
    preparedStatement.executeUpdate();
  }
}
