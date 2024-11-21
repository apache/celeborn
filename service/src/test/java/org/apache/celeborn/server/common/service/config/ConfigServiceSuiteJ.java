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
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.config.DynamicConfig.ConfigType;
import org.apache.celeborn.server.common.service.model.ClusterInfo;
import org.apache.celeborn.server.common.service.store.IServiceManager;
import org.apache.celeborn.server.common.service.store.db.DBSessionFactory;

public class ConfigServiceSuiteJ {
  private ConfigService configService;
  private static final ThreadLocal<DateFormat> DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

  @Test
  public void testDbConfig() throws IOException, ParseException {
    CelebornConf celebornConf = new CelebornConf();
    celebornConf.set(
        CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_JDBC_URL(),
        "jdbc:h2:mem:test;MODE=MYSQL;INIT=RUNSCRIPT FROM 'classpath:celeborn-0.6.0-h2.sql'\\;"
            + "RUNSCRIPT FROM 'classpath:celeborn-0.6.0-h2-ut-data.sql';DB_CLOSE_DELAY=-1;");
    celebornConf.set(
        CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_DRIVER_CLASS_NAME(), "org.h2.Driver");
    celebornConf.set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_MAXIMUM_POOL_SIZE(), "1");
    configService = new DbConfigServiceImpl(celebornConf);
    verifySystemConfig(configService);
    verifyTenantConfig(configService);
    verifyTenantUserConfig(configService);
    verifyTags(configService);

    SqlSessionFactory sqlSessionFactory = DBSessionFactory.get(celebornConf);
    try (SqlSession sqlSession = sqlSessionFactory.openSession(true)) {
      Statement statement = sqlSession.getConnection().createStatement();

      statement.execute(
          "UPDATE celeborn_cluster_system_config SET config_value = 100 WHERE config_key='celeborn.test.int.only'");
      statement.execute("UPDATE celeborn_cluster_tags SET tag = 'tag3' WHERE tag='tag2'");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    configService.refreshCache();
    verifyConfigChanged(configService);
    verifyTagsChanged(configService);
    verifyServiceManager(
        ((DbConfigServiceImpl) configService).getServiceManager(),
        celebornConf,
        DATE_FORMAT.get().parse("2023-08-26 22:08:30").toInstant());
  }

  @Test
  public void testFsConfig() throws IOException {
    CelebornConf celebornConf = new CelebornConf();
    String file = getClass().getResource("/dynamicConfig.yaml").getFile();
    celebornConf.set(CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH(), file);
    celebornConf.set(CelebornConf.DYNAMIC_CONFIG_REFRESH_INTERVAL(), 5L);
    configService = new FsConfigServiceImpl(celebornConf);
    verifySystemConfig(configService);
    verifyTenantConfig(configService);
    verifyTenantUserConfig(configService);
    verifyTags(configService);

    // change -> refresh config
    file = getClass().getResource("/dynamicConfig_2.yaml").getFile();
    celebornConf.set(CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH(), file);
    configService.refreshCache();

    verifyConfigChanged(configService);
    verifyTagsChanged(configService);
  }

  @After
  public void teardown() {
    if (configService != null) {
      configService.shutdown();
    }
  }

  public void verifySystemConfig(ConfigService configService) {
    // ------------- Verify SystemConfig ----------------- //
    SystemConfig systemConfig = configService.getSystemConfigFromCache();
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
        systemConfig.getValue("celeborn.test.timeoutMs.only", null, Long.TYPE, ConfigType.TIME_MS);
    Assert.assertEquals(value.longValue(), 100000);

    // verify systemConfig's BooleanConf
    Boolean booleanConfValue =
        systemConfig.getValue(
            "celeborn.test.timeoutMs.only", null, Boolean.TYPE, ConfigType.STRING);
    Assert.assertFalse(booleanConfValue);

    // verify systemConfig's intConf
    Integer intConfValue =
        systemConfig.getValue("celeborn.test.int.only", null, Integer.TYPE, ConfigType.STRING);
    Assert.assertEquals(intConfValue.intValue(), 10);
  }

  public void verifyTenantConfig(ConfigService configService) {
    // ------------- Verify TenantConfig ----------------- //
    DynamicConfig tenantConfig = configService.getTenantConfigFromCache("tenant_id");
    // verify tenantConfig's bytesConf -- use tenantConf
    Long value =
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

    DynamicConfig tenantConfigNone = configService.getTenantConfigFromCache("tenant_id_none");
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

  public void verifyTenantUserConfig(ConfigService configService) {
    // ------------- Verify UserConfig ----------------- //
    DynamicConfig userConfig = configService.getTenantUserConfigFromCache("tenant_id1", "Jerry");
    // verify userConfig's bytesConf -- use userConf
    Long value =
        userConfig.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_INITIAL_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024);

    // verify userConfig's bytesConf -- defer to tenantConf
    value =
        userConfig.getValue(
            CelebornConf.CLIENT_PUSH_QUEUE_CAPACITY().key(),
            CelebornConf.CLIENT_PUSH_QUEUE_CAPACITY(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024);

    // verify userConfig's bytesConf -- defer to systemConf
    value =
        userConfig.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024000);

    // verify userConfig's bytesConf -- defer to celebornConf
    value =
        userConfig.getValue(
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD().key(),
            CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1073741824);

    // verify userConfig's bytesConf with none
    value =
        userConfig.getValue(
            "celeborn.client.push.buffer.initial.size.only.none",
            null,
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertNull(value);

    DynamicConfig userConfigNone =
        configService.getTenantUserConfigFromCache("tenant_id", "non_exist");
    // verify userConfig's bytesConf -- defer to tenantConf
    value =
        userConfigNone.getValue(
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(),
            CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE(),
            Long.TYPE,
            ConfigType.BYTES);
    Assert.assertEquals(value.longValue(), 1024000);

    Long withDefaultValue =
        userConfigNone.getWithDefaultValue("none", 10L, Long.TYPE, ConfigType.STRING);
    Assert.assertEquals(withDefaultValue.longValue(), 10);
  }

  public void verifyConfigChanged(ConfigService configService) {

    SystemConfig systemConfig = configService.getSystemConfigFromCache();
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

  public void verifyServiceManager(
      IServiceManager serviceManager, CelebornConf celebornConf, Instant gmtTime) {
    ClusterInfo clusterInfo = serviceManager.getClusterInfo(celebornConf.clusterName());
    Assert.assertEquals(gmtTime, clusterInfo.getGmtCreate());
    Assert.assertEquals(gmtTime, clusterInfo.getGmtModify());
  }

  private void verifyTags(ConfigService configService) {
    SystemConfig systemConfig = configService.getSystemConfigFromCache();
    Map<String, Set<String>> tags = systemConfig.getTags();

    Set<String> tag1 = tags.getOrDefault("tag1", new HashSet<>());
    Assert.assertEquals(tag1.size(), 2);
    Assert.assertTrue(tag1.contains("host1:1111"));
    Assert.assertTrue(tag1.contains("host2:2222"));

    Set<String> tag2 = tags.getOrDefault("tag2", new HashSet<>());
    Assert.assertEquals(tag2.size(), 2);
    Assert.assertTrue(tag2.contains("host3:3333"));
    Assert.assertTrue(tag2.contains("host4:4444"));

    Set<String> tag3 = tags.getOrDefault("tag3", new HashSet<>());
    Assert.assertEquals(tag3.size(), 0);

    verifyTenantAndUserTagsAsNull(configService);
  }

  private void verifyTagsChanged(ConfigService configService) {
    System.out.println("Tags changed");

    SystemConfig systemConfig = configService.getSystemConfigFromCache();
    Map<String, Set<String>> tags = systemConfig.getTags();

    Set<String> tag1 = tags.getOrDefault("tag1", new HashSet<>());
    Assert.assertEquals(tag1.size(), 2);
    Assert.assertTrue(tag1.contains("host1:1111"));
    Assert.assertTrue(tag1.contains("host2:2222"));

    Set<String> tag2 = tags.getOrDefault("tag2", new HashSet<>());
    Assert.assertEquals(tag2.size(), 0);

    Set<String> tag3 = tags.getOrDefault("tag3", new HashSet<>());
    Assert.assertEquals(tag3.size(), 2);
    Assert.assertTrue(tag3.contains("host3:3333"));
    Assert.assertTrue(tag3.contains("host4:4444"));
  }

  private void verifyTenantAndUserTagsAsNull(ConfigService configService) {
    TenantConfig tenantConfig = configService.getRawTenantConfigFromCache("tenant_id1");
    Assert.assertNull(tenantConfig.getTags());

    DynamicConfig userConfig = configService.getTenantUserConfigFromCache("tenant_id1", "Jerry");
    Assert.assertNull(userConfig.getTags());
  }
}
