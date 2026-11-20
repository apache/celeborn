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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class StaticDynamicConfigWarningSuiteJ {
  private static final String STATIC_KEY = CelebornConf.WORKER_FLUSHER_BUFFER_SIZE().key();
  private static final String DYNAMIC_KEY = CelebornConf.QUOTA_USER_DISK_BYTES_WRITTEN().key();
  private final List<String> warnings = new CopyOnWriteArrayList<>();
  private final Logger logger = Logger.getLogger(BaseConfigServiceImpl.class);
  private final AppenderSkeleton appender =
      new AppenderSkeleton() {
        @Override
        protected void append(LoggingEvent event) {
          if (event.getLevel().equals(Level.WARN)) {
            warnings.add(event.getRenderedMessage());
          }
        }

        @Override
        public void close() {}

        @Override
        public boolean requiresLayout() {
          return false;
        }
      };
  private Level previousLevel;
  private ConfigService configService;

  @Before
  public void setup() {
    previousLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    if (configService != null) {
      configService.shutdown();
    }
    logger.removeAppender(appender);
    logger.setLevel(previousLevel);
  }

  @Test
  public void testFsInitialLoadAndRefreshWarnings() throws IOException {
    Path file = Files.createTempFile("celeborn-static-dynamic-config", ".yaml");
    try {
      writeConfig(file, STATIC_KEY);
      CelebornConf conf = new CelebornConf();
      conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_FS_PATH(), file.toString());
      configService = new FsConfigServiceImpl(conf);
      assertWarnings(3);
      assertScopes();
      Assert.assertEquals(262144L, conf.workerFlusherBufferSize());
      Assert.assertTrue(
          configService.getSystemConfigFromCache().getConfigs().containsKey(STATIC_KEY));
      configService.refreshCache();
      assertWarnings(3);

      // Dynamic and custom keys are accepted without static-configuration warnings.
      writeConfig(file, DYNAMIC_KEY);
      configService.refreshCache();
      assertWarnings(3);
      Assert.assertEquals(
          268435456L,
          configService
              .getTenantUserConfigFromCache("warning-tenant", "warning-user")
              .getUserStorageQuota()
              .diskBytesWritten());
      Assert.assertTrue(
          configService
              .getSystemConfigFromCache()
              .getConfigs()
              .containsKey("celeborn.custom.warning-test"));

      // Removing a static key resets suppression, so reintroducing it warns again.
      writeConfig(file, STATIC_KEY);
      configService.refreshCache();
      assertWarnings(6);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void testDbRefreshWarnings() throws IOException {
    CelebornConf conf = new CelebornConf();
    conf.set(
        CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_JDBC_URL(),
        "jdbc:h2:mem:test;MODE=MYSQL;INIT=RUNSCRIPT FROM 'classpath:celeborn-0.6.0-h2.sql'\\;"
            + "RUNSCRIPT FROM 'classpath:celeborn-0.6.0-h2-ut-data.sql';DB_CLOSE_DELAY=-1;");
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_DRIVER_CLASS_NAME(), "org.h2.Driver");
    conf.set(CelebornConf.DYNAMIC_CONFIG_STORE_DB_HIKARI_MAXIMUM_POOL_SIZE(), "1");
    configService = new DbConfigServiceImpl(conf);
    warnings.clear();
    try {
      upsertDbConfig(STATIC_KEY);
      configService.refreshCache();
      assertWarnings(3);
      assertScopes();
      Assert.assertEquals(262144L, conf.workerFlusherBufferSize());
      Assert.assertTrue(
          configService.getSystemConfigFromCache().getConfigs().containsKey(STATIC_KEY));
      configService.refreshCache();
      assertWarnings(3);

      deleteDbConfig(STATIC_KEY);
      upsertDbConfig(DYNAMIC_KEY);
      configService.refreshCache();
      assertWarnings(3);
      Assert.assertEquals(
          268435456L,
          configService
              .getTenantUserConfigFromCache("warning-tenant", "warning-user")
              .getUserStorageQuota()
              .diskBytesWritten());
      upsertDbConfig(STATIC_KEY);
      configService.refreshCache();
      assertWarnings(6);

      // A new service also warns when the keys are already stored in the database.
      configService.shutdown();
      warnings.clear();
      configService = new DbConfigServiceImpl(conf);
      assertWarnings(3);
      assertScopes();
    } finally {
      deleteDbConfig(STATIC_KEY);
      deleteDbConfig(DYNAMIC_KEY);
    }
  }

  private void writeConfig(Path file, String key) throws IOException {
    String yaml =
        "- level: SYSTEM\n  config:\n    "
            + key
            + ": 256m\n"
            + "    celeborn.custom.warning-test: custom-value\n"
            + "- level: TENANT\n  tenantId: warning-tenant\n  config:\n    "
            + key
            + ": 256m\n"
            + "  users:\n    - name: warning-user\n      config:\n        "
            + key
            + ": 256m\n";
    Files.write(file, yaml.getBytes(StandardCharsets.UTF_8));
  }

  private void upsertDbConfig(String key) {
    configService.upsertSystemConfig(Collections.singletonMap(key, "256m"));
    configService.upsertTenantConfig(
        ConfigLevel.TENANT, "warning-tenant", null, Collections.singletonMap(key, "256m"));
    configService.upsertTenantConfig(
        ConfigLevel.TENANT_USER,
        "warning-tenant",
        "warning-user",
        Collections.singletonMap(key, "256m"));
  }

  private void deleteDbConfig(String key) {
    configService.deleteSystemConfigByKeys(Collections.singletonList(key));
    configService.deleteTenantConfigByKeys(
        ConfigLevel.TENANT, "warning-tenant", null, Collections.singletonList(key));
    configService.deleteTenantConfigByKeys(
        ConfigLevel.TENANT_USER, "warning-tenant", "warning-user", Collections.singletonList(key));
  }

  private void assertWarnings(int expected) {
    List<String> matching = new ArrayList<>();
    for (String warning : warnings) {
      if (warning.contains(STATIC_KEY)) {
        matching.add(warning);
      }
    }
    Assert.assertEquals(matching.toString(), expected, matching.size());
    for (String warning : matching) {
      Assert.assertTrue(warning.contains("is not marked as dynamic"));
      Assert.assertFalse(warning.contains("256m"));
    }
    Assert.assertTrue(warnings.stream().noneMatch(w -> w.contains(DYNAMIC_KEY)));
    Assert.assertTrue(warnings.stream().noneMatch(w -> w.contains("celeborn.custom.warning-test")));
  }

  private void assertScopes() {
    Assert.assertTrue(
        warnings.stream()
            .anyMatch(
                w ->
                    w.contains(STATIC_KEY) && w.contains("level SYSTEM (tenant=null, user=null)")));
    Assert.assertTrue(
        warnings.stream()
            .anyMatch(
                w ->
                    w.contains(STATIC_KEY)
                        && w.contains("level TENANT (tenant=warning-tenant, user=null)")));
    Assert.assertTrue(
        warnings.stream()
            .anyMatch(
                w ->
                    w.contains(STATIC_KEY)
                        && w.contains(
                            "level TENANT_USER (tenant=warning-tenant, user=warning-user)")));
  }
}
