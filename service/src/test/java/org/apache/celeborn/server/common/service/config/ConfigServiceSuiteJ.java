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

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.server.common.service.config.DynamicConfig.ConfigType;

public class ConfigServiceSuiteJ {

  @Test
  public void testFsConfig() {
    CelebornConf celebornConf = new CelebornConf();
    String file = getClass().getResource("/dynamicConfig.yaml").getFile();
    celebornConf.set(CelebornConf.QUOTA_CONFIGURATION_PATH(), file);
    celebornConf.set(CelebornConf.DYNAMIC_CONFIG_REFRESH_INTERVAL(), 5L);
    FsConfigServiceImpl fsConfigService = new FsConfigServiceImpl(celebornConf);
    try {
      verifyConfig(fsConfigService);

      // change -> refresh config
      file = getClass().getResource("/dynamicConfig_2.yaml").getFile();
      celebornConf.set(CelebornConf.QUOTA_CONFIGURATION_PATH(), file);

      fsConfigService.refreshAllCache();
      SystemConfig systemConfig = fsConfigService.getSystemConfig();

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
    } finally {
      fsConfigService.shutdown();
    }
  }

  public void verifyConfig(ConfigService configService) {
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
}
