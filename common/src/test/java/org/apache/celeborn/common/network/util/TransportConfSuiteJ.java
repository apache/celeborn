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

package org.apache.celeborn.common.network.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TestHelper;
import org.apache.celeborn.common.network.ssl.SslSampleConfigs;
import org.apache.celeborn.common.protocol.TransportModuleConstants;

public class TransportConfSuiteJ {

  private TransportConf transportConf =
      new TransportConf(
          "rpc",
          TestHelper.updateCelebornConfWithMap(
              new CelebornConf(), SslSampleConfigs.createDefaultConfigMapForModule("rpc")));

  @Test
  public void testKeyStorePath() {
    assertEquals(new File(SslSampleConfigs.DEFAULT_KEY_STORE_PATH), transportConf.sslKeyStore());
  }

  @Test
  public void testTrustStorePath() {
    assertEquals(new File(SslSampleConfigs.TRUST_STORE_PATH), transportConf.sslTrustStore());
  }

  @Test
  public void testTrustStoreReloadingEnabled() {
    assertFalse(transportConf.sslTrustStoreReloadingEnabled());
  }

  @Test
  public void testSslEnabled() {
    assertTrue(transportConf.sslEnabled());
  }

  @Test
  public void testSslKeyStorePassword() {
    assertEquals("password", transportConf.sslKeyStorePassword());
  }

  @Test
  public void testSslTrustStorePassword() {
    assertEquals("password", transportConf.sslTrustStorePassword());
  }

  @Test
  public void testSsltrustStoreReloadIntervalMs() {
    assertEquals(10000, transportConf.sslTrustStoreReloadIntervalMs());
  }

  @Test
  public void testSslHandshakeTimeoutMs() {
    assertEquals(10000, transportConf.sslHandshakeTimeoutMs());
  }

  // If a specific key is not set, it should be inherited from celeborn.ssl namespace
  @Test
  public void testInheritance() {

    final String module1 = "rpc";
    final String module2 = "fetch";

    final String module1Protocol = "456";
    final String module2Protocol = "789";

    final long module1ReloadIntervalMs = 123456;
    final long defaultReloadIntervalMs = 83723;

    CelebornConf conf = new CelebornConf();

    // Both should be independently working
    conf.set("celeborn.ssl." + module1 + ".protocol", module1Protocol);
    conf.set("celeborn.ssl." + module2 + ".protocol", module2Protocol);

    // setting at celeborn.ssl should inherit for module2 as it is not overriden
    conf.set(
        "celeborn.ssl." + module1 + ".trustStoreReloadIntervalMs",
        Long.toString(module1ReloadIntervalMs));
    conf.set("celeborn.ssl.trustStoreReloadIntervalMs", Long.toString(defaultReloadIntervalMs));

    TransportConf module1TestConf = new TransportConf(module1, conf);
    TransportConf module2TestConf = new TransportConf(module2, conf);

    assertEquals(module1Protocol, module1TestConf.sslProtocol());
    assertEquals(module2Protocol, module2TestConf.sslProtocol());

    assertEquals(module1ReloadIntervalMs, module1TestConf.sslTrustStoreReloadIntervalMs());
    assertEquals(defaultReloadIntervalMs, module2TestConf.sslTrustStoreReloadIntervalMs());
  }

  @Test
  public void testAutoSslConfiguration() {

    // for rpc_app module, it can be enabled.
    TransportConf conf =
        new TransportConf(
            TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE,
            TestHelper.updateCelebornConfWithMap(
                new CelebornConf(),
                SslSampleConfigs.createAutoSslConfigForModule(
                    TransportModuleConstants.RPC_APP_MODULE)));
    assertTrue(conf.autoSslEnabled());

    // for any other module, it cant be enabled.
    conf =
        new TransportConf(
            TransportModuleConstants.RPC_SERVICE_MODULE,
            TestHelper.updateCelebornConfWithMap(
                new CelebornConf(),
                SslSampleConfigs.createAutoSslConfigForModule(
                    TransportModuleConstants.RPC_SERVICE_MODULE)));
    assertFalse(conf.autoSslEnabled());

    // even if set to true, it gets disabled in case
    // keystore or truststore is specified (which the default config has specified)

    CelebornConf celebornConf = new CelebornConf();
    TestHelper.updateCelebornConfWithMap(
        celebornConf,
        SslSampleConfigs.createDefaultConfigMapForModule(TransportModuleConstants.RPC_APP_MODULE));
    celebornConf.set(
        "celeborn.ssl." + TransportModuleConstants.RPC_APP_MODULE + ".autoSslEnabled", "true");

    conf = new TransportConf(TransportModuleConstants.RPC_APP_MODULE, celebornConf);
    assertFalse(conf.autoSslEnabled());
  }
}
