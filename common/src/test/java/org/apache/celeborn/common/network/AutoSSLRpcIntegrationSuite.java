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

package org.apache.celeborn.common.network;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.ssl.SslSampleConfigs;
import org.apache.celeborn.common.protocol.TransportModuleConstants;

public class AutoSSLRpcIntegrationSuite extends RpcIntegrationSuiteJ {
  @BeforeClass
  public static void setUp() throws Exception {
    // change it to lifecycle manager module
    RpcIntegrationSuiteJ.TEST_MODULE = TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE;
    // set up SSL for TEST_MODULE
    RpcIntegrationSuiteJ.initialize(
        TestHelper.updateCelebornConfWithMap(
            new CelebornConf(),
            // to validate, we are using TransportModuleConstants.RPC_APP_MODULE, to mirror how
            // users will configure
            SslSampleConfigs.createAutoSslConfigForModule(
                TransportModuleConstants.RPC_APP_MODULE)));
  }

  @AfterClass
  public static void tearDown() {
    RpcIntegrationSuiteJ.tearDown();
  }

  @Test
  public void validateSslConfig() {
    // this is to ensure ssl config has been applied.
    assertTrue(conf.sslEnabled());
    assertTrue(conf.autoSslEnabled());
  }
}
