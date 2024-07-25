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

package org.apache.celeborn.service.deploy.worker.network;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TestHelper;
import org.apache.celeborn.common.network.ssl.SslSampleConfigs;

public class SSLRequestTimeoutIntegrationSuiteJ extends RequestTimeoutIntegrationSuiteJ {
  @Before
  @Override
  public void setUp() {
    // set up SSL for TEST_MODULE
    doSetup(
        TestHelper.updateCelebornConfWithMap(
            new CelebornConf(), SslSampleConfigs.createDefaultConfigMapForModule(TEST_MODULE)));
  }

  @After
  @Override
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void validateSslConfig() {
    // this is to ensure ssl config has been applied.
    assertTrue(super.getConf().sslEnabled());
  }
}
