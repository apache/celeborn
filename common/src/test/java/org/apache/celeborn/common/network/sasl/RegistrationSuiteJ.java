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

package org.apache.celeborn.common.network.sasl;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.sasl.registration.RegistrationClientBootstrap;
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo;
import org.apache.celeborn.common.network.sasl.registration.RegistrationRpcHandler;
import org.apache.celeborn.common.network.sasl.registration.RegistrationServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;

/**
 * Jointly tests {@link RegistrationClientBootstrap} and {@link RegistrationRpcHandler}, as both are
 * black boxes.
 */
public class RegistrationSuiteJ extends SaslTestBase {
  private CelebornConf celebornConf;

  @Before
  public void before() throws Exception {
    celebornConf = new CelebornConf();
    celebornConf.set("celeborn.shuffle.io.maxRetries", "1");
  }

  @Test
  public void testRegistration() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, new TestSecretRegistry());
    RegistrationClientBootstrap clientBootstrap =
        new RegistrationClientBootstrap(
            conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET), new RegistrationInfo());
    authHelper(conf, serverBootstrap, clientBootstrap);
  }

  @Test(expected = IOException.class)
  public void testReRegisterationFails() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    // The SecretRegistryImpl already has the entry for TEST_USER so re-registering the app should
    // fail.
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, secretRegistry);
    RegistrationClientBootstrap clientBootstrap =
        new RegistrationClientBootstrap(
            conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET), new RegistrationInfo());

    try {
      authHelper(conf, serverBootstrap, clientBootstrap);
    } catch (Throwable t) {
      assertTrue(Throwables.getStackTraceAsString(t).contains("Application is already registered"));
      throw t.getCause();
    }
  }

  @Test(expected = IOException.class)
  public void testConnectionAuthWithoutRegistrationShouldFail() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, new TestSecretRegistry());
    SaslClientBootstrap clientBootstrap =
        new SaslClientBootstrap(conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET));

    try {
      authHelper(conf, serverBootstrap, clientBootstrap);
    } catch (Throwable t) {
      assertTrue(
          Throwables.getStackTraceAsString(t).contains("Registration information not found"));
      throw t.getCause();
    }
  }

  static class TestSecretRegistry implements SecretRegistry {

    private final Map<String, String> secrets = new HashMap<>();

    @Override
    public void register(String appId, String secret) {
      secrets.put(appId, secret);
    }

    @Override
    public void unregister(String appId) {
      secrets.remove(appId);
    }

    @Override
    public boolean isRegistered(String appId) {
      return secrets.containsKey(appId);
    }

    @Override
    public String getSecretKey(String appId) {
      return secrets.get(appId);
    }
  }
}
