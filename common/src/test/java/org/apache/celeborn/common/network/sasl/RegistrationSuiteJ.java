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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.network.sasl.registration.RegistrationClientBootstrap;
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo;
import org.apache.celeborn.common.network.sasl.registration.RegistrationRpcHandler;
import org.apache.celeborn.common.network.sasl.registration.RegistrationServerBootstrap;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

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
    assertRoundTrip(conf, serverBootstrap, clientBootstrap);
  }

  @Test
  public void testReRegistrationWithSameSecretSucceeds() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    // The SecretRegistryImpl already has the entry for TEST_USER. Re-registering the app with the
    // same secret is idempotent and should succeed.
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, secretRegistry);
    RegistrationClientBootstrap clientBootstrap =
        new RegistrationClientBootstrap(
            conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET), new RegistrationInfo());

    assertRoundTrip(conf, serverBootstrap, clientBootstrap);
  }

  @Test
  public void testReRegistrationWithDifferentSecretFails() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, secretRegistry);
    RegistrationClientBootstrap clientBootstrap =
        new RegistrationClientBootstrap(
            conf,
            TEST_USER,
            new SaslCredentials(TEST_USER, "different-secret"),
            new RegistrationInfo());

    Throwable t =
        assertThrows(
            Throwable.class, () -> assertRoundTrip(conf, serverBootstrap, clientBootstrap));
    assertTrue(
        Throwables.getStackTraceAsString(t)
            .contains("Application is already registered with a different secret"));
  }

  @Test
  public void testStaleRegisteredStateResetsOnConnectionAuthFailure() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    RegistrationInfo registrationInfo = new RegistrationInfo();
    registrationInfo.setRegistrationState(RegistrationInfo.RegistrationState.REGISTERED);
    TestSecretRegistry testSecretRegistry = new TestSecretRegistry();
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, testSecretRegistry);
    RegistrationClientBootstrap clientBootstrap =
        new RegistrationClientBootstrap(
            conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET), registrationInfo);

    Throwable t =
        assertThrows(
            Throwable.class, () -> assertRoundTrip(conf, serverBootstrap, clientBootstrap));
    assertTrue(Throwables.getStackTraceAsString(t).contains("Registration information not found"));
    assertEquals(RegistrationInfo.RegistrationState.FAILED, registrationInfo.getRegistrationState());

    RegistrationServerBootstrap retryServerBootstrap =
        new RegistrationServerBootstrap(conf, testSecretRegistry);
    assertRoundTrip(conf, retryServerBootstrap, clientBootstrap);
    assertEquals(
        RegistrationInfo.RegistrationState.REGISTERED, registrationInfo.getRegistrationState());
  }

  @Test
  public void testConnectionAuthWithoutRegistrationShouldFail() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", celebornConf);
    RegistrationServerBootstrap serverBootstrap =
        new RegistrationServerBootstrap(conf, new TestSecretRegistry());
    SaslClientBootstrap clientBootstrap =
        new SaslClientBootstrap(conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET));

    Throwable t =
        assertThrows(
            Throwable.class, () -> assertRoundTrip(conf, serverBootstrap, clientBootstrap));
    assertTrue(Throwables.getStackTraceAsString(t).contains("Registration information not found"));
  }

  @Test
  public void testProcessMasterNotLeaderException() {
    RuntimeException wrapper =
        new RuntimeException(new MasterNotLeaderException("host1:9097", "host2:9097", null));

    Throwable processed = RegistrationClientBootstrap.processMasterNotLeaderException(wrapper);

    assertTrue(processed instanceof MasterNotLeaderException);
    assertEquals("host2:9097", ((MasterNotLeaderException) processed).getSuggestedLeaderAddress());
  }

  @Test
  public void testProcessMasterNotLeaderExceptionKeepsOtherFailures() {
    RuntimeException failure = new RuntimeException("registration failed");

    assertSame(failure, RegistrationClientBootstrap.processMasterNotLeaderException(failure));
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

  private static void assertRoundTrip(
      TransportConf conf,
      RegistrationServerBootstrap serverBootstrap,
      TransportClientBootstrap clientBootstrap)
      throws Exception {
    BaseMessageHandler rpcHandler = mock(BaseMessageHandler.class);
    doAnswer(
            invocation -> {
              RequestMessage message = (RequestMessage) invocation.getArguments()[1];
              RpcResponseCallback cb = (RpcResponseCallback) invocation.getArguments()[2];
              assertEquals("Ping", JavaUtils.bytesToString(message.body().nioByteBuffer()));
              cb.onSuccess(JavaUtils.stringToBytes("Pong"));
              return null;
            })
        .when(rpcHandler)
        .receive(
            any(TransportClient.class), any(RequestMessage.class), any(RpcResponseCallback.class));
    doReturn(true).when(rpcHandler).checkRegistered();

    try (SaslTestCtx ctx = new SaslTestCtx(conf, rpcHandler, serverBootstrap, clientBootstrap)) {
      ByteBuffer response = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 10000);
      assertEquals("Pong", JavaUtils.bytesToString(response));
    }
  }
}
