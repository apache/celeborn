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

import static org.apache.celeborn.common.network.sasl.SaslUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.util.TransportConf;

/**
 * Jointly tests {@link CelebornSaslClient} and {@link CelebornSaslServer}, as both are black boxes.
 */
public class CelebornSaslSuiteJ extends SaslTestBase {

  @Test
  public void testDigestMatching() {
    CelebornSaslClient client =
        new CelebornSaslClient(
            DIGEST_MD5,
            DEFAULT_SASL_CLIENT_PROPS,
            new CelebornSaslClient.ClientCallbackHandler(TEST_USER, TEST_SECRET));
    TransportClient transportClient = mock(TransportClient.class);
    CelebornSaslServer server =
        new CelebornSaslServer(
            DIGEST_MD5,
            DEFAULT_SASL_SERVER_PROPS,
            new CelebornSaslServer.DigestCallbackHandler(transportClient, secretRegistry));

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    while (!client.isComplete()) {
      clientMessage = client.response(server.response(clientMessage));
    }
    assertTrue(server.isComplete());
    verify(transportClient, times(1)).setClientId(TEST_USER);

    // Disposal should invalidate
    server.dispose();
    assertFalse(server.isComplete());
    client.dispose();
    assertFalse(client.isComplete());
  }

  @Test
  public void testDigestNonMatching() {
    CelebornSaslClient client =
        new CelebornSaslClient(
            DIGEST_MD5,
            DEFAULT_SASL_CLIENT_PROPS,
            new CelebornSaslClient.ClientCallbackHandler(TEST_USER, "invalid" + TEST_SECRET));
    CelebornSaslServer server =
        new CelebornSaslServer(
            DIGEST_MD5,
            DEFAULT_SASL_SERVER_PROPS,
            new CelebornSaslServer.DigestCallbackHandler(
                mock(TransportClient.class), secretRegistry));

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();

    try {
      while (!client.isComplete()) {
        clientMessage = client.response(server.response(clientMessage));
      }
      fail("Should not have completed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Mismatched response"));
      assertFalse(client.isComplete());
      assertFalse(server.isComplete());
    }
  }

  @Test
  public void testSaslAuth() throws Throwable {
    TransportConf conf = new TransportConf("shuffle", new CelebornConf());
    SaslServerBootstrap serverBootstrap = new SaslServerBootstrap(conf, secretRegistry);
    SaslClientBootstrap clientBootstrap =
        new SaslClientBootstrap(conf, TEST_USER, new SaslCredentials(TEST_USER, TEST_SECRET));
    authHelper(conf, serverBootstrap, clientBootstrap);
  }

  @Test
  public void testRpcHandlerDelegate() {
    // Tests all delegates exception for receive(), which is more complicated and already handled
    // by all other tests.
    BaseMessageHandler handler = mock(BaseMessageHandler.class);
    BaseMessageHandler saslHandler = new SaslRpcHandler(null, null, handler, null);

    saslHandler.channelInactive(null);
    verify(handler).channelInactive(isNull());

    saslHandler.exceptionCaught(null, null);
    verify(handler).exceptionCaught(isNull(), isNull());
  }

  @Test
  public void testAnonymous() {
    CelebornSaslClient client = new CelebornSaslClient(ANONYMOUS, null, null);
    CelebornSaslServer server = new CelebornSaslServer(ANONYMOUS, null, null);

    assertFalse(client.isComplete());
    assertFalse(server.isComplete());

    byte[] clientMessage = client.firstToken();
    while (!client.isComplete()) {
      clientMessage = client.response(server.response(clientMessage));
    }
    assertTrue(server.isComplete());

    // Disposal should invalidate
    server.dispose();
    assertFalse(server.isComplete());
    client.dispose();
    assertFalse(client.isComplete());
  }
}
