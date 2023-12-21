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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * Jointly tests {@link CelebornSaslClient} and {@link CelebornSaslServer}, as both are black boxes.
 */
public class CelebornSaslSuiteJ {
  private static final String TEST_USER = "appId";
  private static final String TEST_SECRET = "secret";

  @BeforeClass
  public static void setup() {
    SecretRegistryImpl.getInstance().register(TEST_USER, TEST_SECRET);
  }

  @Test
  public void testDigestMatching() {
    CelebornSaslClient client =
        new CelebornSaslClient(
            DIGEST_MD5,
            DEFAULT_SASL_CLIENT_PROPS,
            new CelebornSaslClient.ClientCallbackHandler(TEST_USER, TEST_SECRET));
    CelebornSaslServer server =
        new CelebornSaslServer(
            DIGEST_MD5,
            DEFAULT_SASL_SERVER_PROPS,
            new CelebornSaslServer.DigestCallbackHandler(SecretRegistryImpl.getInstance()));

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
            new CelebornSaslServer.DigestCallbackHandler(SecretRegistryImpl.getInstance()));

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

    try (SaslTestCtx ctx = new SaslTestCtx(rpcHandler)) {
      ByteBuffer response =
          ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), TimeUnit.SECONDS.toMillis(10));
      assertEquals("Pong", JavaUtils.bytesToString(response));
    } finally {
      // There should be 2 terminated events; one for the client, one for the server.
      Throwable error = null;
      long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
      while (deadline > System.nanoTime()) {
        try {
          verify(rpcHandler, times(2)).channelInactive(any(TransportClient.class));
          error = null;
          break;
        } catch (Throwable t) {
          error = t;
          TimeUnit.MILLISECONDS.sleep(10);
        }
      }
      if (error != null) {
        throw error;
      }
    }
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

  private static class SaslTestCtx implements AutoCloseable {

    final TransportClient client;
    final TransportServer server;
    final TransportContext ctx;

    SaslTestCtx(BaseMessageHandler rpcHandler) throws Exception {
      TransportConf conf = new TransportConf("shuffle", new CelebornConf());

      this.ctx = new TransportContext(conf, rpcHandler);
      this.server =
          ctx.createServer(
              Collections.singletonList(
                  new SaslServerBootstrap(conf, SecretRegistryImpl.getInstance())));
      List<TransportClientBootstrap> clientBootstraps = new ArrayList<>();
      clientBootstraps.add(
          new SaslClientBootstrap(conf, "appId", new SaslCredentials(TEST_USER, TEST_SECRET)));
      try {
        this.client =
            ctx.createClientFactory(clientBootstraps)
                .createClient(JavaUtils.getLocalHost(), server.getPort());
      } catch (Exception e) {
        close();
        throw e;
      }
    }

    @Override
    public void close() {
      if (client != null) {
        client.close();
      }
      if (server != null) {
        server.close();
      }
    }
  }
}
