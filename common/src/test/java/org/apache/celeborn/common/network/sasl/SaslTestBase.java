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
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

public class SaslTestBase {

  protected static final SecretRegistry secretRegistry = new SecretRegistryImpl();

  @BeforeClass
  public static void setup() {
    secretRegistry.register(TEST_USER, TEST_SECRET);
  }

  @AfterClass
  public static void teardown() {
    secretRegistry.unregister(TEST_USER);
  }

  static final String TEST_USER = "appId";
  static final String TEST_SECRET = "secret";

  void authHelper(
      TransportConf conf,
      TransportServerBootstrap serverBootstrap,
      TransportClientBootstrap clientBootstrap)
      throws Throwable {
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
      assertNull(error);
    }
  }

  static class SaslTestCtx implements AutoCloseable {

    final TransportClient client;
    final TransportServer server;
    final TransportContext ctx;

    SaslTestCtx(
        TransportConf conf,
        BaseMessageHandler rpcHandler,
        TransportServerBootstrap serverBootstrap,
        TransportClientBootstrap clientBootstrap)
        throws Exception {

      this.ctx = new TransportContext(conf, rpcHandler);
      this.server = ctx.createServer(Collections.singletonList(serverBootstrap));
      List<TransportClientBootstrap> clientBootstraps = new ArrayList<>();
      clientBootstraps.add(clientBootstrap);
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
      if (null != ctx) {
        ctx.close();
      }
    }
  }
}
