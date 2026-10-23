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

package org.apache.celeborn.common.network.security;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.Channel;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.sasl.SaslClientBootstrap;
import org.apache.celeborn.common.network.sasl.SaslCredentials;
import org.apache.celeborn.common.network.sasl.SaslServerBootstrap;
import org.apache.celeborn.common.network.sasl.SecretRegistryImpl;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.server.TransportServerBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.JavaUtils;

public class TransportBootstrapSuiteJ {
  private static final String APP = "application";
  private static final String SECRET = "application-secret";
  private static final String CLIENT_CLASSES = "celeborn.shuffle.client.bootstrap.classes";
  private static final String SERVER_CLASSES = "celeborn.shuffle.server.bootstrap.classes";

  @Test
  public void configuredBootstrapsRunBeforeCallerBootstrapsWithDirectConstructors()
      throws Exception {
    AtomicInteger clientCalls = new AtomicInteger();
    AtomicInteger serverRequests = new AtomicInteger();
    TransportConf conf =
        transportConf(
            configuration()
                .set(CLIENT_CLASSES, ConfiguredSaslClient.class.getName())
                .set(SERVER_CLASSES, ConfiguredSaslServer.class.getName()));
    try (TransportContext serverContext = new TransportContext(conf, new EchoHandler());
        TransportContext clientContext = new TransportContext(conf, new BaseMessageHandler());
        TransportServer server =
            new TransportServer(
                serverContext,
                "localhost",
                0,
                Collections.singletonList(
                    (channel, delegate) ->
                        new BaseMessageHandler() {
                          @Override
                          public boolean checkRegistered() {
                            return delegate.checkRegistered();
                          }

                          @Override
                          public void receive(
                              TransportClient client,
                              RequestMessage message,
                              RpcResponseCallback callback) {
                            serverRequests.incrementAndGet();
                            delegate.receive(client, message, callback);
                          }
                        }));
        TransportClientFactory factory =
            new TransportClientFactory(
                clientContext,
                Collections.singletonList(
                    client -> {
                      assertEquals(APP, client.getClientId());
                      clientCalls.incrementAndGet();
                    }));
        TransportClient client = factory.createClient("localhost", server.getPort())) {
      assertEquals(
          APP, JavaUtils.bytesToString(client.sendRpcSync(JavaUtils.stringToBytes(APP), 10000)));
      assertEquals(1, clientCalls.get());
      assertEquals("The caller handler must not receive SASL handshakes", 1, serverRequests.get());
    }
  }

  @Test
  public void configurationIsExactModuleAndSupportsBothConstructorForms() {
    CountingBootstrap.created.set(0);
    NoArgBootstrap.created.set(0);
    CelebornConf conf =
        configuration()
            .set("celeborn.rpc.client.bootstrap.classes", String.class.getName())
            .set(
                CLIENT_CLASSES,
                CountingBootstrap.class.getName() + "," + NoArgBootstrap.class.getName());
    try (TransportContext ignored = new TransportContext(transportConf(conf), new EchoHandler());
        TransportContext internal =
            new TransportContext(new TransportConf("rpc_service", conf), new EchoHandler())) {
      assertEquals(1, CountingBootstrap.created.get());
      assertEquals("shuffle", CountingBootstrap.module);
      assertEquals(1, NoArgBootstrap.created.get());
    }
    conf.set(CLIENT_CLASSES, "");
    try (TransportContext ignored = new TransportContext(transportConf(conf), new EchoHandler())) {
      assertEquals(1, CountingBootstrap.created.get());
    }
  }

  @Test
  public void contextOwnsConfiguredInstancesNotFactoriesOrServers() {
    CountingBootstrap.created.set(0);
    CountingBootstrap.closed.set(0);
    TransportConf conf =
        transportConf(
            configuration()
                .set(CLIENT_CLASSES, CountingBootstrap.class.getName())
                .set(SERVER_CLASSES, CountingBootstrap.class.getName()));
    try (TransportContext context = new TransportContext(conf, new EchoHandler())) {
      assertEquals(2, CountingBootstrap.created.get());
      try (TransportClientFactory first = context.createClientFactory();
          TransportClientFactory second = context.createClientFactory();
          TransportServer server = context.createServer("localhost", 0)) {
        assertEquals(2, CountingBootstrap.created.get());
      }
      assertEquals(0, CountingBootstrap.closed.get());
      context.close();
    }
    assertEquals(2, CountingBootstrap.closed.get());
  }

  @Test
  public void invalidBootstrapClosesPreviouslyCreatedInstances() {
    for (Class<?> invalid : new Class<?>[] {String.class, FailingBootstrap.class}) {
      CountingBootstrap.closed.set(0);
      TransportConf conf =
          transportConf(
              configuration()
                  .set(CLIENT_CLASSES, CountingBootstrap.class.getName())
                  .set(SERVER_CLASSES, invalid.getName()));
      assertThrows(
          IllegalArgumentException.class, () -> new TransportContext(conf, new EchoHandler()));
      assertEquals(1, CountingBootstrap.closed.get());
    }
  }

  private static CelebornConf configuration() {
    return new CelebornConf(false)
        .set("celeborn.shuffle.io.maxRetries", "1")
        .set("celeborn.shuffle.io.numConnectionsPerPeer", "1")
        .set("celeborn.shuffle.io.clientThreads", "1")
        .set("celeborn.shuffle.io.serverThreads", "1");
  }

  private static TransportConf transportConf(CelebornConf conf) {
    return new TransportConf("shuffle", conf);
  }

  private static SecretRegistryImpl registry(String secret) {
    SecretRegistryImpl registry = new SecretRegistryImpl();
    registry.register(APP, secret);
    return registry;
  }

  private static class EchoHandler extends BaseMessageHandler {
    @Override
    public boolean checkRegistered() {
      return true;
    }

    @Override
    public void receive(
        TransportClient client, RequestMessage message, RpcResponseCallback callback) {
      callback.onSuccess(JavaUtils.stringToBytes(client.getClientId()));
    }
  }

  public static class ConfiguredSaslClient extends SaslClientBootstrap {
    public ConfiguredSaslClient(TransportConf conf) {
      super(conf, APP, new SaslCredentials(APP, SECRET));
    }
  }

  public static class ConfiguredSaslServer extends SaslServerBootstrap {
    public ConfiguredSaslServer(TransportConf conf) {
      super(conf, registry(SECRET));
    }
  }

  public static class CountingBootstrap
      implements TransportClientBootstrap, TransportServerBootstrap {
    static final AtomicInteger created = new AtomicInteger();
    static final AtomicInteger closed = new AtomicInteger();
    static String module;

    public CountingBootstrap(TransportConf conf) {
      created.incrementAndGet();
      module = conf.getModuleName();
    }

    @Override
    public void doBootstrap(TransportClient client) {}

    @Override
    public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler handler) {
      return handler;
    }

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }

  public static class NoArgBootstrap implements TransportClientBootstrap {
    static final AtomicInteger created = new AtomicInteger();

    public NoArgBootstrap() {
      created.incrementAndGet();
    }

    @Override
    public void doBootstrap(TransportClient client) {}
  }

  public static class FailingBootstrap extends CountingBootstrap {
    public FailingBootstrap(TransportConf conf) {
      super(conf);
      throw new IllegalStateException("Cannot initialize bootstrap");
    }
  }
}
