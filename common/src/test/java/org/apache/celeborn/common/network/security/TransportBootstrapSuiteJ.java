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

import java.io.IOException;
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
import org.apache.celeborn.common.network.sasl.registration.RegistrationClientBootstrap;
import org.apache.celeborn.common.network.sasl.registration.RegistrationInfo;
import org.apache.celeborn.common.network.sasl.registration.RegistrationServerBootstrap;
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
      assertEquals(APP, exchange(client, APP));
      assertEquals(1, clientCalls.get());
      assertEquals("The caller handler must not receive SASL handshakes", 1, serverRequests.get());
    }
  }

  @Test
  public void failedAuthenticationDoesNotPublishAPooledClient() throws Exception {
    TransportConf conf = transportConf(configuration());
    SecretRegistryImpl registry = registry(SECRET);
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(conf, handler);
        TransportContext clientContext = new TransportContext(conf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                "localhost",
                0,
                Collections.singletonList(new SaslServerBootstrap(conf, registry)));
        TransportClientFactory factory =
            clientContext.createClientFactory(
                Collections.singletonList(
                    new SaslClientBootstrap(
                        conf, APP, new SaslCredentials(APP, "wrong-secret"))))) {
      for (int attempt = 0; attempt < 2; attempt++) {
        assertThrows(
            RuntimeException.class, () -> factory.createClient("localhost", server.getPort()));
      }
      assertEquals(0, handler.requests.get());
      registry.unregister(APP);
      registry.register(APP, "wrong-secret");
      try (TransportClient client = factory.createClient("localhost", server.getPort())) {
        assertEquals(APP, exchange(client, APP));
      }
    }
  }

  @Test
  public void unconfiguredNativeSaslAndAnonymousConnectionsKeepTheirBehavior() throws Exception {
    TransportConf conf = transportConf(configuration());
    try (TransportContext context = new TransportContext(conf, new EchoHandler());
        TransportServer server =
            context.createServer(
                "localhost",
                0,
                Collections.singletonList(new SaslServerBootstrap(conf, registry(SECRET))));
        TransportClientFactory factory =
            context.createClientFactory(
                Collections.singletonList(
                    new SaslClientBootstrap(conf, APP, new SaslCredentials(APP, SECRET))));
        TransportClient client = factory.createClient("localhost", server.getPort())) {
      assertEquals(APP, exchange(client, APP));
      assertThrows(IOException.class, () -> exchange(client, "another-application"));
      assertEquals(APP, exchange(client, APP));
    }
    try (TransportContext context = new TransportContext(conf, new EchoHandler());
        TransportServer server = context.createServer("localhost", 0);
        TransportClientFactory factory = context.createClientFactory();
        TransportClient client = factory.createClient("localhost", server.getPort())) {
      assertEquals("anonymous", exchange(client, "any-application"));
    }
  }

  @Test
  public void registrationAndReconnectUseTheExistingBootstrapInterfaces() throws Exception {
    TransportConf conf = transportConf(configuration());
    SecretRegistryImpl registry = new SecretRegistryImpl();
    RegistrationInfo registration = new RegistrationInfo();
    try (TransportContext context = new TransportContext(conf, new EchoHandler());
        TransportServer server =
            context.createServer(
                "localhost",
                0,
                Collections.singletonList(new RegistrationServerBootstrap(conf, registry)))) {
      for (int attempt = 0; attempt < 2; attempt++) {
        try (TransportClientFactory factory =
                context.createClientFactory(
                    Collections.singletonList(
                        new RegistrationClientBootstrap(
                            conf, APP, new SaslCredentials(APP, SECRET), registration)));
            TransportClient client = factory.createClient("localhost", server.getPort())) {
          assertEquals(APP, exchange(client, APP));
          assertEquals(SECRET, registry.getSecretKey(APP));
          assertEquals(
              RegistrationInfo.RegistrationState.REGISTERED, registration.getRegistrationState());
          assertThrows(IOException.class, () -> exchange(client, "another-application"));
        }
      }
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
  public void invalidClassOrConstructorFailsWithoutIgnoringConfiguration() {
    for (String className :
        new String[] {String.class.getName(), FailingBootstrap.class.getName()}) {
      TransportConf conf = transportConf(configuration().set(CLIENT_CLASSES, className));
      assertThrows(RuntimeException.class, () -> new TransportContext(conf, new EchoHandler()));
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
  public void constructorFailureClosesPreviouslyCreatedBootstraps() {
    CountingBootstrap.closed.set(0);
    TransportConf conf =
        transportConf(
            configuration()
                .set(CLIENT_CLASSES, CountingBootstrap.class.getName())
                .set(SERVER_CLASSES, FailingBootstrap.class.getName()));
    assertThrows(RuntimeException.class, () -> new TransportContext(conf, new EchoHandler()));
    assertEquals(1, CountingBootstrap.closed.get());
  }

  @Test
  public void wrongBootstrapTypeIsRejectedBeforeConstruction() {
    ServerOnlyBootstrap.created.set(0);
    TransportConf conf =
        transportConf(configuration().set(CLIENT_CLASSES, ServerOnlyBootstrap.class.getName()));
    assertThrows(
        IllegalArgumentException.class, () -> new TransportContext(conf, new EchoHandler()));
    assertEquals(0, ServerOnlyBootstrap.created.get());
  }

  @Test
  public void classInitializationFailureClosesPreviouslyCreatedBootstraps() {
    CountingBootstrap.closed.set(0);
    TransportConf conf =
        transportConf(
            configuration()
                .set(CLIENT_CLASSES, CountingBootstrap.class.getName())
                .set(SERVER_CLASSES, InitializationFailureBootstrap.class.getName()));
    assertThrows(LinkageError.class, () -> new TransportContext(conf, new EchoHandler()));
    assertEquals(1, CountingBootstrap.closed.get());
  }

  @Test
  public void failingCloseDoesNotPreventOtherBootstrapCleanup() {
    CountingBootstrap.closed.set(0);
    CloseFailureBootstrap.closed.set(0);
    TransportConf conf =
        transportConf(
            configuration()
                .set(
                    CLIENT_CLASSES,
                    CountingBootstrap.class.getName()
                        + ","
                        + CloseFailureBootstrap.class.getName()));
    try (TransportContext ignored = new TransportContext(conf, new EchoHandler())) {
      // The context closes every configured instance, even if one cleanup fails.
    }
    assertEquals(1, CloseFailureBootstrap.closed.get());
    assertEquals(1, CountingBootstrap.closed.get());
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

  private static String exchange(TransportClient client, String appId) throws IOException {
    return JavaUtils.bytesToString(client.sendRpcSync(JavaUtils.stringToBytes(appId), 10000));
  }

  private static class EchoHandler extends BaseMessageHandler {
    final AtomicInteger requests = new AtomicInteger();

    @Override
    public boolean checkRegistered() {
      return true;
    }

    @Override
    public void receive(
        TransportClient client, RequestMessage message, RpcResponseCallback callback) {
      try {
        checkAuth(client, JavaUtils.bytesToString(message.body().nioByteBuffer()));
        requests.incrementAndGet();
        callback.onSuccess(
            JavaUtils.stringToBytes(
                client.getClientId() == null ? "anonymous" : client.getClientId()));
      } catch (IOException e) {
        callback.onFailure(e);
      }
    }
  }

  public static class ConfiguredSaslClient implements TransportClientBootstrap {
    private final SaslClientBootstrap delegate;

    public ConfiguredSaslClient(TransportConf conf) {
      delegate = new SaslClientBootstrap(conf, APP, new SaslCredentials(APP, SECRET));
    }

    @Override
    public void doBootstrap(TransportClient client) {
      delegate.doBootstrap(client);
    }
  }

  public static class ConfiguredSaslServer implements TransportServerBootstrap {
    private final SaslServerBootstrap delegate;

    public ConfiguredSaslServer(TransportConf conf) {
      delegate = new SaslServerBootstrap(conf, registry(SECRET));
    }

    @Override
    public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler handler) {
      return delegate.doBootstrap(channel, handler);
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

  public static class ServerOnlyBootstrap implements TransportServerBootstrap {
    static final AtomicInteger created = new AtomicInteger();

    public ServerOnlyBootstrap() {
      created.incrementAndGet();
    }

    @Override
    public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler handler) {
      return handler;
    }
  }

  public static class InitializationFailureBootstrap implements TransportServerBootstrap {
    static {
      if (Boolean.parseBoolean("true")) {
        throw new ExceptionInInitializerError("Cannot initialize bootstrap class");
      }
    }

    @Override
    public BaseMessageHandler doBootstrap(Channel channel, BaseMessageHandler handler) {
      return handler;
    }
  }

  public static class FailingBootstrap extends CountingBootstrap {
    public FailingBootstrap(TransportConf conf) {
      super(conf);
      throw new IllegalStateException("Cannot initialize bootstrap");
    }
  }

  public static class CloseFailureBootstrap extends NoArgBootstrap {
    static final AtomicInteger closed = new AtomicInteger();

    @Override
    public void close() {
      closed.incrementAndGet();
      throw new IllegalStateException("Cannot close bootstrap");
    }
  }
}
