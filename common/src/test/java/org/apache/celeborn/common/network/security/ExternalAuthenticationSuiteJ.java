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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.tools.ToolProvider;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.OneWayMessage;
import org.apache.celeborn.common.network.protocol.PushData;
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

/** Loads a separately compiled plugin JAR and exercises it through real transport connections. */
public class ExternalAuthenticationSuiteJ {
  private static final String BOOTSTRAP = "example.security.TokenBootstrap";
  private static final String APP = "alpha/application";
  private static final String OTHER_APP = "beta/application";
  private static final String SAME_TENANT_APP = "alpha/another-application";
  private static final UserIdentifier ALPHA_USER = new UserIdentifier("alpha", "alice");
  private static final UserIdentifier BETA_USER = new UserIdentifier("beta", "bob");
  private static final String HOST = "localhost";

  @ClassRule public static final TemporaryFolder PLUGIN_FILES = new TemporaryFolder();
  @Rule public final TemporaryFolder credentials = new TemporaryFolder();

  private static URLClassLoader pluginLoader;
  private ClassLoader previousLoader;

  @BeforeClass
  public static void compileExternalPlugin() throws Exception {
    assertNotNull(
        "Compiling the external fixture requires a JDK", ToolProvider.getSystemJavaCompiler());
    Path directory = PLUGIN_FILES.getRoot().toPath();
    Path source = directory.resolve("TokenBootstrap.java");
    try (InputStream input =
        ExternalAuthenticationSuiteJ.class.getResourceAsStream(
            "/auth-plugin/TokenBootstrap.java")) {
      assertNotNull("External plugin source resource", input);
      Files.copy(input, source);
    }
    Path classes = Files.createDirectory(directory.resolve("plugin-classes"));
    // Exclude test outputs: the external fixture must compile only against production APIs.
    String classpath =
        Arrays.stream(System.getProperty("java.class.path").split(File.pathSeparator))
            .filter(
                path -> {
                  String name = new File(path).getName();
                  return !name.equals("test-classes") && !name.endsWith("-tests.jar");
                })
            .collect(Collectors.joining(File.pathSeparator));
    List<String> compilerArgs = new ArrayList<>();
    if ("1.8".equals(System.getProperty("java.specification.version"))) {
      compilerArgs.addAll(Arrays.asList("-source", "8", "-target", "8"));
    } else {
      compilerArgs.addAll(Arrays.asList("--release", "8"));
    }
    compilerArgs.addAll(
        Arrays.asList("-classpath", classpath, "-d", classes.toString(), source.toString()));
    assertEquals(
        "The external bootstrap must compile against production APIs",
        0,
        ToolProvider.getSystemJavaCompiler()
            .run(null, null, null, compilerArgs.toArray(new String[0])));
    Path jar = directory.resolve("authentication-plugin.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar));
        Stream<Path> entries = Files.walk(classes)) {
      for (Path entry : entries.filter(Files::isRegularFile).collect(Collectors.toList())) {
        output.putNextEntry(
            new JarEntry(classes.relativize(entry).toString().replace(File.separatorChar, '/')));
        Files.copy(entry, output);
        output.closeEntry();
      }
    }
    URL pluginJar = jar.toUri().toURL();
    ClassLoader parent = ExternalAuthenticationSuiteJ.class.getClassLoader();
    assertThrows(ClassNotFoundException.class, () -> Class.forName(BOOTSTRAP, false, parent));
    pluginLoader = new URLClassLoader(new URL[] {pluginJar}, parent);
    assertEquals(
        pluginJar,
        pluginLoader.loadClass(BOOTSTRAP).getProtectionDomain().getCodeSource().getLocation());
  }

  @AfterClass
  public static void closePluginLoader() throws Exception {
    if (pluginLoader != null) {
      pluginLoader.close();
    }
  }

  @Before
  public void usePluginLoader() {
    previousLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(pluginLoader);
  }

  @After
  public void restoreClassLoader() {
    Thread.currentThread().setContextClassLoader(previousLoader);
  }

  @Test
  public void tokenAloneAuthorizesEveryMessageFormWithoutNativeIdentity() throws Exception {
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(conf(), handler);
        TransportContext alphaContext =
            new TransportContext(clientConf("alpha-token"), new BaseMessageHandler());
        TransportContext betaContext =
            new TransportContext(clientConf("beta-token"), new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0);
        TransportClientFactory alphaFactory = alphaContext.createClientFactory();
        TransportClientFactory betaFactory = betaContext.createClientFactory();
        TransportClient alpha = alphaFactory.createClient(HOST, server.getPort());
        TransportClient beta = betaFactory.createClient(HOST, server.getPort())) {
      String alphaUser = exchange(alpha, APP);
      assertNull(
          "The plugin must not bind the native application ID", handler.lastClient.getClientId());
      ConnectionSecurityContext alphaIdentity = handler.lastClient.getSecurityContext();
      assertNotNull("The plugin retains its own verified identity", alphaIdentity);
      assertEquals(ALPHA_USER.toString(), alphaUser);
      assertEquals(BETA_USER.toString(), exchange(beta, OTHER_APP));
      assertNull(handler.lastClient.getClientId());
      assertNotSame(alphaIdentity, handler.lastClient.getSecurityContext());
      assertRejected(alpha, OTHER_APP);
      assertRejected(beta, APP);
      sendOneWayAndData(alpha, APP);
      // A reply on this channel is a fence for the preceding one-way and data frames.
      assertEquals(ALPHA_USER.toString(), exchange(alpha, APP));
      assertSame(alphaIdentity, handler.lastClient.getSecurityContext());
      assertEquals(3, handler.acceptedRpc.get());
      assertEquals(1, handler.oneWays.get());
      assertEquals(1, handler.pushes.get());
    }
  }

  @Test
  public void configuredChallengesFollowListedOrderBeforeNativeSasl() throws Exception {
    for (boolean nativeSasl : new boolean[] {false, true}) {
      String orderedBootstraps = BOOTSTRAP + "$First," + BOOTSTRAP + "$Second";
      TransportConf serverConf = conf();
      serverConf
          .getCelebornConf()
          .set("celeborn.rpc_service.server.bootstrap.classes", orderedBootstraps);
      TransportConf clientConf = clientConf("alpha-token");
      clientConf
          .getCelebornConf()
          .set("celeborn.rpc_service.client.bootstrap.classes", orderedBootstraps);
      SecretRegistryImpl registry = new SecretRegistryImpl();
      registry.register(APP, "native-secret");
      List<TransportServerBootstrap> nativeServer =
          nativeSasl
              ? Collections.singletonList(new SaslServerBootstrap(serverConf, registry))
              : Collections.emptyList();
      List<TransportClientBootstrap> nativeClient =
          nativeSasl
              ? Collections.singletonList(
                  new SaslClientBootstrap(
                      clientConf, APP, new SaslCredentials(APP, "native-secret")))
              : Collections.emptyList();
      EchoHandler handler = new EchoHandler();
      try (TransportContext serverContext = new TransportContext(serverConf, handler);
          TransportContext clientContext =
              new TransportContext(clientConf, new BaseMessageHandler());
          TransportServer server = serverContext.createServer(HOST, 0, nativeServer);
          TransportClientFactory factory = clientContext.createClientFactory(nativeClient);
          TransportClient client = factory.createClient(HOST, server.getPort())) {
        assertEquals(ALPHA_USER.toString(), exchange(client, APP));
        assertEquals(nativeSasl ? APP : null, handler.lastClient.getClientId());
        assertNotNull(handler.lastClient.getSecurityContext());
        assertEquals(
            "FIRST,SECOND",
            clientConf.getCelebornConf().get("celeborn.test.token.client.completed"));
        assertEquals(
            "FIRST,SECOND",
            serverConf.getCelebornConf().get("celeborn.test.token.server.completed"));
        if (nativeSasl) {
          assertEquals(APP, client.getClientId());
        }
      }
    }
  }

  @Test
  public void laterHandshakeCannotReplaceExternalIdentity() throws Exception {
    TransportConf serverConf = conf();
    serverConf
        .getCelebornConf()
        .set(
            "celeborn.rpc_service.server.bootstrap.classes",
            BOOTSTRAP + "$First," + BOOTSTRAP + "$Second");
    // The two subjects deliberately have the same grants; a grant match is not an identity match.
    serverConf.getCelebornConf().set("celeborn.test.token.beta-token.appId", APP);
    serverConf
        .getCelebornConf()
        .set("celeborn.test.token.beta-token.tenant", ALPHA_USER.tenantId());
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext rawContext =
            new TransportContext(withoutPlugin(), new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0);
        TransportClientFactory factory = rawContext.createClientFactory();
        TransportClient client = factory.createClient(HOST, server.getPort())) {
      assertEquals("OK", exchange(client, "FIRST alpha-token"));
      assertRejected(client, APP);
      assertEquals(0, handler.invocations.get());
      IOException failure = assertRejected(client, "SECOND beta-token");
      Throwable cause = failure;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage(), cause.getMessage().contains("Conflicting token identity"));
      assertEquals(0, handler.invocations.get());
      assertEquals("OK", exchange(client, "SECOND alpha-token"));
      assertEquals(ALPHA_USER.toString(), exchange(client, APP));
      assertNull(handler.lastClient.getClientId());
      assertRejected(client, OTHER_APP);
    }
  }

  @Test
  public void verifierCannotAuthenticateWithoutTheContextOwner() throws Exception {
    for (String bootstraps :
        new String[] {BOOTSTRAP + "$Second", BOOTSTRAP + "$Second," + BOOTSTRAP + "$First"}) {
      TransportConf serverConf = conf();
      serverConf.getCelebornConf().set("celeborn.rpc_service.server.bootstrap.classes", bootstraps);
      EchoHandler handler = new EchoHandler();
      try (TransportContext serverContext = new TransportContext(serverConf, handler);
          TransportContext rawContext =
              new TransportContext(withoutPlugin(), new BaseMessageHandler());
          TransportServer server = serverContext.createServer(HOST, 0);
          TransportClientFactory factory = rawContext.createClientFactory();
          TransportClient client = factory.createClient(HOST, server.getPort())) {
        assertRejected(client, "SECOND alpha-token");
        assertEquals(0, handler.invocations.get());
      }
    }
  }

  @Test
  public void validNativeCredentialsOutsideTokenGrantCannotAuthorizeBusiness() throws Exception {
    TransportConf serverConf = conf();
    TransportConf clientConf = clientConf("alpha-token");
    SecretRegistryImpl registry = new SecretRegistryImpl();
    registry.register(OTHER_APP, "native-secret");
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                HOST, 0, Collections.singletonList(new SaslServerBootstrap(serverConf, registry)));
        TransportClientFactory factory =
            clientContext.createClientFactory(
                Collections.singletonList(
                    new SaslClientBootstrap(
                        clientConf, OTHER_APP, new SaslCredentials(OTHER_APP, "native-secret"))));
        TransportClient client = factory.createClient(HOST, server.getPort())) {
      // Both handshakes finish; their association is checked before each business operation.
      assertEquals(OTHER_APP, client.getClientId());
      assertRejected(client, OTHER_APP);
      sendOneWayAndData(client, APP);
      assertRejected(client, APP);
      assertEquals(0, handler.acceptedRpc.get());
      assertEquals(0, handler.oneWays.get());
      assertEquals(0, handler.pushes.get());
    }
  }

  @Test
  public void tokenComposesWithRuntimeRegistrationAndSaslOnReconnect() throws Exception {
    TransportConf serverConf = conf();
    TransportConf clientConf = clientConf("alpha-token");
    String secret = UUID.randomUUID().toString();
    SecretRegistryImpl registry = new SecretRegistryImpl();
    RegistrationInfo registration = new RegistrationInfo();
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                HOST,
                0,
                Collections.singletonList(new RegistrationServerBootstrap(serverConf, registry)))) {
      for (int attempt = 0; attempt < 2; attempt++) {
        try (TransportClientFactory factory =
                clientContext.createClientFactory(
                    Collections.singletonList(
                        new RegistrationClientBootstrap(
                            clientConf, APP, new SaslCredentials(APP, secret), registration)));
            TransportClient client = factory.createClient(HOST, server.getPort())) {
          assertEquals(APP, client.getClientId());
          assertEquals(secret, registry.getSecretKey(APP));
          assertEquals(
              RegistrationInfo.RegistrationState.REGISTERED, registration.getRegistrationState());
          assertEquals(ALPHA_USER.toString(), exchange(client, APP));
          assertEquals(APP, handler.lastClient.getClientId());
          assertNotNull(handler.lastClient.getSecurityContext());
          assertRejected(client, OTHER_APP);
        }
      }
    }
  }

  @Test
  public void invalidTokenDoesNotRegisterAnApplicationAndFactoryCanRetry() throws Exception {
    TransportConf serverConf = conf();
    TransportConf clientConf = clientConf("invalid-token");
    SecretRegistryImpl registry = new SecretRegistryImpl();
    RegistrationInfo registration = new RegistrationInfo();
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                HOST,
                0,
                Collections.singletonList(new RegistrationServerBootstrap(serverConf, registry)));
        TransportClientFactory factory =
            clientContext.createClientFactory(
                Collections.singletonList(
                    new RegistrationClientBootstrap(
                        clientConf,
                        APP,
                        new SaslCredentials(APP, "runtime-secret"),
                        registration)))) {
      assertThrows(RuntimeException.class, () -> factory.createClient(HOST, server.getPort()));
      assertNull("Bad token must not reach native registration", registry.getSecretKey(APP));
      assertEquals(0, handler.invocations.get());
      writeToken(clientConf, "alpha-token");
      try (TransportClient client = factory.createClient(HOST, server.getPort())) {
        assertEquals(ALPHA_USER.toString(), exchange(client, APP));
      }
    }
  }

  @Test
  public void unauthenticatedRpcOneWayAndDataNeverReachTheDelegate() throws Exception {
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(conf(), handler);
        TransportContext rawContext =
            new TransportContext(withoutPlugin(), new BaseMessageHandler());
        TransportContext authenticatedContext =
            new TransportContext(clientConf("alpha-token"), new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0);
        TransportClientFactory rawFactory = rawContext.createClientFactory();
        TransportClientFactory authenticatedFactory = authenticatedContext.createClientFactory();
        TransportClient raw = rawFactory.createClient(HOST, server.getPort())) {
      sendOneWayAndData(raw, APP);
      IOException failure = assertRejected(raw, APP);
      Throwable cause = failure;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      assertTrue(cause.getMessage(), cause.getMessage().contains("Invalid token"));
      assertEquals(
          "Rejected frames must not invoke either delegate overload", 0, handler.invocations.get());
      try (TransportClient valid = authenticatedFactory.createClient(HOST, server.getPort())) {
        assertEquals(ALPHA_USER.toString(), exchange(valid, APP));
      }
    }
  }

  @Test
  public void validTokenCannotBypassWrongOrMissingNativeAuthentication() throws Exception {
    TransportConf serverConf = conf();
    TransportConf clientConf = clientConf("alpha-token");
    SecretRegistryImpl registry = new SecretRegistryImpl();
    registry.register(APP, "correct-secret");
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                HOST, 0, Collections.singletonList(new SaslServerBootstrap(serverConf, registry)));
        TransportClientFactory wrongSecretFactory =
            clientContext.createClientFactory(
                Collections.singletonList(
                    new SaslClientBootstrap(
                        clientConf, APP, new SaslCredentials(APP, "wrong-secret"))));
        TransportClientFactory tokenOnlyFactory = clientContext.createClientFactory()) {
      assertThrows(
          RuntimeException.class, () -> wrongSecretFactory.createClient(HOST, server.getPort()));
      try (TransportClient partial = tokenOnlyFactory.createClient(HOST, server.getPort())) {
        sendOneWayAndData(partial, APP);
        assertRejected(partial, APP);
      }
      assertEquals(
          "Partial authentication must not invoke either delegate overload",
          0,
          handler.invocations.get());
    }
  }

  @Test
  public void nativeOnlyPeerCannotBypassTheConfiguredTokenHandshake() throws Exception {
    TransportConf serverConf = conf();
    TransportConf nativeConf = withoutPlugin();
    SecretRegistryImpl registry = new SecretRegistryImpl();
    registry.register(APP, "correct-secret");
    EchoHandler handler = new EchoHandler();
    try (TransportContext serverContext = new TransportContext(serverConf, handler);
        TransportContext nativeContext =
            new TransportContext(nativeConf, new BaseMessageHandler());
        TransportServer server =
            serverContext.createServer(
                HOST, 0, Collections.singletonList(new SaslServerBootstrap(serverConf, registry)));
        TransportClientFactory nativeFactory =
            nativeContext.createClientFactory(
                Collections.singletonList(
                    new SaslClientBootstrap(
                        nativeConf, APP, new SaslCredentials(APP, "correct-secret"))))) {
      assertThrows(
          RuntimeException.class, () -> nativeFactory.createClient(HOST, server.getPort()));
      assertEquals(0, handler.invocations.get());
    }
  }

  @Test
  public void configuredBootstrapDoesNotInventNativeSaslForAnInternalContext() throws Exception {
    TransportConf serverConf = conf();
    serverConf.getCelebornConf().set("celeborn.auth.enabled", "true");
    serverConf.getCelebornConf().set("celeborn.internal.port.enabled", "true");
    try (TransportContext serverContext = new TransportContext(serverConf, new EchoHandler());
        TransportContext clientContext =
            new TransportContext(clientConf("alpha-token"), new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0);
        TransportClientFactory factory = clientContext.createClientFactory();
        TransportClient client = factory.createClient(HOST, server.getPort())) {
      assertEquals(ALPHA_USER.toString(), exchange(client, APP));
    }
  }

  @Test
  public void newConnectionsReadRotatedCredentialsWithoutChangingExistingIdentity()
      throws Exception {
    TransportConf clientConf = clientConf("alpha-token");
    try (TransportContext serverContext = new TransportContext(conf(), new EchoHandler());
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0);
        TransportClientFactory factory = clientContext.createClientFactory();
        TransportClientFactory otherFactory = clientContext.createClientFactory();
        TransportClient existing = factory.createClient(HOST, server.getPort())) {
      assertEquals(ALPHA_USER.toString(), exchange(existing, APP));
      writeToken(clientConf, "beta-token");
      try (TransportClient rotated = otherFactory.createClient(HOST, server.getPort())) {
        assertEquals(BETA_USER.toString(), exchange(rotated, OTHER_APP));
        assertRejected(rotated, APP);
        assertEquals(ALPHA_USER.toString(), exchange(existing, APP));
        assertRejected(existing, OTHER_APP);
      }
      existing.close();
      try (TransportClient reconnected = factory.createClient(HOST, server.getPort())) {
        assertNotSame(existing, reconnected);
        assertEquals(BETA_USER.toString(), exchange(reconnected, OTHER_APP));
        assertRejected(reconnected, APP);
      }
    }
  }

  @Test
  public void tenantPolicyWorksAloneAndAfterNativeAuthentication() throws Exception {
    for (boolean nativeSasl : new boolean[] {false, true}) {
      TransportConf serverConf = conf();
      serverConf.getCelebornConf().set("celeborn.test.token.policy", "tenant");
      TransportConf clientConf = clientConf("alpha-token");
      SecretRegistryImpl registry = new SecretRegistryImpl();
      registry.register(SAME_TENANT_APP, "native-secret");
      EchoHandler handler = new EchoHandler();
      try (TransportContext serverContext = new TransportContext(serverConf, handler);
          TransportContext clientContext =
              new TransportContext(clientConf, new BaseMessageHandler());
          TransportServer server =
              serverContext.createServer(
                  HOST,
                  0,
                  nativeSasl
                      ? Collections.singletonList(new SaslServerBootstrap(serverConf, registry))
                      : Collections.emptyList());
          TransportClientFactory factory =
              clientContext.createClientFactory(
                  nativeSasl
                      ? Collections.singletonList(
                          new SaslClientBootstrap(
                              clientConf,
                              SAME_TENANT_APP,
                              new SaslCredentials(SAME_TENANT_APP, "native-secret")))
                      : Collections.emptyList());
          TransportClient client = factory.createClient(HOST, server.getPort())) {
        assertEquals(ALPHA_USER.toString(), exchange(client, SAME_TENANT_APP));
        assertRejected(client, OTHER_APP);
        sendOneWayAndData(client, SAME_TENANT_APP);
        assertEquals(ALPHA_USER.toString(), exchange(client, APP));
        assertEquals(nativeSasl ? SAME_TENANT_APP : null, handler.lastClient.getClientId());
        assertNotNull(handler.lastClient.getSecurityContext());
        assertEquals(nativeSasl ? SAME_TENANT_APP : null, client.getClientId());
        assertEquals(1, handler.oneWays.get());
        assertEquals(1, handler.pushes.get());
      }
    }
  }

  @Test
  public void conflictingNativeApplicationCannotWriteRegistrationSecret() throws Exception {
    assertRegistrationDeniedBeforeWrite(SAME_TENANT_APP, "tenant", true);
  }

  @Test
  public void pluginCanDenyRegistrationBeforeAnySecretIsStored() throws Exception {
    assertRegistrationDeniedBeforeWrite(APP, "deny-registration", false);
  }

  @Test
  public void tokenCannotRegisterAnUnauthorizedApplication() throws Exception {
    assertRegistrationDeniedBeforeWrite(OTHER_APP, "application", false);
  }

  private void assertRegistrationDeniedBeforeWrite(
      String registeredApp, String policy, boolean authenticateNativeApp) throws Exception {
    TransportConf serverConf = conf();
    serverConf.getCelebornConf().set("celeborn.test.token.policy", policy);
    TransportConf clientConf = clientConf("alpha-token");
    SecretRegistryImpl registry = new SecretRegistryImpl();
    List<TransportServerBootstrap> serverBootstraps = new ArrayList<>();
    serverBootstraps.add(new RegistrationServerBootstrap(serverConf, registry));
    List<TransportClientBootstrap> clientBootstraps = new ArrayList<>();
    if (authenticateNativeApp) {
      registry.register(APP, "native-secret");
      // Complete real native authentication before attempting to register another application.
      serverBootstraps.add(new SaslServerBootstrap(serverConf, registry));
      clientBootstraps.add(
          new SaslClientBootstrap(clientConf, APP, new SaslCredentials(APP, "native-secret")));
    }
    clientBootstraps.add(
        new RegistrationClientBootstrap(
            clientConf,
            registeredApp,
            new SaslCredentials(registeredApp, "must-not-be-written"),
            new RegistrationInfo()));
    try (TransportContext serverContext = new TransportContext(serverConf, new EchoHandler());
        TransportContext clientContext =
            new TransportContext(clientConf, new BaseMessageHandler());
        TransportServer server = serverContext.createServer(HOST, 0, serverBootstraps);
        TransportClientFactory factory = clientContext.createClientFactory(clientBootstraps)) {
      RuntimeException rejected = null;
      try (TransportClient ignored = factory.createClient(HOST, server.getPort())) {
        // Assert the registry first: even a later handshake failure must not leave a secret.
      } catch (RuntimeException failure) {
        rejected = failure;
      }
      assertNull(
          "Denied registration must not modify the secret registry",
          registry.getSecretKey(registeredApp));
      if (authenticateNativeApp) {
        assertEquals("native-secret", registry.getSecretKey(APP));
      }
      assertNotNull("The caller must observe registration denial", rejected);
      for (Throwable cause = rejected; cause != null; cause = cause.getCause()) {
        assertFalse(
            "A timeout is not an authorization rejection", cause instanceof TimeoutException);
      }
    }
  }

  private static TransportConf conf() {
    return new TransportConf(
        "rpc_service",
        new CelebornConf(false)
            .set("celeborn.rpc_service.client.bootstrap.classes", BOOTSTRAP)
            .set("celeborn.rpc_service.server.bootstrap.classes", BOOTSTRAP)
            .set("celeborn.rpc_service.io.clientThreads", "1")
            .set("celeborn.rpc_service.io.serverThreads", "1")
            .set("celeborn.rpc_service.io.numConnectionsPerPeer", "1")
            .set("celeborn.rpc_service.io.maxRetries", "1")
            .set("celeborn.test.token.alpha-token.appId", APP)
            .set("celeborn.test.token.alpha-token.tenant", ALPHA_USER.tenantId())
            .set("celeborn.test.token.alpha-token.principal", ALPHA_USER.name())
            .set("celeborn.test.token.beta-token.appId", OTHER_APP)
            .set("celeborn.test.token.beta-token.tenant", BETA_USER.tenantId())
            .set("celeborn.test.token.beta-token.principal", BETA_USER.name()));
  }

  private static TransportConf withoutPlugin() {
    TransportConf conf = conf();
    conf.getCelebornConf().set("celeborn.rpc_service.client.bootstrap.classes", "");
    conf.getCelebornConf().set("celeborn.rpc_service.server.bootstrap.classes", "");
    return conf;
  }

  private TransportConf clientConf(String token) throws IOException {
    TransportConf conf = conf();
    conf.getCelebornConf().set("celeborn.test.token.file", credentials.newFile().getAbsolutePath());
    writeToken(conf, token);
    return conf;
  }

  private static void writeToken(TransportConf conf, String token) throws IOException {
    Files.write(
        new File(conf.getCelebornConf().get("celeborn.test.token.file")).toPath(),
        token.getBytes(StandardCharsets.UTF_8));
  }

  private static String exchange(TransportClient client, String appId) throws IOException {
    return JavaUtils.bytesToString(client.sendRpcSync(JavaUtils.stringToBytes(appId), 10000));
  }

  private static IOException assertRejected(TransportClient client, String appId) {
    IOException failure = assertThrows(IOException.class, () -> exchange(client, appId));
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      assertFalse(
          "A timeout is not an authentication rejection", cause instanceof TimeoutException);
    }
    return failure;
  }

  private static void sendOneWayAndData(TransportClient client, String appId) {
    client.send(JavaUtils.stringToBytes(appId));
    client
        .getChannel()
        .writeAndFlush(
            new PushData(
                (byte) 0,
                appId + "-shuffle",
                "partition",
                new NioManagedBuffer(JavaUtils.stringToBytes(appId))))
        .syncUninterruptibly();
  }

  private static class EchoHandler extends BaseMessageHandler {
    final AtomicInteger invocations = new AtomicInteger();
    final AtomicInteger acceptedRpc = new AtomicInteger();
    final AtomicInteger oneWays = new AtomicInteger();
    final AtomicInteger pushes = new AtomicInteger();
    volatile TransportClient lastClient;

    @Override
    public boolean checkRegistered() {
      return true;
    }

    @Override
    public void receive(
        TransportClient client, RequestMessage message, RpcResponseCallback callback) {
      invocations.incrementAndGet();
      try {
        UserIdentifier user =
            client.authorize(
                AuthorizationRequest.forApplication(
                    SecurityOperation.APPLICATION_ACCESS,
                    JavaUtils.bytesToString(message.body().nioByteBuffer())));
        lastClient = client;
        acceptedRpc.incrementAndGet();
        callback.onSuccess(JavaUtils.stringToBytes(String.valueOf(user)));
      } catch (IOException e) {
        callback.onFailure(e);
      }
    }

    @Override
    public void receive(TransportClient client, RequestMessage message) {
      invocations.incrementAndGet();
      try {
        checkAuth(client, JavaUtils.bytesToString(message.body().nioByteBuffer()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (message instanceof OneWayMessage) {
        oneWays.incrementAndGet();
      } else if (message instanceof PushData) {
        pushes.incrementAndGet();
      } else {
        throw new IllegalArgumentException("Unexpected message: " + message.getClass().getName());
      }
    }
  }
}
