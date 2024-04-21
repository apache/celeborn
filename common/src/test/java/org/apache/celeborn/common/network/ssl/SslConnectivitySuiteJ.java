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

package org.apache.celeborn.common.network.ssl;

import static org.apache.celeborn.common.util.JavaUtils.getLocalHost;
import static org.junit.Assert.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.TestHelper;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.protocol.TransportModuleConstants;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * A few negative tests to ensure that a non SSL client cant talk to an SSL server and vice versa.
 * Also, a few tests to ensure non-SSL client and servers can talk to each other, SSL client and
 * server also can talk to each other.
 */
public class SslConnectivitySuiteJ {

  private static final String TEST_MODULE = "rpc";

  private static final String RESPONSE_PREFIX = "Test-prefix...";
  private static final String RESPONSE_SUFFIX = "...Suffix";

  private static final TestBaseMessageHandler DEFAULT_HANDLER = new TestBaseMessageHandler();

  private static TransportConf createTransportConf(
      String module,
      boolean enableSsl,
      boolean useDefault,
      Function<CelebornConf, CelebornConf> postProcessConf) {

    CelebornConf celebornConf = new CelebornConf();
    // in case the default gets flipped to true in future
    celebornConf.set("celeborn.ssl." + module + ".enabled", "false");
    if (enableSsl) {
      Map<String, String> configMap;
      if (useDefault) {
        configMap = SslSampleConfigs.createDefaultConfigMapForModule(module);
      } else {
        configMap = SslSampleConfigs.createAnotherConfigMapForModule(module);
      }
      TestHelper.updateCelebornConfWithMap(celebornConf, configMap);
    }

    celebornConf = postProcessConf.apply(celebornConf);

    TransportConf conf = new TransportConf(module, celebornConf);
    assertEquals(enableSsl, conf.sslEnabled());

    return conf;
  }

  private static class TestTransportState implements Closeable {
    final TransportContext serverContext;
    final TransportContext clientContext;
    final TransportServer server;
    final TransportClientFactory clientFactory;

    TestTransportState(
        BaseMessageHandler handler, TransportConf serverConf, TransportConf clientConf) {
      this.serverContext = new TransportContext(serverConf, handler);
      this.clientContext = new TransportContext(clientConf, handler);

      this.server = serverContext.createServer();
      this.clientFactory = clientContext.createClientFactory();
    }

    TransportClient createClient() throws IOException, InterruptedException {
      return clientFactory.createClient(getLocalHost(), server.getPort());
    }

    @Override
    public void close() throws IOException {
      JavaUtils.closeQuietly(server);
      JavaUtils.closeQuietly(clientFactory);
      JavaUtils.closeQuietly(serverContext);
      JavaUtils.closeQuietly(clientContext);
    }
  }

  private static class TestBaseMessageHandler extends BaseMessageHandler {

    @Override
    public void receive(
        TransportClient client, RequestMessage requestMessage, RpcResponseCallback callback) {
      try {
        String msg = JavaUtils.bytesToString(requestMessage.body().nioByteBuffer());
        String response = RESPONSE_PREFIX + msg + RESPONSE_SUFFIX;
        callback.onSuccess(JavaUtils.stringToBytes(response));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean checkRegistered() {
      return true;
    }
  }

  // Pair<Success, Failure> ... should be an Either actually.

  private Pair<String, String> sendRPC(TransportClient client, String message, boolean canTimeout)
      throws Exception {
    final Semaphore sem = new Semaphore(0);

    final AtomicReference<String> response = new AtomicReference<>(null);
    final AtomicReference<String> errorResponse = new AtomicReference<>(null);

    RpcResponseCallback callback =
        new RpcResponseCallback() {
          @Override
          public void onSuccess(ByteBuffer message) {
            String res = JavaUtils.bytesToString(message);
            response.set(res);
            sem.release();
          }

          @Override
          public void onFailure(Throwable e) {
            errorResponse.set(e.getMessage());
            sem.release();
          }
        };

    client.sendRpc(JavaUtils.stringToBytes(message), callback);

    if (!sem.tryAcquire(1, 5, TimeUnit.SECONDS)) {
      if (canTimeout) {
        throw new IOException("Timed out sending rpc message");
      } else {
        fail("Timeout getting response from the server");
      }
    }

    return Pair.of(response.get(), errorResponse.get());
  }

  // A basic validation test to check if non-ssl client and non-ssl server can talk to each other.
  // This not only validates non-SSL flow, but also is a check to verify that the negative
  // tests are not indicating any other error.
  @Test
  public void testNormalConnectivityWorks() throws Exception {
    testSuccessfulConnectivity(
        false,
        // does not matter what these are set to
        true,
        true,
        Function.identity(),
        Function.identity());
  }

  @Test
  public void testNormalConnectivityWithAuthWorks() throws Exception {
    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          conf.set("celeborn.auth.enabled", "true");
          return conf;
        };

    testSuccessfulConnectivity(
        false,
        // does not matter what these are set to
        true,
        true,
        updateConf,
        updateConf);
  }

  // Both server and client are on SSL, and should be able to successfully communicate
  // This is the SSL version of testNormalConnectivityWorks above.
  @Test
  public void testBasicSslClientConnectivityWorks() throws Exception {

    final Function<CelebornConf, CelebornConf> updateClientConf =
        conf -> {
          // ignore incoming conf, and return a new which only has ssl enabled = true
          // This is essentially testing two things:
          // a) client can talk SSL to a server, and does not need anything else to be specified
          // b) When no truststore is configured at client, it is trusts all server certs
          CelebornConf newConf = new CelebornConf();
          newConf.set("celeborn.ssl." + TEST_MODULE + ".enabled", "true");
          return newConf;
        };

    // primaryConfigForClient param does not matter - we are completely overriding it above
    // in updateClientConf. Adding both just for completeness sake.
    testSuccessfulConnectivity(true, true, true, Function.identity(), updateClientConf);
    testSuccessfulConnectivity(true, true, false, Function.identity(), updateClientConf);

    // just for validation, this should fail (ssl for client, plain for server) ...
    testConnectivityFailure(false, true, false, false, Function.identity(), updateClientConf);
  }

  // Only SSL client can talk to a SSL server.
  @Test
  public void testSslServerNormalClientFails() throws Exception {
    // Will fail for both primary and seconday jks (primaryConfigForServer) - adding
    // both just for completeness
    testConnectivityFailure(true, false, true, true, Function.identity(), Function.identity());
    testConnectivityFailure(true, false, false, true, Function.identity(), Function.identity());
  }

  // Only non-SSL client can talk to SSL server
  @Test
  public void testSslClientNormalServerFails() throws Exception {
    // Will fail for both primaryConfigForClient - adding both just for completeness
    testConnectivityFailure(false, true, false, false, Function.identity(), Function.identity());
    testConnectivityFailure(false, true, false, true, Function.identity(), Function.identity());
  }

  @Test
  public void testUntrustedServerCertFails() throws Exception {
    final String trustStoreKey = "celeborn.ssl." + TEST_MODULE + ".trustStore";

    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          assertTrue(conf.getOption(trustStoreKey).isDefined());
          conf.set(trustStoreKey, SslSampleConfigs.TRUST_STORE_WITHOUT_CA);
          return conf;
        };

    // will fail for all combinations - since we dont have the CA's in the truststore
    testConnectivityFailure(true, true, true, false, updateConf, updateConf);
    testConnectivityFailure(true, true, true, true, updateConf, updateConf);
    testConnectivityFailure(true, true, false, true, updateConf, updateConf);
    testConnectivityFailure(true, true, false, false, updateConf, updateConf);
  }

  // This is a variant of testUntrustedServerCertFails - where the server does not trust the
  // client cert, and the client does provide a cert.
  @Test
  public void testUntrustedClientCertFails() throws Exception {
    final String trustStoreKey = "celeborn.ssl." + TEST_MODULE + ".trustStore";

    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          assertTrue(conf.getOption(trustStoreKey).isDefined());
          conf.set(trustStoreKey, SslSampleConfigs.TRUST_STORE_WITHOUT_CA);
          return conf;
        };

    // will fail for all combinations - since server does not have the client cert's CA
    // in its truststore
    testConnectivityFailure(true, true, true, false, updateConf, Function.identity());
    testConnectivityFailure(true, true, true, true, updateConf, Function.identity());
    testConnectivityFailure(true, true, false, true, updateConf, Function.identity());
    testConnectivityFailure(true, true, false, false, updateConf, Function.identity());
  }

  @Test
  public void testUntrustedServerCertWorksIfTrustStoreDisabled() throws Exception {
    // Same as testUntrustedServerCertFails, but remove the truststore - which should result in
    // accepting all certs. Note, for jks at client side

    final String trustStoreKey = "celeborn.ssl." + TEST_MODULE + ".trustStore";

    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          assertTrue(conf.getOption(trustStoreKey).isDefined());
          conf.unset(trustStoreKey);
          assertNull(new TransportConf(TEST_MODULE, conf).sslTrustStore());
          return conf;
        };

    testSuccessfulConnectivity(true, true, true, updateConf, updateConf);
    testSuccessfulConnectivity(true, true, false, updateConf, updateConf);
    testSuccessfulConnectivity(true, false, true, updateConf, updateConf);
    testSuccessfulConnectivity(true, false, false, updateConf, updateConf);
  }

  private void testSuccessfulConnectivity(
      boolean enableSsl,
      boolean primaryConfigForServer,
      boolean primaryConfigForClient,
      Function<CelebornConf, CelebornConf> postProcessServerConf,
      Function<CelebornConf, CelebornConf> postProcessClientConf)
      throws Exception {

    testSuccessfulConnectivity(
        TEST_MODULE,
        TEST_MODULE,
        enableSsl,
        primaryConfigForServer,
        primaryConfigForClient,
        postProcessServerConf,
        postProcessClientConf);
  }

  private void testSuccessfulConnectivity(
      String serverModule,
      String clientModule,
      boolean enableSsl,
      boolean primaryConfigForServer,
      boolean primaryConfigForClient,
      Function<CelebornConf, CelebornConf> postProcessServerConf,
      Function<CelebornConf, CelebornConf> postProcessClientConf)
      throws Exception {
    try (TestTransportState state =
            new TestTransportState(
                DEFAULT_HANDLER,
                createTransportConf(
                    serverModule, enableSsl, primaryConfigForServer, postProcessServerConf),
                createTransportConf(
                    clientModule, enableSsl, primaryConfigForClient, postProcessClientConf));
        TransportClient client = state.createClient()) {

      String msg = " hi ";
      Pair<String, String> response = sendRPC(client, msg, false);
      assertNotNull("Failed ? " + response.getRight(), response.getLeft());
      assertNull(response.getRight());
      assertEquals(RESPONSE_PREFIX + msg + RESPONSE_SUFFIX, response.getLeft());
    }
  }

  private void testConnectivityFailure(
      boolean serverSsl,
      boolean clientSsl,
      boolean primaryConfigForServer,
      boolean primaryConfigForClient,
      Function<CelebornConf, CelebornConf> postProcessServerConf,
      Function<CelebornConf, CelebornConf> postProcessClientConf)
      throws Exception {
    testConnectivityFailure(
        TEST_MODULE,
        serverSsl,
        clientSsl,
        primaryConfigForServer,
        primaryConfigForClient,
        postProcessServerConf,
        postProcessClientConf);
  }

  private void testConnectivityFailure(
      String module,
      boolean serverSsl,
      boolean clientSsl,
      boolean primaryConfigForServer,
      boolean primaryConfigForClient,
      Function<CelebornConf, CelebornConf> postProcessServerConf,
      Function<CelebornConf, CelebornConf> postProcessClientConf)
      throws Exception {
    try (TestTransportState state =
            new TestTransportState(
                DEFAULT_HANDLER,
                createTransportConf(
                    module, serverSsl, primaryConfigForServer, postProcessServerConf),
                createTransportConf(
                    module, clientSsl, primaryConfigForClient, postProcessClientConf));
        TransportClient client = state.createClient()) {

      String msg = " hi ";
      Pair<String, String> response = sendRPC(client, msg, true);
      assertNull(response.getLeft());
      assertNotNull(response.getRight());
    } catch (IOException ioEx) {
      // this is fine - expected to fail
    }
  }

  @Test
  public void testAutoSslConnectivity() throws Exception {

    // Mirror how user will configure - so configure module based on RPC_APP_MODULE
    // while create the server transport config for lifecycle manager (to match driver),
    // and client to app_client similar to executors

    final Function<CelebornConf, CelebornConf> updateServerConf =
        conf -> {
          // return a new config
          String module = TransportModuleConstants.RPC_APP_MODULE;
          CelebornConf celebornConf = new CelebornConf();
          celebornConf.set("celeborn.ssl." + module + ".enabled", "true");
          celebornConf.set("celeborn.ssl." + module + ".protocol", "TLSv1.2");
          celebornConf.set("celeborn.ssl." + module + ".autoSslEnabled", "true");
          return celebornConf;
        };

    final Function<CelebornConf, CelebornConf> updateClientConf =
        conf -> {
          // return a new config
          String module = TransportModuleConstants.RPC_APP_MODULE;
          CelebornConf celebornConf = new CelebornConf();
          celebornConf.set("celeborn.ssl." + module + ".enabled", "true");
          celebornConf.set("celeborn.ssl." + module + ".protocol", "TLSv1.2");
          return celebornConf;
        };

    testSuccessfulConnectivity(
        TransportModuleConstants.RPC_LIFECYCLEMANAGER_MODULE,
        TransportModuleConstants.RPC_APP_CLIENT_MODULE,
        true,
        true,
        true,
        updateServerConf,
        updateClientConf);
  }
}
