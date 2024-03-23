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
import org.apache.celeborn.common.util.JavaUtils;

/**
 * A few negative tests to ensure that a non SSL client cant talk to an SSL server and vice versa.
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

  // This is a test to validate that this test suite is working fine - so that the negative
  // tests are not indicating an error
  @Test
  public void testNormalConnectivityWorks() throws Exception {
    testSuccessfulConnectivity(false,
        // does not matter what these are set to
        true, true,
        Function.identity(), Function.identity());
  }

  @Test
  public void testBasicSslClientConnectivityWorks() throws Exception {

    final Function<CelebornConf, CelebornConf> updateClientConf =
        conf -> {
          // ignore incoming conf, and return a new which only has ssl enabled = true
          CelebornConf newConf = new CelebornConf();
          newConf.set("celeborn.ssl." + TEST_MODULE + ".enabled", "true");
          return newConf;
        };

    testSuccessfulConnectivity(true, true, true, Function.identity(), updateClientConf);
    testSuccessfulConnectivity(true, true, false, Function.identity(), updateClientConf);

    // just for validation, this should fail (ssl for client, plain for server) ...
    testConnectivityFailure(false, true, false, false, Function.identity(), updateClientConf);
  }

  @Test
  public void testSslServerNormalClientFails() throws Exception {
    testConnectivityFailure(true, false, true, true, Function.identity(), Function.identity());
    testConnectivityFailure(true, false, false, true, Function.identity(), Function.identity());
  }

  @Test
  public void testSslClientNormalServerFails() throws Exception {
    testConnectivityFailure(false, true, false, false, Function.identity(), Function.identity());
    testConnectivityFailure(false, true, false, true, Function.identity(), Function.identity());
  }

  private void testSuccessfulConnectivity(
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
                    TEST_MODULE, enableSsl, primaryConfigForServer, postProcessServerConf),
                createTransportConf(
                    TEST_MODULE, enableSsl, primaryConfigForClient, postProcessClientConf));
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
    try (TestTransportState state =
            new TestTransportState(
                DEFAULT_HANDLER,
                createTransportConf(
                    TEST_MODULE, serverSsl, primaryConfigForServer, postProcessServerConf),
                createTransportConf(
                    TEST_MODULE, clientSsl, primaryConfigForClient, postProcessClientConf));
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
  public void testUntrustedServerCertFails() throws Exception {
    // since we are specifying for the exact module, this is fine
    final String trustStoreKey = "celeborn.ssl." + TEST_MODULE + ".trustStore";

    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          if (conf.getOption(trustStoreKey).isDefined()) {
            conf.set(trustStoreKey, SslSampleConfigs.TRUST_STORE_WITHOUT_CA);
          }
          return conf;
        };

    // will fail for all combinations - since we dont have the CA's in the truststore
    testConnectivityFailure(true, true, true, false, updateConf, updateConf);
    testConnectivityFailure(true, true, false, true, updateConf, updateConf);
    testConnectivityFailure(true, true, true, true, updateConf, updateConf);
    testConnectivityFailure(true, true, false, false, updateConf, updateConf);
  }

  @Test
  public void testUntrustedServerCertWorksIfTrustStoreDisabled() throws Exception {
    // Same as testUntrustedServerCertFails, but remove the truststore - which should result in
    // accepting all certs. Note, for jks at client side

    final String trustStoreKey = "celeborn.ssl." + TEST_MODULE + ".trustStore";

    final Function<CelebornConf, CelebornConf> updateConf =
        conf -> {
          if (conf.getOption(trustStoreKey).isDefined()) {
            conf.unset(trustStoreKey);
          }
          return conf;
        };

    // checking nettyssl == false at both client and server does not make sense in this context
    // it is the same cert for both :)
    testSuccessfulConnectivity(true, true, true, updateConf, updateConf);
    testSuccessfulConnectivity(true, true, false, updateConf, updateConf);
    testSuccessfulConnectivity(true, false, true, updateConf, updateConf);
    testSuccessfulConnectivity(true, false, false, updateConf, updateConf);
  }
}
