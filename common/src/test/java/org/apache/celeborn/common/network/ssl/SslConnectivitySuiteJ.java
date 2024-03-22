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

import com.google.common.collect.Sets;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.RpcIntegrationSuiteJ;
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
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.celeborn.common.util.JavaUtils.getLocalHost;
import static org.junit.Assert.*;


/**
 * A few negative tests to ensure that a non SSL client cant talk to an SSL server and
 * vice versa.
 */
public class SslConnectivitySuiteJ {

  private static final String TEST_MODULE = "rpc";

  private static final String RESPONSE_PREFIX = "Test-prefix...";
  private static final String RESPONSE_SUFFIX = "...Suffix";

  private static final TestBaseMessageHandler DEFAULT_HANDLER = new TestBaseMessageHandler();

  private static class TestTransportState implements Closeable {
    final TransportConf serverConf;
    final TransportConf clientConf;
    final TransportContext serverContext;
    final TransportContext clientContext;
    final TransportServer server;
    final TransportClientFactory clientFactory;

    TestTransportState(BaseMessageHandler handler, String module,
        boolean sslServer, boolean sslClient) {
      this.serverConf = createTransportConf(module, sslServer);
      this.clientConf = createTransportConf(module, sslClient);

      this.serverContext = new TransportContext(serverConf, handler);
      this.clientContext = new TransportContext(clientConf, handler);

      this.server = serverContext.createServer();
      this.clientFactory = clientContext.createClientFactory();
    }

    TransportClient createClient() throws IOException, InterruptedException {
      return clientFactory.createClient(getLocalHost(), server.getPort());
    }

    private TransportConf createTransportConf(String module, boolean enableSsl) {
      CelebornConf celebornConf = new CelebornConf();
      // in case the default gets flipped to true in future
      celebornConf.set("celeborn.ssl." + module + ".enabled", "false");
      if (enableSsl) {
        TestHelper.updateCelebornConfWithMap(celebornConf,
            SslSampleConfigs.createDefaultConfigMapForModule(module));
      }

      TransportConf conf = new TransportConf(module, celebornConf);

      assertEquals(enableSsl, conf.sslEnabled());
      return conf;
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

  private Pair<String, String> sendRPC(TransportClient client, String message,
      boolean canTimeout) throws Exception {
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
    testSuccessfulConnectivity(false);
  }

  @Test
  public void testSslConnectivityWorks() throws Exception {
    testSuccessfulConnectivity(true);
  }

  @Test
  public void testSslServerNormalClientFails() throws Exception {
    testConnectivityFailure(true, false);
  }

  @Test
  public void testSslClientNormalServerFails() throws Exception {
    testConnectivityFailure(false, true);
  }

  private void testSuccessfulConnectivity(boolean enableSsl) throws Exception {
    try (TestTransportState state =
             new TestTransportState(DEFAULT_HANDLER, TEST_MODULE, enableSsl, enableSsl);
         TransportClient client = state.createClient()) {

      String msg = " hi ";
      Pair<String, String> response = sendRPC(client, msg, false);
      assertNotNull(response.getLeft());
      assertNull(response.getRight());
      assertEquals(RESPONSE_PREFIX + msg + RESPONSE_SUFFIX, response.getLeft());
    }
  }

  private void testConnectivityFailure(boolean serverSsl, boolean clientSsl) throws Exception {
    try (TestTransportState state =
             new TestTransportState(DEFAULT_HANDLER, TEST_MODULE, serverSsl, clientSsl);
         TransportClient client = state.createClient()) {

      String msg = " hi ";
      Pair<String, String> response = sendRPC(client, msg, true);
      assertNull(response.getLeft());
      assertNotNull(response.getRight());
    } catch (IOException ioEx) {
      // this is fine - expected to fail
    }
  }
}
