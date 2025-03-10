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

package org.apache.celeborn.common.network;

import static org.apache.celeborn.common.util.JavaUtils.getLocalHost;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.server.BaseMessageHandler;
import org.apache.celeborn.common.network.server.TransportServer;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.network.util.TransportFrameDecoder;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;

public class TransportClientFactorySuiteJ {

  static final String TEST_MODULE = "shuffle";

  private TransportContext context;
  private TransportServer server1;
  private TransportServer server2;

  protected void doSetup(CelebornConf celebornConf) {
    TransportConf conf = new TransportConf(TEST_MODULE, celebornConf);
    BaseMessageHandler handler = new BaseMessageHandler();
    context = new TransportContext(conf, handler);
    server1 = context.createServer();
    server2 = context.createServer();
  }

  @Before
  public void setUp() {
    doSetup(new CelebornConf());
  }

  // for validation in subclasses
  TransportConf getTransportContextConf() {
    return context.getConf();
  }

  @After
  public void tearDown() {
    JavaUtils.closeQuietly(server1);
    JavaUtils.closeQuietly(server2);
    JavaUtils.closeQuietly(context);
  }

  /**
   * Request a bunch of clients to a single server to test we create up to maxConnections of
   * clients.
   *
   * <p>If concurrent is true, create multiple threads to create clients in parallel.
   */
  @SuppressWarnings("DoNotCall")
  private void testClientReuse(int maxConnections, boolean concurrent)
      throws IOException, InterruptedException {

    CelebornConf _conf = new CelebornConf();
    _conf.set("celeborn.shuffle.io.numConnectionsPerPeer", Integer.toString(maxConnections));
    TransportConf conf = new TransportConf(TEST_MODULE, _conf);

    BaseMessageHandler handler = new BaseMessageHandler();
    TransportContext context = new TransportContext(conf, handler);
    TransportClientFactory factory = context.createClientFactory();
    Set<TransportClient> clients = Collections.synchronizedSet(new HashSet<TransportClient>());

    AtomicInteger failed = new AtomicInteger();
    Thread[] attempts = new Thread[maxConnections * 10];

    // Launch a bunch of threads to create new clients.
    for (int i = 0; i < attempts.length; i++) {
      attempts[i] =
          ThreadUtils.newThread(
              () -> {
                try {
                  TransportClient client = factory.createClient(getLocalHost(), server1.getPort());
                  assertTrue(client.isActive());
                  clients.add(client);
                } catch (IOException e) {
                  failed.incrementAndGet();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              },
              "test-thread");

      if (concurrent) {
        attempts[i].start();
      } else {
        attempts[i].run();
      }
    }

    // Wait until all the threads complete.
    for (Thread attempt : attempts) {
      attempt.join();
    }

    assertEquals(0, failed.get());
    assertTrue(clients.size() <= maxConnections);

    for (TransportClient client : clients) {
      client.close();
    }

    factory.close();
    context.close();
  }

  @Test
  public void reuseClientsUpToConfigVariable() throws Exception {
    testClientReuse(1, false);
    testClientReuse(2, false);
    testClientReuse(3, false);
    testClientReuse(4, false);
  }

  @Test
  public void reuseClientsUpToConfigVariableConcurrent() throws Exception {
    testClientReuse(1, true);
    testClientReuse(2, true);
    testClientReuse(3, true);
    testClientReuse(4, true);
  }

  @Test
  public void returnDifferentClientsForDifferentServers() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    assertNotSame(c1, c2);
    factory.close();
  }

  @Test
  public void neverReturnInactiveClients() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(getLocalHost(), server1.getPort());
    c1.close();

    long start = System.currentTimeMillis();
    while (c1.isActive() && (System.currentTimeMillis() - start) < 3000) {
      Thread.sleep(10);
    }
    assertFalse(c1.isActive());

    TransportClient c2 = factory.createClient(getLocalHost(), server1.getPort());
    assertNotSame(c1, c2);
    assertTrue(c2.isActive());
    factory.close();
  }

  @Test
  public void closeBlockClientsWithFactory() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    TransportClient c1 = factory.createClient(getLocalHost(), server1.getPort());
    TransportClient c2 = factory.createClient(getLocalHost(), server2.getPort());
    assertTrue(c1.isActive());
    assertTrue(c2.isActive());
    factory.close();
    assertFalse(c1.isActive());
    assertFalse(c2.isActive());
  }

  @Test
  public void closeIdleConnectionForRequestTimeOut() throws IOException, InterruptedException {
    CelebornConf _conf = new CelebornConf();
    _conf.set("celeborn.shuffle.io.connectionTimeout", "1s");
    _conf.set("celeborn.closeIdleConnections", "true");
    TransportConf conf = new TransportConf(TEST_MODULE, _conf);
    TransportContext context = new TransportContext(conf, new BaseMessageHandler());
    try (TransportClientFactory factory = context.createClientFactory()) {
      TransportClient c1 = factory.createClient(getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      long expiredTime = System.currentTimeMillis() + 10000; // 10 seconds
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      assertFalse(c1.isActive());
    }
    context.close();
  }

  @Test(expected = IOException.class)
  public void closeFactoryBeforeCreateClient() throws IOException, InterruptedException {
    TransportClientFactory factory = context.createClientFactory();
    factory.close();
    factory.createClient(getLocalHost(), server1.getPort());
  }

  @Test
  public void unlimitedConnectionAndCreationTimeouts() throws IOException, InterruptedException {
    CelebornConf _conf = new CelebornConf();
    _conf.set("celeborn.shuffle.io.connectTimeout", "-1");
    _conf.set("celeborn.shuffle.io.connectionTimeout", "-1");
    TransportConf conf = new TransportConf(TEST_MODULE, _conf);
    try (TransportContext ctx = new TransportContext(conf, new BaseMessageHandler(), true);
        TransportClientFactory factory = ctx.createClientFactory()) {
      TransportClient c1 = factory.createClient(getLocalHost(), server1.getPort());
      assertTrue(c1.isActive());
      long expiredTime = System.currentTimeMillis() + 5000;
      while (c1.isActive() && System.currentTimeMillis() < expiredTime) {
        Thread.sleep(10);
      }
      assertTrue(c1.isActive());
      // When connectionTimeout is unlimited, the connection shall be able to fail when the server
      // is not reachable.
      TransportServer server = ctx.createServer();
      int unreachablePort = server.getPort();
      JavaUtils.closeQuietly(server);
      IOException exception =
          assertThrows(
              IOException.class, () -> factory.createClient(getLocalHost(), unreachablePort));
      assertNotEquals(exception.getCause(), null);
    }
  }

  @Test
  public void testRetryCreateClient() throws IOException, InterruptedException {
    TransportClientFactory factory = Mockito.spy(context.createClientFactory());
    TransportClient client = mock(TransportClient.class);
    Mockito.doThrow(new IOException("xx"))
        .doReturn(client)
        .when(factory)
        .createClient(anyString(), anyInt(), anyInt(), any());
    TransportClient transportClient =
        factory.retryCreateClient("xxx", 10, 1, TransportFrameDecoder::new);
    Assert.assertEquals(transportClient, client);
  }
}
