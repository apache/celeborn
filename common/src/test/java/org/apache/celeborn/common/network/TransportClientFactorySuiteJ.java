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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.EventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
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
    doSetup(newCelebornConf());
  }

  /**
   * A fresh conf carrying whatever this suite needs on top of the defaults, overridden by {@link
   * SSLTransportClientFactorySuiteJ}. The dead-event-loop tests below build their own
   * TransportContext and must start from this rather than a bare CelebornConf, or they would run a
   * plain client against the SSL server1/server2 that the subclass sets up, silently testing
   * something other than what the subclass exists to cover.
   */
  protected CelebornConf newCelebornConf() {
    return new CelebornConf();
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
    TransportConf conf = new TransportConf(TEST_MODULE, _conf);
    TransportContext context = new TransportContext(conf, new BaseMessageHandler(), true);
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

  @Test
  public void closeFactoryBeforeCreateClient() {
    TransportClientFactory factory = context.createClientFactory();
    EventLoopGroup groupBeforeClose = factory.getWorkerGroup();
    factory.close();
    assertThrows(IOException.class, () -> factory.createClient(getLocalHost(), server1.getPort()));
    // SPARK-58292: createClient on a closed factory fails with the terminated-executor cause, but
    // the closed factory must NOT recreate a fresh worker group (which would leak threads). The
    // group is left as-is, i.e. the shut-down one from before close().
    assertSame(groupBeforeClose, factory.getWorkerGroup());
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

  /**
   * A dead netty worker event loop is never replaced within a fixed-size group and permanently
   * poisons connections (SPARK-58292). Simulate it by shutting the factory's whole worker group
   * down and waiting for it to terminate: the next connection's channel registration is then
   * rejected with "event executor terminated", exactly as it would be by a single loop whose thread
   * has died.
   *
   * <p>The simulation is not faithful in one respect: shutting a loop down closes the channels
   * registered on it and delivers channelInactive(), whereas a loop whose thread has died leaves
   * them open and delivers nothing. Tests that care must account for that.
   */
  private static EventLoopGroup simulateDeadWorkerEventLoop(TransportClientFactory factory)
      throws InterruptedException {
    EventLoopGroup deadGroup = factory.getWorkerGroup();
    deadGroup.shutdownGracefully().sync();
    // isShutdown(), not merely isShuttingDown(): the terminated-loop rejection this simulates is
    // the one netty throws once the loop is fully shut down.
    assertTrue(deadGroup.isShutdown());
    return deadGroup;
  }

  private TransportContext newContext(CelebornConf celebornConf) {
    return new TransportContext(
        new TransportConf(TEST_MODULE, celebornConf), new BaseMessageHandler());
  }

  @Test
  public void recreatesWorkerGroupWhenEventLoopIsDead() throws Exception {
    CelebornConf _conf = newCelebornConf();
    _conf.set("celeborn.shuffle.io.retryWait", "100ms");
    TransportContext ctx = newContext(_conf);
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      EventLoopGroup deadGroup = simulateDeadWorkerEventLoop(factory);

      // The first connect attempt fails because the (dead) group rejects the channel
      // registration; the factory replaces the worker group and reconnects on it inline.
      TransportClient client = factory.createClient(getLocalHost(), server1.getPort());
      assertTrue(client.isActive());

      EventLoopGroup freshGroup = factory.getWorkerGroup();
      assertNotSame(deadGroup, freshGroup);
      assertFalse(freshGroup.isShuttingDown());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void retiresSupersededWorkerGroupWithNoChannelsLeft() throws Exception {
    // SPARK-58292: a superseded group must not be retained, along with its selector threads, until
    // the factory closes. See TransportClientFactory#retireWorkerGroupIfDrained.
    TransportContext ctx = newContext(newCelebornConf());
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      simulateDeadWorkerEventLoop(factory);

      assertTrue(factory.createClient(getLocalHost(), server1.getPort()).isActive());

      // The superseded group had no channels left to serve, so it was retired at once rather than
      // being parked on the retained list for close() to deal with.
      assertEquals(0, factory.supersededWorkerGroupCount());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void recreatedWorkerGroupIsUsedWithoutConsumingTheRetryBudget() throws Exception {
    // SPARK-58292: replacing the dead group only helps the triggering request if that request can
    // actually use the replacement. celeborn.<module>.io.maxRetries may be as low as 1, leaving no
    // retry to spend on the fresh group, so the reconnect must happen inline instead of being
    // charged to retryCreateClient.
    CelebornConf _conf = newCelebornConf();
    _conf.set("celeborn.shuffle.io.maxRetries", "1");
    TransportContext ctx = newContext(_conf);
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      EventLoopGroup deadGroup = simulateDeadWorkerEventLoop(factory);

      TransportClient client = factory.createClient(getLocalHost(), server1.getPort());
      assertTrue(client.isActive());
      assertNotSame(deadGroup, factory.getWorkerGroup());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void createUnmanagedClientRecoversFromDeadEventLoop() throws Exception {
    // createUnmanagedClient has no retry wrapper at all, so it can only recover from a dead event
    // loop if the reconnect on the recreated group happens inline.
    TransportContext ctx = newContext(newCelebornConf());
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      EventLoopGroup deadGroup = simulateDeadWorkerEventLoop(factory);

      TransportClient client = factory.createUnmanagedClient(getLocalHost(), server1.getPort());
      assertTrue(client.isActive());
      assertNotSame(deadGroup, factory.getWorkerGroup());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void failsOutstandingRequestsOfPooledClientsOnDeadEventLoops() throws Exception {
    // SPARK-58292: the request that recovers the worker group must also unblock owners that are
    // still holding a client pinned to the dead loop -- a dead loop delivers neither the write
    // listener nor channelInactive(), so nothing else would ever fail their callbacks.
    TransportContext ctx = newContext(newCelebornConf());
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      TransportClient client = factory.createClient(getLocalHost(), server1.getPort());
      simulateDeadWorkerEventLoop(factory);

      // Register the outstanding request only once the group has terminated. Shutting a loop down
      // closes its channels and delivers channelInactive(), which would fail the request by itself
      // and leave the sweep untested; a genuinely dead loop does neither. Past termination no loop
      // thread is left running, so the sweep is the only thing that can complete the request.
      AtomicReference<Throwable> failure = new AtomicReference<>();
      client
          .getHandler()
          .addRpcRequest(
              1L,
              new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {}

                @Override
                public void onFailure(Throwable e) {
                  failure.set(e);
                }
              });

      assertTrue(client.isEventLoopDead());
      assertFalse(client.isActive());
      assertTrue(client.getHandler().hasOutstandingRequests());

      factory.failClientsOnDeadEventLoops();

      assertNotNull("the pooled client's outstanding RPC must be failed", failure.get());
      assertFalse(client.getHandler().hasOutstandingRequests());
    } finally {
      ctx.close();
    }
  }

  @Test
  public void doesNotRecreateWorkerGroupWhenDisabled() throws Exception {
    // With the recreation disabled, a dead worker group is NOT recreated: createClient still
    // fails, but the worker group is left unchanged.
    CelebornConf _conf = newCelebornConf();
    _conf.set("celeborn.shuffle.io.recreateWorkerGroupOnDeadEventLoop", "false");
    _conf.set("celeborn.shuffle.io.retryWait", "100ms");
    TransportContext ctx = newContext(_conf);
    try (TransportClientFactory factory = ctx.createClientFactory()) {
      EventLoopGroup deadGroup = simulateDeadWorkerEventLoop(factory);

      assertThrows(
          IOException.class, () -> factory.createClient(getLocalHost(), server1.getPort()));
      assertSame(deadGroup, factory.getWorkerGroup());
      // Nothing was superseded, so none of the retirement bookkeeping kicked in either.
      assertEquals(0, factory.supersededWorkerGroupCount());
    } finally {
      ctx.close();
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

  @Test
  public void doNotRetryCreateClientWhenInterruptedExceptionIsWrapped() throws Exception {
    TransportClientFactory factory = Mockito.spy(context.createClientFactory());
    InterruptedException interruptedException = new InterruptedException("test");
    Mockito.doThrow(new IOException("wrapped", interruptedException))
        .when(factory)
        .createClient(anyString(), anyInt(), anyInt(), any());

    try {
      InterruptedException thrown =
          assertThrows(
              InterruptedException.class,
              () -> factory.retryCreateClient("xxx", 10, 1, TransportFrameDecoder::new));

      assertSame(interruptedException, thrown);
      assertTrue(Thread.currentThread().isInterrupted());
      Mockito.verify(factory, Mockito.times(1))
          .createClient(anyString(), anyInt(), anyInt(), any());
    } finally {
      Thread.interrupted();
    }
  }
}
