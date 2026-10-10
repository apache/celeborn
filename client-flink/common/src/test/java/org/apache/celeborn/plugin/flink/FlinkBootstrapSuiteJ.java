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

package org.apache.celeborn.plugin.flink;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientBootstrap;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.plugin.flink.client.FlinkShuffleClientImpl;

public class FlinkBootstrapSuiteJ {
  @Before
  public void resetCounts() {
    CountingBootstrap.created.set(0);
    CountingBootstrap.closed.set(0);
  }

  @Test
  public void shutdownClosesBothDataContextsExactlyOnce() {
    FlinkShuffleClientImpl client = new TestClient(configuration(false));
    try {
      client.getDataClientFactory();
      assertEquals(2, CountingBootstrap.created.get());
    } finally {
      client.shutdown();
      client.shutdown();
    }
    assertEquals(2, CountingBootstrap.closed.get());
  }

  @Test
  public void readContextWaitsForLifecycleManagerAndRecoversFromInitializationFailure() {
    AtomicInteger calls = new AtomicInteger();
    FlinkShuffleClientImpl client =
        new TestClient(configuration(true)) {
          @Override
          protected List<TransportClientBootstrap> createBootstraps() {
            // The first call initializes push; the second initializes Flink read.
            if (calls.incrementAndGet() == 2) {
              throw new IllegalStateException("Application registration is unavailable");
            }
            return Collections.emptyList();
          }
        };
    try {
      assertEquals(0, CountingBootstrap.created.get());
      RpcEndpointRef endpoint = mock(RpcEndpointRef.class);
      assertThrows(IllegalStateException.class, () -> client.setupLifecycleManagerRef(endpoint));
      assertEquals(CountingBootstrap.closed.get() + 1, CountingBootstrap.created.get());
      client.setupLifecycleManagerRef(endpoint);
      assertNotNull(client.getDataClientFactory());
      assertEquals(CountingBootstrap.closed.get() + 2, CountingBootstrap.created.get());
    } finally {
      client.shutdown();
    }
    assertEquals(CountingBootstrap.created.get(), CountingBootstrap.closed.get());
  }

  @Test
  public void lifecycleManagerSetupFailureClosesTheConstructedPushContext() {
    assertThrows(
        IllegalStateException.class,
        () ->
            new FlinkShuffleClientImpl(
                "application", "localhost", 1232, 1L, configuration(false), null, 64) {
              @Override
              public void setupLifecycleManagerRef(String host, int port) {
                throw new IllegalStateException("LifecycleManager is unavailable");
              }
            });
    assertEquals(1, CountingBootstrap.created.get());
    assertEquals(1, CountingBootstrap.closed.get());
  }

  private static CelebornConf configuration(boolean authenticated) {
    return new CelebornConf(false)
        .set("celeborn.auth.enabled", Boolean.toString(authenticated))
        .set("celeborn.internal.port.enabled", "true")
        .set("celeborn.data.client.bootstrap.classes", CountingBootstrap.class.getName())
        .set("celeborn.data.io.clientThreads", "1")
        .set("celeborn.rpc_app_client.io.clientThreads", "1")
        .set("celeborn.rpc_app_client.dispatcher.threads", "1");
  }

  private static class TestClient extends FlinkShuffleClientImpl {
    TestClient(CelebornConf conf) {
      super("application", "localhost", 1232, 1L, conf, null, 64);
    }

    @Override
    public void setupLifecycleManagerRef(String host, int port) {}

    @Override
    protected List<TransportClientBootstrap> createBootstraps() {
      return Collections.emptyList();
    }
  }

  public static class CountingBootstrap implements TransportClientBootstrap {
    static final AtomicInteger created = new AtomicInteger();
    static final AtomicInteger closed = new AtomicInteger();

    public CountingBootstrap(TransportConf conf) {
      created.incrementAndGet();
    }

    @Override
    public void doBootstrap(TransportClient client) {}

    @Override
    public void close() {
      closed.incrementAndGet();
    }
  }
}
