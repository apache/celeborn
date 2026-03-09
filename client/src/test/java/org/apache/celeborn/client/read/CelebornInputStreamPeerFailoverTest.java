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

package org.apache.celeborn.client.read;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;

/** Tests for CelebornInputStream peer failover when stream cleanup throws RuntimeException. */
public class CelebornInputStreamPeerFailoverTest {

  private static final String SHUFFLE_KEY = "appid-1-1";
  private static final String PRIMARY_HOST = "host1";
  private static final String REPLICA_HOST = "host2";
  private static final int PORT = 9999;

  private CelebornConf conf;
  private TransportClientFactory clientFactory;
  private ShuffleClient shuffleClient;
  private ConcurrentHashMap<String, Long> fetchExcludedWorkers;

  @Before
  public void setUp() {
    conf = new CelebornConf();
    conf.set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED().key(), "true");
    conf.set(CelebornConf.CLIENT_FETCH_MAX_RETRIES_FOR_EACH_REPLICA().key(), "2");
    clientFactory = mock(TransportClientFactory.class);
    shuffleClient = mock(ShuffleClient.class);
    fetchExcludedWorkers = new ConcurrentHashMap<>();
  }

  /**
   * Tests that peer failover succeeds when stream cleanup throws RuntimeException.
   *
   * <p>Before the fix, a RuntimeException during cleanup (e.g. SASL wrapping IOException) would
   * escape the {@code catch (InterruptedException | IOException)} guard and bypass peer failover.
   * The fix adds {@code RuntimeException} to the catch so cleanup failures never block retry.
   */
  @Test
  public void testPeerFailoverWithRuntimeExceptionDuringCleanup() throws Exception {
    AtomicInteger attemptCount = new AtomicInteger(0);

    when(clientFactory.createClient(anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              String host = invocation.getArgument(0);
              int attempt = attemptCount.incrementAndGet();
              if (PRIMARY_HOST.equals(host)) {
                if (attempt == 1) {
                  // First attempt: reader creation on primary fails
                  throw new IOException("Worker Not Registered!");
                } else if (attempt == 2) {
                  // Second attempt: cleanup throws RuntimeException (simulates SASL wrapping)
                  throw new RuntimeException(new IOException("SASL handshake failed"));
                }
              } else if (REPLICA_HOST.equals(host)) {
                return mockReplicaClient();
              }
              throw new IOException("Unexpected host: " + host);
            });

    try {
      createInputStream(PRIMARY_HOST, REPLICA_HOST);
      verify(clientFactory, atLeast(2)).createClient(anyString(), anyInt());
    } catch (CelebornIOException e) {
      fail(
          "Peer failover should succeed despite RuntimeException during cleanup: "
              + e.getMessage());
    }
  }

  /** Tests that peer failover also works correctly when cleanup throws a plain IOException. */
  @Test
  public void testPeerFailoverWithIOExceptionDuringCleanup() throws Exception {
    AtomicInteger attemptCount = new AtomicInteger(0);

    when(clientFactory.createClient(anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              String host = invocation.getArgument(0);
              int attempt = attemptCount.incrementAndGet();
              if (PRIMARY_HOST.equals(host)) {
                if (attempt == 1) {
                  throw new IOException("Worker Not Registered!");
                } else if (attempt == 2) {
                  throw new IOException("Connection timeout");
                }
              } else if (REPLICA_HOST.equals(host)) {
                return mockReplicaClient();
              }
              throw new IOException("Unexpected host: " + host);
            });

    try {
      createInputStream(PRIMARY_HOST, REPLICA_HOST);
      verify(clientFactory, atLeast(2)).createClient(anyString(), anyInt());
    } catch (CelebornIOException e) {
      fail("Peer failover should succeed despite IOException during cleanup: " + e.getMessage());
    }
  }

  /** Tests that all retries are exhausted and an exception is thrown when there is no peer. */
  @Test(expected = CelebornIOException.class)
  public void testFailureWithoutPeer() throws Exception {
    when(clientFactory.createClient(anyString(), anyInt()))
        .thenThrow(new IOException("Worker Not Registered!"));

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(createPartitionLocation(PRIMARY_HOST));

    ArrayList<PbStreamHandler> streamHandlers = new ArrayList<>();
    streamHandlers.add(PbStreamHandler.newBuilder().setStreamId(789L).setNumChunks(1).build());

    CelebornInputStream.create(
        conf,
        clientFactory,
        SHUFFLE_KEY,
        locations,
        streamHandlers,
        new int[] {0},
        new HashMap<>(),
        new HashMap<>(),
        0,
        1L,
        0,
        100,
        fetchExcludedWorkers,
        shuffleClient,
        1,
        1,
        0,
        null,
        new TestMetricsCallback(),
        false);
  }

  private void createInputStream(String primaryHost, String replicaHost) throws IOException {
    PartitionLocation primary = createPartitionLocation(primaryHost);
    PartitionLocation replica = createPartitionLocation(replicaHost);
    primary.setPeer(replica);
    replica.setPeer(primary);

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(primary);

    ArrayList<PbStreamHandler> streamHandlers = new ArrayList<>();
    streamHandlers.add(PbStreamHandler.newBuilder().setStreamId(123L).setNumChunks(10).build());

    CelebornInputStream.create(
        conf,
        clientFactory,
        SHUFFLE_KEY,
        locations,
        streamHandlers,
        new int[] {0},
        new HashMap<>(),
        new HashMap<>(),
        0,
        1L,
        0,
        100,
        fetchExcludedWorkers,
        shuffleClient,
        1,
        1,
        0,
        null,
        new TestMetricsCallback(),
        false);
  }

  /**
   * Returns a mock TransportClient for the replica that responds to sendRpcSync with a valid
   * STREAM_HANDLER message, simulating a healthy replica worker.
   */
  private TransportClient mockReplicaClient() throws Exception {
    TransportClient client = mock(TransportClient.class);
    doNothing().when(client).sendRpc(any(ByteBuffer.class));
    PbStreamHandler replicaHandler =
        PbStreamHandler.newBuilder().setStreamId(456L).setNumChunks(10).build();
    ByteBuffer response =
        new TransportMessage(MessageType.STREAM_HANDLER, replicaHandler.toByteArray())
            .toByteBuffer();
    when(client.sendRpcSync(any(ByteBuffer.class), anyLong())).thenReturn(response);
    return client;
  }

  private PartitionLocation createPartitionLocation(String host) {
    PartitionLocation location =
        new PartitionLocation(
            0, 0, host, PORT, PORT + 1, PORT + 2, PORT + 3, PartitionLocation.Mode.PRIMARY);
    location.setStorageInfo(new StorageInfo(StorageInfo.Type.HDD, true, "/mnt/disk1/test"));
    return location;
  }

  private static class TestMetricsCallback implements MetricsCallback {
    @Override
    public void incBytesRead(long bytes) {}

    @Override
    public void incDuplicateBytesRead(long bytes) {}

    @Override
    public void incReadTime(long time) {}
  }
}
