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

package org.apache.celeborn.client;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.celeborn.client.compress.Compressor;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.CommitMetadata;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.SerdeVersion;
import org.apache.celeborn.common.protocol.CompressionCodec;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbReadReducerPartitionEnd;
import org.apache.celeborn.common.protocol.PbReadReducerPartitionEndResponse;
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroup;
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse$;
import org.apache.celeborn.common.protocol.message.ControlMessages.RegisterShuffleResponse$;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.rpc.RpcTimeoutException;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.common.write.PushState;

public class ShuffleClientSuiteJ {

  private ShuffleClientImpl shuffleClient;
  private final RpcEndpointRef endpointRef = mock(RpcEndpointRef.class);
  private final TransportClientFactory clientFactory = mock(TransportClientFactory.class);
  private final TransportClient client = mock(TransportClient.class);

  private static final String TEST_APPLICATION_ID = "testapp1";
  private static final int TEST_SHUFFLE_ID = 1;
  private static final int TEST_ATTEMPT_ID = 0;
  private static final int TEST_REDUCRE_ID = 0;
  private static final int TEST_MAP_ID = 0;

  private static final int PRIMARY_RPC_PORT = 1234;
  private static final int PRIMARY_PUSH_PORT = 1235;
  private static final int PRIMARY_FETCH_PORT = 1236;
  private static final int PRIMARY_REPLICATE_PORT = 1237;
  private static final int REPLICA_RPC_PORT = 4321;
  private static final int REPLICA_PUSH_PORT = 4322;
  private static final int REPLICA_FETCH_PORT = 4323;
  private static final int REPLICA_REPLICATE_PORT = 4324;
  private static final PartitionLocation primaryLocation =
      new PartitionLocation(
          0,
          1,
          "localhost",
          PRIMARY_RPC_PORT,
          PRIMARY_PUSH_PORT,
          PRIMARY_FETCH_PORT,
          PRIMARY_REPLICATE_PORT,
          PartitionLocation.Mode.PRIMARY);
  private static final PartitionLocation replicaLocation =
      new PartitionLocation(
          0,
          1,
          "localhost",
          REPLICA_RPC_PORT,
          REPLICA_PUSH_PORT,
          REPLICA_FETCH_PORT,
          REPLICA_REPLICATE_PORT,
          PartitionLocation.Mode.REPLICA);

  private static final byte[] TEST_BUF1 = "hello world".getBytes(StandardCharsets.UTF_8);
  private final int BATCH_HEADER_SIZE = 4 * 4;

  private static void assertWaitingOnInFlightLoad(Thread thread) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Thread.State state = thread.getState();
    while (System.nanoTime() < deadlineNanos) {
      state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
        return;
      }
      Thread.sleep(10);
    }
    Assert.fail("Expected thread to wait on the in-flight reducer file group load, state=" + state);
  }

  private static void assertThreadBlocked(Thread thread) throws InterruptedException {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Thread.State state = thread.getState();
    while (System.nanoTime() < deadlineNanos) {
      state = thread.getState();
      if (state == Thread.State.BLOCKED) {
        return;
      }
      Thread.sleep(10);
    }
    Assert.fail("Expected thread to block on reducer file group publication, state=" + state);
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  public void testPushData() throws IOException, InterruptedException {
    for (CompressionCodec codec : CompressionCodec.values()) {
      CelebornConf conf = setupEnv(codec);

      int pushDataLen =
          shuffleClient.pushData(
              TEST_SHUFFLE_ID,
              TEST_ATTEMPT_ID,
              TEST_ATTEMPT_ID,
              TEST_REDUCRE_ID,
              TEST_BUF1,
              0,
              TEST_BUF1.length,
              1,
              1);

      if (codec.equals(CompressionCodec.NONE)) {
        assertEquals(TEST_BUF1.length + BATCH_HEADER_SIZE, pushDataLen);
      } else {
        Compressor compressor = Compressor.getCompressor(conf);
        compressor.compress(TEST_BUF1, 0, TEST_BUF1.length);
        final int compressedTotalSize = compressor.getCompressedTotalSize();
        assertEquals(compressedTotalSize + BATCH_HEADER_SIZE, pushDataLen);
      }
    }
  }

  @Test
  public void testPushDataAndInterrupted() throws IOException, InterruptedException {
    CelebornConf conf = setupEnv(CompressionCodec.NONE, StatusCode.SUCCESS, true);
    try {
      shuffleClient.pushData(
          TEST_SHUFFLE_ID,
          TEST_ATTEMPT_ID,
          TEST_ATTEMPT_ID,
          TEST_REDUCRE_ID,
          TEST_BUF1,
          0,
          TEST_BUF1.length,
          1,
          1);
      Thread.sleep(10 * 1000); // waiting for interrupt
      fail();
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        assertTrue(true);
      } else {
        fail();
      }
    }
  }

  @Test
  public void testMergeData() throws IOException, InterruptedException {
    for (CompressionCodec codec : CompressionCodec.values()) {
      CelebornConf conf = setupEnv(codec);

      int mergeSize =
          shuffleClient.mergeData(
              TEST_SHUFFLE_ID,
              TEST_ATTEMPT_ID,
              TEST_ATTEMPT_ID,
              TEST_REDUCRE_ID,
              TEST_BUF1,
              0,
              TEST_BUF1.length,
              1,
              1);

      if (codec.equals(CompressionCodec.NONE)) {
        assertEquals(TEST_BUF1.length + BATCH_HEADER_SIZE, mergeSize);
      } else {
        Compressor compressor = Compressor.getCompressor(conf);
        compressor.compress(TEST_BUF1, 0, TEST_BUF1.length);
        final int compressedTotalSize = compressor.getCompressedTotalSize();
        assertEquals(compressedTotalSize + BATCH_HEADER_SIZE, mergeSize);
      }

      byte[] buf1k = RandomStringUtils.random(4000).getBytes(StandardCharsets.UTF_8);
      int largeMergeSize =
          shuffleClient.mergeData(
              TEST_SHUFFLE_ID,
              TEST_ATTEMPT_ID,
              TEST_ATTEMPT_ID,
              TEST_REDUCRE_ID,
              buf1k,
              0,
              buf1k.length,
              1,
              1);

      if (codec.equals(CompressionCodec.NONE)) {
        assertEquals(buf1k.length + BATCH_HEADER_SIZE, largeMergeSize);
      } else {
        Compressor compressor = Compressor.getCompressor(conf);
        compressor.compress(buf1k, 0, buf1k.length);
        final int compressedTotalSize = compressor.getCompressedTotalSize();
        assertEquals(compressedTotalSize + BATCH_HEADER_SIZE, largeMergeSize);
      }
    }
  }

  @Test
  public void testRegisterShuffleFailed() throws IOException, InterruptedException {
    verifyRegisterShuffleFailure(StatusCode.SLOT_NOT_AVAILABLE);
    verifyRegisterShuffleFailure(StatusCode.RESERVE_SLOTS_FAILED);
    verifyRegisterShuffleFailure(StatusCode.REQUEST_FAILED);
  }

  private void verifyRegisterShuffleFailure(StatusCode statusCode)
      throws IOException, InterruptedException {
    setupEnv(CompressionCodec.NONE, statusCode);
    CelebornIOException e =
        assertThrows(
            CelebornIOException.class,
            () ->
                shuffleClient.pushData(
                    TEST_SHUFFLE_ID,
                    TEST_ATTEMPT_ID,
                    TEST_ATTEMPT_ID,
                    TEST_REDUCRE_ID,
                    TEST_BUF1,
                    0,
                    TEST_BUF1.length,
                    1,
                    1));
    assertTrue(
        e.getMessage()
            .contains("Register shuffle failed for shuffle 1, reason: " + statusCode.name()));
  }

  private CelebornConf setupEnv(CompressionCodec codec) throws IOException, InterruptedException {
    return setupEnv(codec, StatusCode.SUCCESS);
  }

  private CelebornConf setupEnv(CompressionCodec codec, StatusCode statusCode)
      throws IOException, InterruptedException {
    return setupEnv(codec, statusCode, false);
  }

  private CelebornConf setupEnv(
      CompressionCodec codec, StatusCode statusCode, boolean interruptWhenPushData)
      throws IOException, InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set(CelebornConf.SHUFFLE_COMPRESSION_CODEC().key(), codec.name());
    conf.set(CelebornConf.CLIENT_PUSH_RETRY_THREADS().key(), "1");
    conf.set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE().key(), "1K");
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));

    primaryLocation.setPeer(replicaLocation);
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    statusCode, new PartitionLocation[] {primaryLocation}, SerdeVersion.V1));

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t ->
                RegisterShuffleResponse$.MODULE$.apply(
                    statusCode, new PartitionLocation[] {primaryLocation}, SerdeVersion.V1));

    shuffleClient.setupLifecycleManagerRef(endpointRef);

    ChannelFuture mockedFuture =
        new ChannelFuture() {
          @Override
          public Channel channel() {
            return null;
          }

          @Override
          public ChannelFuture addListener(
              GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
          }

          @SafeVarargs
          @Override
          public final ChannelFuture addListeners(
              GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
          }

          @Override
          public ChannelFuture removeListener(
              GenericFutureListener<? extends Future<? super Void>> listener) {
            return null;
          }

          @SafeVarargs
          @Override
          public final ChannelFuture removeListeners(
              GenericFutureListener<? extends Future<? super Void>>... listeners) {
            return null;
          }

          @Override
          public ChannelFuture sync() {
            return null;
          }

          @Override
          public ChannelFuture syncUninterruptibly() {
            return null;
          }

          @Override
          public ChannelFuture await() {
            return null;
          }

          @Override
          public ChannelFuture awaitUninterruptibly() {
            return null;
          }

          @Override
          public boolean isVoid() {
            return false;
          }

          @Override
          public boolean isSuccess() {
            return true;
          }

          @Override
          public boolean isCancellable() {
            return false;
          }

          @Override
          public Throwable cause() {
            return null;
          }

          @Override
          public boolean await(long timeout, TimeUnit unit) {
            return true;
          }

          @Override
          public boolean await(long timeoutMillis) {
            return true;
          }

          @Override
          public boolean awaitUninterruptibly(long timeout, TimeUnit unit) {
            return true;
          }

          @Override
          public boolean awaitUninterruptibly(long timeoutMillis) {
            return true;
          }

          @Override
          public Void getNow() {
            return null;
          }

          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
          }

          @Override
          public boolean isCancelled() {
            return false;
          }

          @Override
          public boolean isDone() {
            return true;
          }

          @Override
          public Void get() {
            return null;
          }

          @SuppressWarnings("NullableProblems")
          @Override
          public Void get(long timeout, TimeUnit unit) {
            return null;
          }
        };

    if (interruptWhenPushData) {
      willAnswer(
              invocation -> {
                throw new InterruptedException("test");
              })
          .given(client)
          .pushData(any(), anyLong(), any());
    } else {
      when(client.pushData(any(), anyLong(), any())).thenAnswer(t -> mockedFuture);
    }
    when(clientFactory.createClient(
            primaryLocation.getHost(), primaryLocation.getPushPort(), TEST_REDUCRE_ID))
        .thenAnswer(t -> client);

    when(client.pushMergedData(any(), anyLong(), any())).thenAnswer(t -> mockedFuture);
    when(clientFactory.createClient(primaryLocation.getHost(), primaryLocation.getPushPort()))
        .thenAnswer(t -> client);

    shuffleClient.dataClientFactory = clientFactory;
    return conf;
  }

  @Test
  public void testUpdateReducerFileGroupInterrupted() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.spark.stageRerun.enabled", "true");
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t -> {
              Thread.sleep(60 * 1000);
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t -> {
              Thread.sleep(60 * 1000);
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    Thread thread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                try {
                  shuffleClient.updateFileGroup(0, 0);
                } catch (CelebornIOException e) {
                  exceptionRef.set(e);
                }
              }
            });

    thread.start();
    Thread.sleep(1000);
    thread.interrupt();
    Thread.sleep(1000);

    Exception exception = exceptionRef.get();
    Assert.assertTrue(exception.getCause() instanceof InterruptedException);
  }

  @Test
  public void testUpdateReducerFileGroupNonFetchFailureExceptions() throws CelebornIOException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.spark.stageRerun.enabled", "true");
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    AtomicInteger unregisteredRpcCalls = new AtomicInteger();
    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t -> {
              unregisteredRpcCalls.incrementAndGet();
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SHUFFLE_UNREGISTERED,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t -> {
              unregisteredRpcCalls.incrementAndGet();
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SHUFFLE_UNREGISTERED,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    Assert.assertNotNull(shuffleClient.updateFileGroup(0, 0));
    Assert.assertNotNull(shuffleClient.updateFileGroup(0, 1));
    Assert.assertEquals(1, unregisteredRpcCalls.get());

    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t -> {
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.STAGE_END_TIMEOUT,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t -> {
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.STAGE_END_TIMEOUT,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    CelebornIOException stageEndTimeout =
        Assert.assertThrows(CelebornIOException.class, () -> shuffleClient.updateFileGroup(0, 0));
    Assert.assertNull(stageEndTimeout.getCause());

    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            t -> {
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SHUFFLE_DATA_LOST,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            t -> {
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SHUFFLE_DATA_LOST,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    CelebornIOException shuffleDataLost =
        Assert.assertThrows(CelebornIOException.class, () -> shuffleClient.updateFileGroup(0, 0));
    Assert.assertNull(shuffleDataLost.getCause());
  }

  @Test
  public void testUpdateReducerFileGroupTimeout() throws InterruptedException {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.rpc.getReducerFileGroup.askTimeout", "1ms");

    when(endpointRef.askSync(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              throw new RpcTimeoutException(
                  "Rpc timeout", new TimeoutException("ask sync timeout"));
            });
    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              throw new RpcTimeoutException(
                  "Rpc timeout", new TimeoutException("ask sync timeout"));
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    try {
      shuffleClient.updateFileGroup(0, 0);
    } catch (CelebornIOException e) {
      exceptionRef.set(e);
    }

    Exception exception = exceptionRef.get();
    Assert.assertTrue(exception.getCause() instanceof TimeoutException);
  }

  @Test
  public void testSuccessfulReadReducePartitionEnd() throws IOException {
    CelebornConf conf = new CelebornConf();
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    ClassTag<PbReadReducerPartitionEndResponse> classTag =
        ClassTag$.MODULE$.apply(PbReadReducerPartitionEndResponse.class);
    PbReadReducerPartitionEndResponse mockResponse =
        PbReadReducerPartitionEndResponse.newBuilder()
            .setStatus(StatusCode.SUCCESS.getValue())
            .build();
    when(endpointRef.askSync(any(PbReadReducerPartitionEnd.class), any(), eq(classTag)))
        .thenReturn(mockResponse);

    shuffleClient.readReducerPartitionEnd(1, 2, 3, 4, 12345, 10000L);
  }

  @Test
  public void testFailedReadReducerPartitionEnd() throws IOException {
    CelebornConf conf = new CelebornConf();
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);
    ClassTag<PbReadReducerPartitionEndResponse> classTag =
        ClassTag$.MODULE$.apply(PbReadReducerPartitionEndResponse.class);

    String errorMsg = "Test error message";
    PbReadReducerPartitionEndResponse mockResponse =
        PbReadReducerPartitionEndResponse.newBuilder()
            .setStatus(StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue())
            .setErrorMsg(errorMsg)
            .build();

    when(endpointRef.askSync(any(PbReadReducerPartitionEnd.class), any(), eq(classTag)))
        .thenReturn(mockResponse);

    try {
      shuffleClient.readReducerPartitionEnd(1, 2, 3, 4, 12345, 10000L);
    } catch (CelebornIOException e) {
      Assert.assertEquals(errorMsg, e.getMessage());
    }
  }

  @Test
  public void testCorrectParametersPassedInRequest() throws IOException {
    CelebornConf conf = new CelebornConf();
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    int shuffleId = 123;
    int partitionId = 456;
    int startMapIndex = 0;
    int endMapIndex = 10;
    int crc32 = 98765;
    long bytesWritten = 54321L;

    PbReadReducerPartitionEndResponse mockResponse =
        PbReadReducerPartitionEndResponse.newBuilder()
            .setStatus(StatusCode.SUCCESS.getValue())
            .build();
    ArgumentCaptor<PbReadReducerPartitionEnd> requestCaptor =
        ArgumentCaptor.forClass(PbReadReducerPartitionEnd.class);
    ClassTag<PbReadReducerPartitionEndResponse> classTag =
        ClassTag$.MODULE$.apply(PbReadReducerPartitionEndResponse.class);

    when(endpointRef.askSync(requestCaptor.capture(), any(), eq(classTag)))
        .thenReturn(mockResponse);

    shuffleClient.readReducerPartitionEnd(
        shuffleId, partitionId, startMapIndex, endMapIndex, crc32, bytesWritten);
    PbReadReducerPartitionEnd capturedRequest = requestCaptor.getValue();
    assertEquals(shuffleId, capturedRequest.getShuffleId());
    assertEquals(partitionId, capturedRequest.getPartitionId());
    assertEquals(startMapIndex, capturedRequest.getStartMaxIndex());
    assertEquals(endMapIndex, capturedRequest.getEndMapIndex());
    assertEquals(crc32, capturedRequest.getCrc32());
    assertEquals(bytesWritten, capturedRequest.getBytesWritten());
  }

  @Test
  public void testComputeBatchCRCAccumulatesCorrectly() {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.shuffle.integrityCheck.enabled", "true");
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    byte[] batch0 = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] batch1a = "foo".getBytes(StandardCharsets.UTF_8);
    byte[] batch1b = "bar".getBytes(StandardCharsets.UTF_8);

    shuffleClient.computeBatchCRC(
        TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID, 0, batch0, 0, batch0.length);
    shuffleClient.computeBatchCRC(
        TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID, 1, batch1a, 0, batch1a.length);
    shuffleClient.computeBatchCRC(
        TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID, 1, batch1b, 0, batch1b.length);

    PushState pushState =
        shuffleClient.getPushState(Utils.makeMapKey(TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID));

    int numPartitions = 2;
    int[] crcPerPartition = pushState.getCRC32PerPartition(true, numPartitions);
    long[] bytesPerPartition = pushState.getBytesWrittenPerPartition(true, numPartitions);

    // compute expected values via CommitMetadata — same code path as production
    CommitMetadata expected0 = new CommitMetadata();
    expected0.addDataWithOffsetAndLength(batch0, 0, batch0.length);
    assertEquals(expected0.getChecksum(), crcPerPartition[0]);
    assertEquals(expected0.getBytes(), bytesPerPartition[0]);

    CommitMetadata expected1 = new CommitMetadata();
    expected1.addDataWithOffsetAndLength(batch1a, 0, batch1a.length);
    expected1.addDataWithOffsetAndLength(batch1b, 0, batch1b.length);
    assertEquals(expected1.getChecksum(), crcPerPartition[1]);
    assertEquals(expected1.getBytes(), bytesPerPartition[1]);
  }

  @Test
  public void testComputeBatchCRCDisabled() {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.shuffle.integrityCheck.enabled", "false");
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
    shuffleClient.computeBatchCRC(
        TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID, 0, data, 0, data.length);

    PushState pushState =
        shuffleClient.getPushState(Utils.makeMapKey(TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID));

    int numPartitions = 2;
    // When integrity check is disabled at the client level, computeBatchCRC is a no-op,
    // so the PushState should remain empty even though we pass true to the getter.
    int[] crcPerPartition = pushState.getCRC32PerPartition(true, numPartitions);
    long[] bytesPerPartition = pushState.getBytesWrittenPerPartition(true, numPartitions);

    for (int i = 0; i < numPartitions; i++) {
      assertEquals("Partition " + i + " CRC should be zero", 0, crcPerPartition[i]);
      assertEquals("Partition " + i + " bytes should be zero", 0, bytesPerPartition[i]);
    }
  }

  @Test
  public void testComputeBatchCRCAttemptIdConsistency() {
    CelebornConf conf = new CelebornConf();
    conf.set("celeborn.client.shuffle.integrityCheck.enabled", "true");
    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    byte[] data = "test".getBytes(StandardCharsets.UTF_8);

    // Call with attemptId=0
    shuffleClient.computeBatchCRC(
        TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID, 0, data, 0, data.length);

    // PushState for attemptId=0 should have the data
    PushState pushState0 =
        shuffleClient.getPushState(Utils.makeMapKey(TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID));
    int[] crc0 = pushState0.getCRC32PerPartition(true, 2);
    assertTrue(crc0[0] != 0);

    // PushState for attemptId=1 should be empty
    PushState pushState1 =
        shuffleClient.getPushState(
            Utils.makeMapKey(TEST_SHUFFLE_ID, TEST_MAP_ID, TEST_ATTEMPT_ID + 1));
    int[] crc1 = pushState1.getCRC32PerPartition(true, 2);
    assertEquals(0, crc1[0]);
  }

  @Test
  public void testUpdateReducerFileGroupEmptyRangeDoesNotLoadMetadata() throws Exception {
    CelebornConf conf = new CelebornConf();
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              rpcCalls.incrementAndGet();
              return null;
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    ShuffleClientImpl.ReduceFileGroups fileGroups = shuffleClient.updateFileGroup(7, 2, 2);

    Assert.assertTrue(fileGroups.partitionGroups.isEmpty());
    Assert.assertNull(fileGroups.mapAttempts);
    Assert.assertTrue(fileGroups.partitionIds.isEmpty());
    Assert.assertTrue(fileGroups.pushFailedBatches.isEmpty());
    Assert.assertEquals(0, rpcCalls.get());
    Assert.assertFalse(shuffleClient.hasReducerFileGroupRangeCache(7));
  }

  @Test
  public void testLegacyUpdateReducerFileGroupOverloadPreservesSubclassDispatch() throws Exception {
    CelebornConf conf = new CelebornConf();
    AtomicInteger calls = new AtomicInteger();
    ShuffleClientImpl.ReduceFileGroups expected = new ShuffleClientImpl.ReduceFileGroups();
    ShuffleClientImpl overriddenClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock")) {
          @Override
          public ShuffleClientImpl.ReduceFileGroups updateFileGroup(
              int shuffleId, int partitionId, boolean isSegmentGranularityVisible) {
            Assert.assertEquals(7, shuffleId);
            Assert.assertEquals(2, partitionId);
            Assert.assertFalse(isSegmentGranularityVisible);
            calls.incrementAndGet();
            return expected;
          }
        };

    try {
      Assert.assertSame(expected, overriddenClient.updateFileGroup(7, 2));
      Assert.assertEquals(1, calls.get());
    } finally {
      overriddenClient.shutdown();
    }
  }

  @Test
  public void testUpdateReducerFileGroupConcurrentLoadsShareSingleRpc() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    CountDownLatch secondThreadStarted = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              rpcCalls.incrementAndGet();
              rpcStarted.countDown();
              releaseRpc.await(10, TimeUnit.SECONDS);
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<ShuffleClientImpl.ReduceFileGroups> firstResult = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> secondResult = new AtomicReference<>();
    AtomicReference<Exception> firstException = new AtomicReference<>();
    AtomicReference<Exception> secondException = new AtomicReference<>();

    Thread firstThread =
        new Thread(
            () -> {
              try {
                firstResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread secondThread =
        new Thread(
            () -> {
              secondThreadStarted.countDown();
              try {
                secondResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                secondException.set(e);
              }
            });

    firstThread.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    secondThread.start();
    Assert.assertTrue(secondThreadStarted.await(10, TimeUnit.SECONDS));
    assertWaitingOnInFlightLoad(secondThread);

    Assert.assertEquals(1, rpcCalls.get());

    releaseRpc.countDown();
    firstThread.join(10 * 1000);
    secondThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(secondThread.isAlive());
    Assert.assertNull(firstException.get());
    Assert.assertNull(secondException.get());
    Assert.assertNotNull(firstResult.get());
    Assert.assertNotNull(secondResult.get());
    Assert.assertSame(firstResult.get(), secondResult.get());
  }

  @Test
  public void testWaitingReducerFileGroupLoadCanBeInterrupted() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              rpcCalls.incrementAndGet();
              rpcStarted.countDown();
              releaseRpc.await(10, TimeUnit.SECONDS);
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> ownerException = new AtomicReference<>();
    AtomicReference<Exception> waiterException = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                ownerException.set(e);
              }
            });
    Thread waiter =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                waiterException.set(e);
              }
            });

    owner.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    waiter.start();
    assertWaitingOnInFlightLoad(waiter);

    waiter.interrupt();
    waiter.join(10 * 1000);

    Assert.assertFalse(waiter.isAlive());
    Assert.assertTrue(owner.isAlive());
    Assert.assertTrue(waiterException.get() instanceof CelebornIOException);
    Assert.assertTrue(waiterException.get().getCause() instanceof InterruptedException);
    Assert.assertEquals(1, rpcCalls.get());

    releaseRpc.countDown();
    owner.join(10 * 1000);
    Assert.assertFalse(owner.isAlive());
    Assert.assertNull(ownerException.get());
  }

  @Test
  public void testInterruptedReducerFileGroupOwnerDoesNotFailExactRangeWaiter() throws Exception {
    assertInterruptedReducerFileGroupOwnerDoesNotFailWaiter(0);
  }

  @Test
  public void testInterruptedReducerFileGroupOwnerDoesNotFailColdRangeWaiter() throws Exception {
    assertInterruptedReducerFileGroupOwnerDoesNotFailWaiter(1);
  }

  private void assertInterruptedReducerFileGroupOwnerDoesNotFailWaiter(int waiterPartition)
      throws Exception {
    CelebornConf conf = new CelebornConf();
    CountDownLatch firstRpcStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstRpc = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();
    AtomicReference<GetReducerFileGroup> retryRequest = new AtomicReference<>();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              if (rpcCalls.incrementAndGet() == 1) {
                firstRpcStarted.countDown();
                releaseFirstRpc.await(10, TimeUnit.SECONDS);
              } else {
                retryRequest.set(request);
              }
              Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
              locations.put(request.startPartition(), Collections.emptySet());
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[] {0},
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Throwable> ownerException = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> waiterResult = new AtomicReference<>();
    AtomicReference<Throwable> waiterException = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Throwable e) {
                ownerException.set(e);
              }
            });
    Thread waiter =
        new Thread(
            () -> {
              try {
                waiterResult.set(shuffleClient.updateFileGroup(0, waiterPartition));
              } catch (Throwable e) {
                waiterException.set(e);
              }
            });

    owner.start();
    Assert.assertTrue(firstRpcStarted.await(10, TimeUnit.SECONDS));
    waiter.start();
    assertWaitingOnInFlightLoad(waiter);

    owner.interrupt();
    owner.join(10 * 1000);
    waiter.join(10 * 1000);

    Assert.assertFalse(owner.isAlive());
    Assert.assertFalse(waiter.isAlive());
    Assert.assertTrue(ownerException.get() instanceof CelebornIOException);
    Assert.assertTrue(ownerException.get().getCause() instanceof InterruptedException);
    Assert.assertNull(waiterException.get());
    Assert.assertNotNull(waiterResult.get());
    Assert.assertEquals(
        Collections.singleton(waiterPartition), waiterResult.get().partitionGroups.keySet());
    Assert.assertEquals(2, rpcCalls.get());
    Assert.assertEquals(waiterPartition, retryRequest.get().startPartition());
    Assert.assertEquals(waiterPartition + 1, retryRequest.get().endPartition());
    Assert.assertFalse(retryRequest.get().omitMapAttempts());
  }

  @Test
  public void testUpdateReducerFileGroupWarmDifferentRangesDoNotWaitForEachOther()
      throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    int[] mapAttempts = new int[] {0};
    CountDownLatch slowRpcStarted = new CountDownLatch(1);
    CountDownLatch releaseSlowRpc = new CountDownLatch(1);
    CountDownLatch fastRpcFinished = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              rpcCalls.incrementAndGet();
              Assert.assertTrue(request.hasPartitionRange());
              if (request.startPartition() == 2) {
                Assert.assertEquals(3, request.endPartition());
                Assert.assertFalse(request.omitMapAttempts());
              } else if (request.startPartition() == 0) {
                Assert.assertEquals(1, request.endPartition());
                Assert.assertTrue(request.omitMapAttempts());
                slowRpcStarted.countDown();
                releaseSlowRpc.await(10, TimeUnit.SECONDS);
              } else {
                Assert.assertEquals(1, request.startPartition());
                Assert.assertEquals(2, request.endPartition());
                Assert.assertTrue(request.omitMapAttempts());
                fastRpcFinished.countDown();
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  request.omitMapAttempts() ? new int[0] : mapAttempts,
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);
    shuffleClient.updateFileGroup(0, 2);
    Assert.assertEquals(1, rpcCalls.get());

    AtomicReference<Exception> slowException = new AtomicReference<>();
    AtomicReference<Exception> fastException = new AtomicReference<>();
    Thread slowThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                slowException.set(e);
              }
            });
    Thread fastThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 1);
              } catch (Exception e) {
                fastException.set(e);
              }
            });

    slowThread.start();
    Assert.assertTrue(slowRpcStarted.await(10, TimeUnit.SECONDS));
    fastThread.start();
    Assert.assertTrue(fastRpcFinished.await(10, TimeUnit.SECONDS));
    fastThread.join(10 * 1000);

    Assert.assertFalse(fastThread.isAlive());
    Assert.assertTrue(slowThread.isAlive());
    Assert.assertNull(fastException.get());
    Assert.assertEquals(3, rpcCalls.get());

    releaseSlowRpc.countDown();
    slowThread.join(10 * 1000);
    Assert.assertFalse(slowThread.isAlive());
    Assert.assertNull(slowException.get());
  }

  @Test
  public void testUpdateReducerFileGroupRequestsAndCachesOnlyNeededRange() throws Exception {
    CelebornConf conf = new CelebornConf();
    AtomicInteger rpcCalls = new AtomicInteger();
    AtomicReference<GetReducerFileGroup> firstRequest = new AtomicReference<>();
    AtomicReference<GetReducerFileGroup> secondRequest = new AtomicReference<>();
    CountDownLatch firstRpcStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstRpc = new CountDownLatch(1);
    int[] mapAttempts = new int[] {0, 1, 2};

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              int call = rpcCalls.getAndIncrement();
              if (call == 0) {
                firstRequest.set(request);
                firstRpcStarted.countDown();
                releaseFirstRpc.await(10, TimeUnit.SECONDS);
              } else if (call == 1) {
                secondRequest.set(request);
              }
              Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
              for (int partitionId = request.startPartition();
                  partitionId < request.endPartition();
                  partitionId++) {
                locations.put(partitionId, Collections.emptySet());
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  request.omitMapAttempts() ? new int[0] : mapAttempts,
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<ShuffleClientImpl.ReduceFileGroups> firstResult = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> adjacentResult = new AtomicReference<>();
    AtomicReference<Exception> firstException = new AtomicReference<>();
    AtomicReference<Exception> adjacentException = new AtomicReference<>();
    Thread firstThread =
        new Thread(
            () -> {
              try {
                firstResult.set(shuffleClient.updateFileGroup(7, 10, 20));
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread adjacentThread =
        new Thread(
            () -> {
              try {
                adjacentResult.set(shuffleClient.updateFileGroup(7, 20, 21));
              } catch (Exception e) {
                adjacentException.set(e);
              }
            });

    firstThread.start();
    Assert.assertTrue(firstRpcStarted.await(10, TimeUnit.SECONDS));
    adjacentThread.start();
    assertWaitingOnInFlightLoad(adjacentThread);

    Assert.assertEquals(1, rpcCalls.get());
    releaseFirstRpc.countDown();
    firstThread.join(10 * 1000);
    adjacentThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(adjacentThread.isAlive());
    Assert.assertNull(firstException.get());
    Assert.assertNull(adjacentException.get());

    ShuffleClientImpl.ReduceFileGroups first = firstResult.get();
    ShuffleClientImpl.ReduceFileGroups adjacent = adjacentResult.get();
    ShuffleClientImpl.ReduceFileGroups nested = shuffleClient.updateFileGroup(7, 11, 12);

    Assert.assertEquals(2, rpcCalls.get());
    Assert.assertEquals(10, first.partitionGroups.size());
    Assert.assertEquals(Collections.singleton(11), nested.partitionGroups.keySet());
    Assert.assertTrue(firstRequest.get().hasPartitionRange());
    Assert.assertEquals(10, firstRequest.get().startPartition());
    Assert.assertEquals(20, firstRequest.get().endPartition());
    Assert.assertFalse(firstRequest.get().omitMapAttempts());
    Assert.assertArrayEquals(mapAttempts, first.mapAttempts);

    Assert.assertEquals(Collections.singleton(20), adjacent.partitionGroups.keySet());
    Assert.assertTrue(secondRequest.get().omitMapAttempts());
    Assert.assertArrayEquals(mapAttempts, adjacent.mapAttempts);
  }

  @Test
  public void testLegacyFullResponseIsCachedForAllRanges() throws Exception {
    CelebornConf conf = new CelebornConf();
    AtomicInteger rpcCalls = new AtomicInteger();
    Map<Integer, Set<PartitionLocation>> fullResponse = new HashMap<>();
    fullResponse.put(0, Collections.emptySet());
    fullResponse.put(100, Collections.emptySet());

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              rpcCalls.incrementAndGet();
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  fullResponse,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    shuffleClient.updateFileGroup(7, 0, 1);
    ShuffleClientImpl.ReduceFileGroups second = shuffleClient.updateFileGroup(7, 100, 101);

    Assert.assertEquals(1, rpcCalls.get());
    Assert.assertEquals(Collections.singleton(100), second.partitionGroups.keySet());
  }

  @Test
  public void testUpdateReducerFileGroupConcurrentFailuresAreSharedAndNextCallRetries()
      throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseFailure = new CountDownLatch(1);
    CountDownLatch secondThreadStarted = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              if (rpcCalls.incrementAndGet() == 1) {
                rpcStarted.countDown();
                releaseFailure.await(10, TimeUnit.SECONDS);
                return GetReducerFileGroupResponse$.MODULE$.apply(
                    StatusCode.STAGE_END_TIMEOUT,
                    locations,
                    new int[0],
                    Collections.emptySet(),
                    Collections.emptyMap(),
                    new byte[0],
                    SerdeVersion.V1,
                    0,
                    0,
                    false);
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> firstException = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> secondResult = new AtomicReference<>();
    AtomicReference<Exception> secondException = new AtomicReference<>();

    Thread firstThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread secondThread =
        new Thread(
            () -> {
              secondThreadStarted.countDown();
              try {
                secondResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                secondException.set(e);
              }
            });

    firstThread.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    secondThread.start();
    Assert.assertTrue(secondThreadStarted.await(10, TimeUnit.SECONDS));
    assertWaitingOnInFlightLoad(secondThread);
    Assert.assertEquals(1, rpcCalls.get());

    releaseFailure.countDown();
    firstThread.join(10 * 1000);
    secondThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(secondThread.isAlive());
    Assert.assertTrue(firstException.get() instanceof CelebornIOException);
    Assert.assertTrue(secondException.get() instanceof CelebornIOException);
    Assert.assertNull(secondResult.get());
    Assert.assertEquals(1, rpcCalls.get());

    Assert.assertNotNull(shuffleClient.updateFileGroup(0, 0));
    Assert.assertEquals(2, rpcCalls.get());
  }

  @Test
  public void testCleanupShuffleDoesNotRestoreReducerFileGroupCache() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    CountDownLatch cleanupFinished = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              if (rpcCalls.incrementAndGet() == 1) {
                rpcStarted.countDown();
                releaseRpc.await(10, TimeUnit.SECONDS);
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> firstException = new AtomicReference<>();
    Thread firstThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread cleanupThread =
        new Thread(
            () -> {
              shuffleClient.cleanupShuffle(0);
              cleanupFinished.countDown();
            });

    firstThread.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    cleanupThread.start();
    Assert.assertTrue(cleanupFinished.await(10, TimeUnit.SECONDS));
    releaseRpc.countDown();
    firstThread.join(10 * 1000);
    cleanupThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(cleanupThread.isAlive());
    Assert.assertTrue(firstException.get() instanceof CelebornIOException);
    Assert.assertFalse(shuffleClient.hasReducerFileGroupRangeCache(0));

    Assert.assertNotNull(shuffleClient.updateFileGroup(0, 0));
    Assert.assertEquals(2, rpcCalls.get());
  }

  @Test
  public void testCleanupShuffleFailsWaitingReducerFileGroupLoads() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    CountDownLatch secondThreadStarted = new CountDownLatch(1);
    CountDownLatch cleanupFinished = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              if (rpcCalls.incrementAndGet() == 1) {
                rpcStarted.countDown();
                releaseRpc.await(10, TimeUnit.SECONDS);
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Exception> firstException = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> secondResult = new AtomicReference<>();
    AtomicReference<Exception> secondException = new AtomicReference<>();
    Thread firstThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread secondThread =
        new Thread(
            () -> {
              secondThreadStarted.countDown();
              try {
                secondResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                secondException.set(e);
              }
            });
    Thread cleanupThread =
        new Thread(
            () -> {
              shuffleClient.cleanupShuffle(0);
              cleanupFinished.countDown();
            });

    firstThread.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    secondThread.start();
    Assert.assertTrue(secondThreadStarted.await(10, TimeUnit.SECONDS));
    assertWaitingOnInFlightLoad(secondThread);
    cleanupThread.start();
    Assert.assertTrue(cleanupFinished.await(10, TimeUnit.SECONDS));

    releaseRpc.countDown();
    firstThread.join(10 * 1000);
    secondThread.join(10 * 1000);
    cleanupThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(secondThread.isAlive());
    Assert.assertFalse(cleanupThread.isAlive());
    Assert.assertTrue(firstException.get() instanceof CelebornIOException);
    Assert.assertTrue(secondException.get() instanceof CelebornIOException);
    Assert.assertNull(secondResult.get());
    Assert.assertEquals(1, rpcCalls.get());

    Assert.assertNotNull(shuffleClient.updateFileGroup(0, 0));
    Assert.assertEquals(2, rpcCalls.get());
  }

  @Test
  public void testColdBootstrapWaiterDoesNotRecreateCacheAfterCleanup() throws Exception {
    CelebornConf conf = new CelebornConf();
    CountDownLatch firstRpcStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstRpc = new CountDownLatch(1);
    CountDownLatch publicationStarted = new CountDownLatch(1);
    CountDownLatch releasePublication = new CountDownLatch(1);
    CountDownLatch cleanupFinished = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              GetReducerFileGroup request = invocation.getArgument(0);
              if (rpcCalls.incrementAndGet() == 1) {
                firstRpcStarted.countDown();
                releaseFirstRpc.await(10, TimeUnit.SECONDS);
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.SUCCESS,
                  Collections.emptyMap(),
                  new int[] {0},
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  request.startPartition(),
                  request.endPartition(),
                  true);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);
    shuffleClient.reduceFileGroupsBeforeInFlightRelease =
        () -> {
          publicationStarted.countDown();
          awaitLatch(releasePublication);
        };

    AtomicReference<Exception> ownerException = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> waiterResult = new AtomicReference<>();
    AtomicReference<Exception> waiterException = new AtomicReference<>();
    Thread owner =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                ownerException.set(e);
              }
            });
    Thread waiter =
        new Thread(
            () -> {
              try {
                waiterResult.set(shuffleClient.updateFileGroup(0, 1));
              } catch (Exception e) {
                waiterException.set(e);
              }
            });
    Thread cleanupThread =
        new Thread(
            () -> {
              shuffleClient.cleanupShuffle(0);
              cleanupFinished.countDown();
            });

    owner.start();
    Assert.assertTrue(firstRpcStarted.await(10, TimeUnit.SECONDS));
    waiter.start();
    assertWaitingOnInFlightLoad(waiter);
    releaseFirstRpc.countDown();
    Assert.assertTrue(publicationStarted.await(10, TimeUnit.SECONDS));

    cleanupThread.start();
    assertThreadBlocked(cleanupThread);
    Assert.assertFalse(shuffleClient.hasReducerFileGroupRangeCache(0));
    releasePublication.countDown();

    owner.join(10 * 1000);
    waiter.join(10 * 1000);
    cleanupThread.join(10 * 1000);

    Assert.assertFalse(owner.isAlive());
    Assert.assertFalse(waiter.isAlive());
    Assert.assertFalse(cleanupThread.isAlive());
    Assert.assertTrue(cleanupFinished.await(10, TimeUnit.SECONDS));
    Assert.assertNull(ownerException.get());
    Assert.assertNull(waiterResult.get());
    Assert.assertTrue(waiterException.get() instanceof CelebornIOException);
    Assert.assertEquals(1, rpcCalls.get());
    Assert.assertFalse(shuffleClient.hasReducerFileGroupRangeCache(0));
  }

  @Test
  public void testUpdateReducerFileGroupRechecksCacheAfterClaimingLoad() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch firstRpcStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstRpc = new CountDownLatch(1);
    CountDownLatch secondCacheMiss = new CountDownLatch(1);
    CountDownLatch releaseSecondCacheMiss = new CountDownLatch(1);
    AtomicInteger cacheMisses = new AtomicInteger();
    AtomicInteger rpcCalls = new AtomicInteger();

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              if (rpcCalls.incrementAndGet() == 1) {
                firstRpcStarted.countDown();
                releaseFirstRpc.await(10, TimeUnit.SECONDS);
                return GetReducerFileGroupResponse$.MODULE$.apply(
                    StatusCode.SUCCESS,
                    locations,
                    new int[0],
                    Collections.emptySet(),
                    Collections.emptyMap(),
                    new byte[0],
                    SerdeVersion.V1,
                    0,
                    0,
                    false);
              }
              return GetReducerFileGroupResponse$.MODULE$.apply(
                  StatusCode.STAGE_END_TIMEOUT,
                  locations,
                  new int[0],
                  Collections.emptySet(),
                  Collections.emptyMap(),
                  new byte[0],
                  SerdeVersion.V1,
                  0,
                  0,
                  false);
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);
    shuffleClient.reduceFileGroupsAfterCacheMiss =
        () -> {
          if (cacheMisses.incrementAndGet() == 2) {
            secondCacheMiss.countDown();
            awaitLatch(releaseSecondCacheMiss);
          }
        };

    AtomicReference<ShuffleClientImpl.ReduceFileGroups> firstResult = new AtomicReference<>();
    AtomicReference<ShuffleClientImpl.ReduceFileGroups> secondResult = new AtomicReference<>();
    AtomicReference<Exception> firstException = new AtomicReference<>();
    AtomicReference<Exception> secondException = new AtomicReference<>();
    Thread firstThread =
        new Thread(
            () -> {
              try {
                firstResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                firstException.set(e);
              }
            });
    Thread secondThread =
        new Thread(
            () -> {
              try {
                secondResult.set(shuffleClient.updateFileGroup(0, 0));
              } catch (Exception e) {
                secondException.set(e);
              }
            });

    firstThread.start();
    Assert.assertTrue(firstRpcStarted.await(10, TimeUnit.SECONDS));
    secondThread.start();
    Assert.assertTrue(secondCacheMiss.await(10, TimeUnit.SECONDS));
    releaseFirstRpc.countDown();
    firstThread.join(10 * 1000);
    Assert.assertFalse(firstThread.isAlive());

    releaseSecondCacheMiss.countDown();
    secondThread.join(10 * 1000);

    Assert.assertFalse(secondThread.isAlive());
    Assert.assertNull(firstException.get());
    Assert.assertNull(secondException.get());
    Assert.assertNotNull(firstResult.get());
    Assert.assertEquals(firstResult.get().partitionGroups, secondResult.get().partitionGroups);
    Assert.assertEquals(1, rpcCalls.get());
  }

  @Test
  public void testCleanupShuffleWaitsForReducerFileGroupPublication() throws Exception {
    CelebornConf conf = new CelebornConf();
    Map<Integer, Set<PartitionLocation>> locations = new HashMap<>();
    CountDownLatch publicationStarted = new CountDownLatch(1);
    CountDownLatch releasePublication = new CountDownLatch(1);
    CountDownLatch cleanupFinished = new CountDownLatch(1);

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation ->
                GetReducerFileGroupResponse$.MODULE$.apply(
                    StatusCode.SUCCESS,
                    locations,
                    new int[0],
                    Collections.emptySet(),
                    Collections.emptyMap(),
                    new byte[0],
                    SerdeVersion.V1,
                    0,
                    0,
                    false));

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);
    shuffleClient.reduceFileGroupsBeforeInFlightRelease =
        () -> {
          publicationStarted.countDown();
          awaitLatch(releasePublication);
        };

    AtomicReference<Exception> loadException = new AtomicReference<>();
    Thread loadThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Exception e) {
                loadException.set(e);
              }
            });
    Thread cleanupThread =
        new Thread(
            () -> {
              shuffleClient.cleanupShuffle(0);
              cleanupFinished.countDown();
            });

    loadThread.start();
    Assert.assertTrue(publicationStarted.await(10, TimeUnit.SECONDS));
    cleanupThread.start();
    assertThreadBlocked(cleanupThread);
    Assert.assertFalse(cleanupFinished.await(100, TimeUnit.MILLISECONDS));

    releasePublication.countDown();
    loadThread.join(10 * 1000);
    cleanupThread.join(10 * 1000);

    Assert.assertFalse(loadThread.isAlive());
    Assert.assertFalse(cleanupThread.isAlive());
    Assert.assertNull(loadException.get());
    Assert.assertFalse(shuffleClient.hasReducerFileGroupRangeCache(0));
  }

  @Test
  public void testUpdateReducerFileGroupWaitingReadersPreserveFatalErrors() throws Exception {
    CelebornConf conf = new CelebornConf();
    CountDownLatch rpcStarted = new CountDownLatch(1);
    CountDownLatch releaseRpc = new CountDownLatch(1);
    AtomicInteger rpcCalls = new AtomicInteger();
    AssertionError fatalError = new AssertionError("test fatal error");

    when(endpointRef.askSync(any(), any(), any(Integer.class), any(Long.class), any()))
        .thenAnswer(
            invocation -> {
              rpcCalls.incrementAndGet();
              rpcStarted.countDown();
              releaseRpc.await(10, TimeUnit.SECONDS);
              throw fatalError;
            });

    shuffleClient =
        new ShuffleClientImpl(TEST_APPLICATION_ID, conf, new UserIdentifier("mock", "mock"));
    shuffleClient.setupLifecycleManagerRef(endpointRef);

    AtomicReference<Throwable> firstError = new AtomicReference<>();
    AtomicReference<Throwable> secondError = new AtomicReference<>();
    Thread firstThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Throwable e) {
                firstError.set(e);
              }
            });
    Thread secondThread =
        new Thread(
            () -> {
              try {
                shuffleClient.updateFileGroup(0, 0);
              } catch (Throwable e) {
                secondError.set(e);
              }
            });

    firstThread.start();
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));
    secondThread.start();
    assertWaitingOnInFlightLoad(secondThread);
    releaseRpc.countDown();
    firstThread.join(10 * 1000);
    secondThread.join(10 * 1000);

    Assert.assertFalse(firstThread.isAlive());
    Assert.assertFalse(secondThread.isAlive());
    Assert.assertSame(fatalError, firstError.get());
    Assert.assertSame(fatalError, secondError.get());
    Assert.assertEquals(1, rpcCalls.get());
  }
}
