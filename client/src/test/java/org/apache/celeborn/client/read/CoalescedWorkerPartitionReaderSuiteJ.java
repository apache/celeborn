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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbCoalescedChunkBoundary;
import org.apache.celeborn.common.protocol.PbCoalescedStreamHandler;
import org.apache.celeborn.common.protocol.PbOpenCoalescedStream;

public class CoalescedWorkerPartitionReaderSuiteJ {
  @Test
  public void testRetryResumesAfterCheckpointedChunk() throws Exception {
    SharedCoalescedStream stream = mock(SharedCoalescedStream.class);
    when(stream.getChunk(0)).thenReturn(Unpooled.wrappedBuffer(new byte[] {1, 2}));
    when(stream.getChunk(1))
        .thenThrow(new IOException("transient failure"))
        .thenReturn(Unpooled.wrappedBuffer(new byte[] {3, 4}));

    PbCoalescedChunkBoundary boundary =
        PbCoalescedChunkBoundary.newBuilder()
            .setStartChunkIndex(0)
            .setStartChunkOffset(0)
            .setEndChunkIndex(1)
            .setEndChunkOffset(2)
            .build();
    PartitionLocation location = mock(PartitionLocation.class);
    CoalescedWorkerPartitionReader reader =
        new CoalescedWorkerPartitionReader(location, stream, boundary, Optional.empty());

    ByteBuf first = reader.next();
    try {
      assertEquals(2, first.readableBytes());
    } finally {
      first.release();
    }
    assertThrows(IOException.class, reader::next);

    CoalescedWorkerPartitionReader retryReader =
        new CoalescedWorkerPartitionReader(
            location, stream, boundary, reader.getPartitionReaderCheckpointMetadata());
    assertTrue(retryReader.hasNext());
    ByteBuf second = retryReader.next();
    try {
      assertEquals(2, second.readableBytes());
    } finally {
      second.release();
    }
  }

  @Test
  public void testSlicesFromReadableStart() throws Exception {
    SharedCoalescedStream stream = mock(SharedCoalescedStream.class);
    ByteBuf chunk = Unpooled.wrappedBuffer(new byte[] {9, 9, 1, 2, 3, 4});
    chunk.readerIndex(2);
    when(stream.getChunk(0)).thenReturn(chunk);

    PbCoalescedChunkBoundary boundary =
        PbCoalescedChunkBoundary.newBuilder()
            .setStartChunkIndex(0)
            .setStartChunkOffset(0)
            .setEndChunkIndex(0)
            .setEndChunkOffset(4)
            .build();
    PartitionLocation location = mock(PartitionLocation.class);
    CoalescedWorkerPartitionReader reader =
        new CoalescedWorkerPartitionReader(location, stream, boundary, Optional.empty());

    ByteBuf slice = reader.next();
    try {
      byte[] actual = new byte[slice.readableBytes()];
      slice.readBytes(actual);
      assertArrayEquals(new byte[] {1, 2, 3, 4}, actual);
    } finally {
      slice.release();
      chunk.release();
    }
  }

  @Test
  public void testSharedCoalescedStreamWaitsForTransportCallback() throws Exception {
    CelebornConf conf = new CelebornConf().set("celeborn.client.fetch.timeout", "1ms");
    PartitionLocation location = mock(PartitionLocation.class);
    when(location.getHost()).thenReturn("worker");
    when(location.getFetchPort()).thenReturn(19098);
    when(location.hostAndFetchPort()).thenReturn("worker:19098");
    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient("worker", 19098)).thenReturn(client);
    TestMetricsCallback metricsCallback = new TestMetricsCallback();
    doAnswer(
            invocation -> {
              ChunkReceivedCallback callback = invocation.getArgument(3);
              new Thread(
                      () -> {
                        try {
                          Thread.sleep(50);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        callback.onSuccess(
                            0, new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[] {1, 2})));
                      })
                  .start();
              return null;
            })
        .when(client)
        .fetchChunk(
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any());
    SharedCoalescedStream stream =
        new SharedCoalescedStream(
            conf,
            "shuffle-key",
            location,
            openRequest(),
            PbCoalescedStreamHandler.newBuilder().setStreamId(1L).build(),
            clientFactory,
            metricsCallback);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<ByteBuf> future = executor.submit(() -> stream.getChunk(0));
      ByteBuf chunk = future.get(1, TimeUnit.SECONDS);
      try {
        assertEquals(2, chunk.readableBytes());
        assertEquals(1, metricsCallback.chunkFetchRequestCount);
        assertEquals(1, metricsCallback.chunkFetchSuccessCount);
        assertEquals(0, metricsCallback.chunkFetchFailureCount);
        assertEquals(1, metricsCallback.remoteWorkerStreamsRead);
        assertEquals("worker:19098", metricsCallback.remoteReadWorker);
      } finally {
        chunk.release();
      }
    } finally {
      executor.shutdownNow();
      stream.close();
    }
  }

  private static final class TestMetricsCallback implements MetricsCallback {
    private long chunkFetchRequestCount;
    private long chunkFetchSuccessCount;
    private long chunkFetchFailureCount;
    private long remoteWorkerStreamsRead;
    private String remoteReadWorker;

    @Override
    public void incBytesRead(long bytesWritten) {}

    @Override
    public void incReadTime(long time) {}

    @Override
    public void incChunkFetchRequestCount(long count) {
      chunkFetchRequestCount += count;
    }

    @Override
    public void incChunkFetchSuccessCount(long count) {
      chunkFetchSuccessCount += count;
    }

    @Override
    public void incChunkFetchFailureCount(long count) {
      chunkFetchFailureCount += count;
    }

    @Override
    public void recordRemoteReadWorker(String hostAndFetchPort) {
      remoteReadWorker = hostAndFetchPort;
    }

    @Override
    public void incRemoteWorkerStreamsRead(long count) {
      remoteWorkerStreamsRead += count;
    }
  }

  @Test
  public void testSharedCoalescedStreamReopensAfterFetchFailure() throws Exception {
    CelebornConf conf = new CelebornConf();
    PartitionLocation location = mock(PartitionLocation.class);
    when(location.getHost()).thenReturn("worker");
    when(location.getFetchPort()).thenReturn(19098);
    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    when(client.sendRpcSync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(
            new TransportMessage(MessageType.COALESCED_STREAM_HANDLER, handler(2L).toByteArray())
                .toByteBuffer());
    doAnswer(
            invocation -> {
              long streamId = invocation.getArgument(0);
              ChunkReceivedCallback callback = invocation.getArgument(3);
              if (streamId == 1L) {
                callback.onFailure(0, new IOException("lost worker stream"));
              } else {
                callback.onSuccess(
                    0, new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[] {2})));
              }
              return null;
            })
        .when(client)
        .fetchChunk(
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any());
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient("worker", 19098)).thenReturn(client);
    SharedCoalescedStream stream =
        new SharedCoalescedStream(
            conf,
            "shuffle-key",
            location,
            openRequest(),
            handler(1L),
            clientFactory,
            noopMetricsCallback());
    try {
      ByteBuf chunk = stream.getChunk(0);
      try {
        assertEquals(2, chunk.readByte());
      } finally {
        chunk.release();
      }
    } finally {
      stream.close();
    }
  }

  @Test
  public void testReopenPreservesReducerSlicesWhenChunkSpansBoundaries() throws Exception {
    CelebornConf conf = new CelebornConf();
    PartitionLocation location = mock(PartitionLocation.class);
    when(location.getHost()).thenReturn("worker");
    when(location.getFetchPort()).thenReturn(19098);
    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    when(client.sendRpcSync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(
            new TransportMessage(
                    MessageType.COALESCED_STREAM_HANDLER, spanningBoundaryHandler(2L).toByteArray())
                .toByteBuffer());
    doAnswer(
            invocation -> {
              long streamId = invocation.getArgument(0);
              ChunkReceivedCallback callback = invocation.getArgument(3);
              if (streamId == 1L) {
                callback.onFailure(0, new IOException("lost worker stream"));
              } else {
                callback.onSuccess(
                    0, new NettyManagedBuffer(Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4})));
              }
              return null;
            })
        .when(client)
        .fetchChunk(
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any());
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient("worker", 19098)).thenReturn(client);
    SharedCoalescedStream stream =
        new SharedCoalescedStream(
            conf,
            "shuffle-key",
            location,
            openRequest(),
            spanningBoundaryHandler(1L),
            clientFactory,
            noopMetricsCallback());
    try {
      CoalescedWorkerPartitionReader firstReader =
          new CoalescedWorkerPartitionReader(
              location, stream, spanningBoundaryHandler(1L).getBoundaries(0), Optional.empty());
      CoalescedWorkerPartitionReader secondReader =
          new CoalescedWorkerPartitionReader(
              location, stream, spanningBoundaryHandler(1L).getBoundaries(1), Optional.empty());

      ByteBuf first = firstReader.next();
      ByteBuf second = secondReader.next();
      try {
        byte[] firstBytes = new byte[first.readableBytes()];
        first.readBytes(firstBytes);
        byte[] secondBytes = new byte[second.readableBytes()];
        second.readBytes(secondBytes);
        assertArrayEquals(new byte[] {1, 2}, firstBytes);
        assertArrayEquals(new byte[] {3, 4}, secondBytes);
      } finally {
        first.release();
        second.release();
      }
    } finally {
      stream.close();
    }
  }

  @Test
  public void testSharedCoalescedStreamRejectsChangedLayoutOnReopen() throws Exception {
    CelebornConf conf = new CelebornConf();
    PartitionLocation location = mock(PartitionLocation.class);
    when(location.getHost()).thenReturn("worker");
    when(location.getFetchPort()).thenReturn(19098);
    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    when(client.sendRpcSync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(
            new TransportMessage(
                    MessageType.COALESCED_STREAM_HANDLER, changedHandler(2L).toByteArray())
                .toByteBuffer());
    doAnswer(
            invocation -> {
              ChunkReceivedCallback callback = invocation.getArgument(3);
              callback.onFailure(0, new IOException("lost worker stream"));
              return null;
            })
        .when(client)
        .fetchChunk(
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any());
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient("worker", 19098)).thenReturn(client);
    SharedCoalescedStream stream =
        new SharedCoalescedStream(
            conf,
            "shuffle-key",
            location,
            openRequest(),
            handler(1L),
            clientFactory,
            noopMetricsCallback());
    try {
      assertThrows(IOException.class, () -> stream.getChunk(0));
    } finally {
      stream.close();
    }
  }

  private static PbOpenCoalescedStream openRequest() {
    return PbOpenCoalescedStream.newBuilder()
        .setShuffleKey("shuffle-key")
        .setMaxChunkBytes(1024)
        .build();
  }

  private static MetricsCallback noopMetricsCallback() {
    return new MetricsCallback() {
      @Override
      public void incBytesRead(long bytesWritten) {}

      @Override
      public void incReadTime(long time) {}
    };
  }

  private static PbCoalescedStreamHandler handler(long streamId) {
    return PbCoalescedStreamHandler.newBuilder()
        .setStreamId(streamId)
        .setNumChunks(1)
        .addBoundaries(
            PbCoalescedChunkBoundary.newBuilder()
                .setFileName("file-0")
                .setReducerId(0)
                .setStartChunkIndex(0)
                .setStartChunkOffset(0)
                .setEndChunkIndex(0)
                .setEndChunkOffset(1))
        .build();
  }

  private static PbCoalescedStreamHandler spanningBoundaryHandler(long streamId) {
    return PbCoalescedStreamHandler.newBuilder()
        .setStreamId(streamId)
        .setNumChunks(1)
        .addBoundaries(
            PbCoalescedChunkBoundary.newBuilder()
                .setFileName("file-0")
                .setReducerId(0)
                .setStartChunkIndex(0)
                .setStartChunkOffset(0)
                .setEndChunkIndex(0)
                .setEndChunkOffset(2))
        .addBoundaries(
            PbCoalescedChunkBoundary.newBuilder()
                .setFileName("file-1")
                .setReducerId(1)
                .setStartChunkIndex(0)
                .setStartChunkOffset(2)
                .setEndChunkIndex(0)
                .setEndChunkOffset(4))
        .build();
  }

  private static PbCoalescedStreamHandler changedHandler(long streamId) {
    return PbCoalescedStreamHandler.newBuilder()
        .setStreamId(streamId)
        .setNumChunks(1)
        .addBoundaries(
            PbCoalescedChunkBoundary.newBuilder()
                .setFileName("file-0")
                .setReducerId(0)
                .setStartChunkIndex(0)
                .setStartChunkOffset(0)
                .setEndChunkIndex(0)
                .setEndChunkOffset(2))
        .build();
  }
}
