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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.client.ShuffleClient;
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
import org.apache.celeborn.common.protocol.PbOpenStream;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.unsafe.Platform;

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
    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    TransportClientFactory clientFactory = mock(TransportClientFactory.class);
    when(clientFactory.createClient("worker", 19098)).thenReturn(client);
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
            PbCoalescedStreamHandler.newBuilder().setStreamId(1L).build(),
            clientFactory);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<ByteBuf> future = executor.submit(() -> stream.getChunk(0));
      ByteBuf chunk = future.get(1, TimeUnit.SECONDS);
      try {
        assertEquals(2, chunk.readableBytes());
      } finally {
        chunk.release();
      }
    } finally {
      executor.shutdownNow();
      stream.close();
    }
  }

  @Test
  public void testCelebornInputStreamFallsBackToNormalReaderAfterCoalescedFailure()
      throws Exception {
    SharedCoalescedStream stream = mock(SharedCoalescedStream.class);
    when(stream.getChunk(0)).thenThrow(new IOException("lost shared stream"));

    PbCoalescedChunkBoundary boundary =
        PbCoalescedChunkBoundary.newBuilder()
            .setStartChunkIndex(0)
            .setStartChunkOffset(0)
            .setEndChunkIndex(0)
            .setEndChunkOffset(17)
            .build();
    PartitionLocation location = mock(PartitionLocation.class);
    StorageInfo storageInfo = mock(StorageInfo.class);
    when(location.getUniqueId()).thenReturn("location-0");
    when(location.hostAndFetchPort()).thenReturn("worker:19098");
    when(location.getStorageInfo()).thenReturn(storageInfo);
    when(location.getHost()).thenReturn("worker");
    when(location.getFetchPort()).thenReturn(19098);
    when(location.getFileName()).thenReturn("file-0");
    when(storageInfo.getType()).thenReturn(StorageInfo.Type.HDD);

    TransportClient client = mock(TransportClient.class);
    when(client.isActive()).thenReturn(true);
    when(client.sendRpcSync(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong()))
        .thenReturn(
            new TransportMessage(
                    MessageType.STREAM_HANDLER,
                    PbStreamHandler.newBuilder()
                        .setStreamId(2L)
                        .setNumChunks(1)
                        .build()
                        .toByteArray())
                .toByteBuffer());
    doAnswer(
            invocation -> {
              ChunkReceivedCallback callback = invocation.getArgument(3);
              callback.onSuccess(0, new NettyManagedBuffer(batch(0, (byte) 2)));
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

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(location);
    HashMap<String, CoalescedPartitionInfo> coalescedInfos = new HashMap<>();
    coalescedInfos.put("location-0", new CoalescedPartitionInfo(stream, boundary));
    CelebornConf conf =
        new CelebornConf()
            .set("celeborn.shuffle.compression.codec", "NONE")
            .set("celeborn.data.io.retryWait", "0s");
    CelebornInputStream inputStream =
        CelebornInputStream.create(
            conf,
            clientFactory,
            "shuffle-key",
            locations,
            null,
            new int[] {0},
            null,
            null,
            coalescedInfos,
            0,
            0L,
            0,
            Integer.MAX_VALUE,
            new ConcurrentHashMap<>(),
            mock(ShuffleClient.class),
            0,
            0,
            0,
            null,
            new MetricsCallback() {
              @Override
              public void incBytesRead(long bytesWritten) {}

              @Override
              public void incReadTime(long time) {}
            },
            false);
    try {
      assertEquals(2, inputStream.read());
      assertEquals(-1, inputStream.read());
      assertTrue(coalescedInfos.isEmpty());
      org.mockito.ArgumentCaptor<java.nio.ByteBuffer> request =
          org.mockito.ArgumentCaptor.forClass(java.nio.ByteBuffer.class);
      verify(client).sendRpcSync(request.capture(), org.mockito.ArgumentMatchers.anyLong());
      PbOpenStream openStream =
          TransportMessage.fromByteBuffer(request.getValue()).getParsedPayload();
      assertEquals("file-0", openStream.getFileName());
    } finally {
      inputStream.close();
    }
  }

  private static ByteBuf batch(int batchId, byte payload) {
    byte[] bytes = new byte[17];
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET, 0);
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET + 4, 0);
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET + 8, batchId);
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET + 12, 1);
    bytes[16] = payload;
    return Unpooled.wrappedBuffer(bytes);
  }
}
