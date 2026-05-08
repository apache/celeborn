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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbCoalescedChunkBoundary;

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
}
