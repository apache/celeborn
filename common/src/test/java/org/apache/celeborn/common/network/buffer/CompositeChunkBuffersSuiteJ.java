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

package org.apache.celeborn.common.network.buffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class CompositeChunkBuffersSuiteJ {
  @Test
  public void testManyTinyReducersUseFewerChunksAndKeepBoundaries() throws Exception {
    List<ChunkBuffers> inputs = new ArrayList<>();
    List<byte[]> expected = new ArrayList<>();
    for (int reducerId = 0; reducerId < 1600; reducerId++) {
      byte[] bytes =
          new byte[] {
            (byte) (reducerId & 0xff),
            (byte) ((reducerId >> 8) & 0xff),
            (byte) ((reducerId >> 16) & 0xff),
            (byte) ((reducerId >> 24) & 0xff)
          };
      expected.add(bytes);
      inputs.add(new ByteArrayChunkBuffers(bytes));
    }

    CompositeChunkBuffers composite = new CompositeChunkBuffers(inputs, 1024);
    assertTrue(composite.numChunks() < inputs.size());

    for (int reducerId = 0; reducerId < expected.size(); reducerId++) {
      CompositeChunkBuffers.Boundary boundary = composite.boundaries().get(reducerId);
      byte[] actual = readBoundary(composite, boundary);
      assertArrayEquals(expected.get(reducerId), actual);
    }
  }

  @Test
  public void testSingleSegmentChunkReusesUnderlyingBuffer() throws Exception {
    byte[] bytes = new byte[] {1, 2, 3, 4};
    CompositeChunkBuffers composite =
        new CompositeChunkBuffers(Collections.singletonList(new ByteArrayChunkBuffers(bytes)), 8);

    ByteBuffer chunk = composite.chunk(0, 1, 2).nioByteBuffer();
    bytes[1] = 9;
    assertArrayEquals(new byte[] {9, 3}, new byte[] {chunk.get(), chunk.get()});
  }

  @Test
  public void testOriginalChunksAreNotSplit() {
    CompositeChunkBuffers composite =
        new CompositeChunkBuffers(
            Collections.singletonList(new MultiChunkBuffers(new byte[][] {{1, 2, 3, 4}, {5, 6}})),
            3);

    assertEquals(2, composite.numChunks());
    assertEquals(4, composite.getChunkLength(0));
    assertEquals(2, composite.getChunkLength(1));
  }

  private byte[] readBoundary(
      CompositeChunkBuffers composite, CompositeChunkBuffers.Boundary boundary) throws Exception {
    ByteBuffer result = ByteBuffer.allocate(4);
    int chunkIndex = boundary.startChunkIndex;
    int chunkOffset = boundary.startChunkOffset;
    while (chunkIndex < boundary.endChunkIndex
        || (chunkIndex == boundary.endChunkIndex && chunkOffset < boundary.endChunkOffset)) {
      ByteBuffer chunk = composite.chunk(chunkIndex, 0, Integer.MAX_VALUE).nioByteBuffer();
      int endOffset =
          chunkIndex == boundary.endChunkIndex ? boundary.endChunkOffset : chunk.remaining();
      chunk.position(chunkOffset);
      chunk.limit(endOffset);
      result.put(chunk);
      if (chunkIndex == boundary.endChunkIndex) {
        chunkOffset = endOffset;
      } else {
        chunkIndex++;
        chunkOffset = 0;
      }
    }
    return result.array();
  }

  private static class ByteArrayChunkBuffers extends ChunkBuffers {
    private final byte[] bytes;

    ByteArrayChunkBuffers(byte[] bytes) {
      super(1);
      this.bytes = bytes;
    }

    @Override
    public long getChunkLength(int chunkIndex) {
      return bytes.length;
    }

    @Override
    public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
      int length = Math.min(bytes.length - offset, len);
      return new NioManagedBuffer(ByteBuffer.wrap(bytes, offset, length));
    }
  }

  private static class MultiChunkBuffers extends ChunkBuffers {
    private final byte[][] chunks;

    MultiChunkBuffers(byte[][] chunks) {
      super(chunks.length);
      this.chunks = chunks;
    }

    @Override
    public long getChunkLength(int chunkIndex) {
      return chunks[chunkIndex].length;
    }

    @Override
    public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
      byte[] bytes = chunks[chunkIndex];
      int length = Math.min(bytes.length - offset, len);
      return new NioManagedBuffer(ByteBuffer.wrap(bytes, offset, length));
    }
  }
}
