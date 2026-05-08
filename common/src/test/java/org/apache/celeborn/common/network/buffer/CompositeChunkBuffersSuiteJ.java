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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.celeborn.common.meta.ReduceFileMeta;

public class CompositeChunkBuffersSuiteJ {

  @Test
  public void compositeChunksPreserveChildChunkBoundariesAndOffsets() throws IOException {
    CompositeChunkBuffers buffers =
        new CompositeChunkBuffers(
            Arrays.asList(
                chunkBuffers(0, 5, 0L, 2L, 5L),
                chunkBuffers(10, 4, 0L, 3L, 4L),
                chunkBuffers(20, 2, 0L, 2L)),
            6);

    assertEquals(2, buffers.numChunks());
    assertArrayEquals(
        new byte[] {0, 1, 2, 3, 4}, toByteArray(buffers.chunk(0, 0, Integer.MAX_VALUE)));
    assertArrayEquals(
        new byte[] {10, 11, 12, 13, 20, 21}, toByteArray(buffers.chunk(1, 0, Integer.MAX_VALUE)));
    assertArrayEquals(new byte[] {11, 12, 13, 20}, toByteArray(buffers.chunk(1, 1, 4)));
  }

  private static ChunkBuffers chunkBuffers(int firstByte, int length, Long... offsets) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (firstByte + i);
    }
    return new TestChunkBuffers(data, Arrays.asList(offsets));
  }

  private static byte[] toByteArray(ManagedBuffer buffer) throws IOException {
    ByteBuffer byteBuffer = buffer.nioByteBuffer();
    byte[] data = new byte[byteBuffer.remaining()];
    byteBuffer.get(data);
    return data;
  }

  private static final class TestChunkBuffers extends ChunkBuffers {
    private final byte[] data;

    private TestChunkBuffers(byte[] data, List<Long> offsets) {
      super(new ReduceFileMeta(offsets, 8));
      this.data = data;
    }

    @Override
    public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
      scala.Tuple2<Long, Long> offsetLen = getChunkOffsetLength(chunkIndex, offset, len);
      return new NioManagedBuffer(
          java.nio.ByteBuffer.wrap(data, offsetLen._1().intValue(), offsetLen._2().intValue()));
    }
  }
}
