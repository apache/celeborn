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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Presents several reducer files as one chunked stream without requiring the serialized shuffle
 * streams to become one logical reducer stream.
 */
public class CompositeChunkBuffers extends ChunkBuffers {
  public static class Boundary {
    public final int startChunkIndex;
    public final int startChunkOffset;
    public final int endChunkIndex;
    public final int endChunkOffset;

    Boundary(int startChunkIndex, int startChunkOffset, int endChunkIndex, int endChunkOffset) {
      this.startChunkIndex = startChunkIndex;
      this.startChunkOffset = startChunkOffset;
      this.endChunkIndex = endChunkIndex;
      this.endChunkOffset = endChunkOffset;
    }
  }

  private static class Segment {
    final ChunkBuffers buffers;
    final int chunkIndex;
    final int offset;
    final int length;

    Segment(ChunkBuffers buffers, int chunkIndex, int offset, int length) {
      this.buffers = buffers;
      this.chunkIndex = chunkIndex;
      this.offset = offset;
      this.length = length;
    }
  }

  private final List<List<Segment>> chunks;
  private final List<Boundary> boundaries;

  public CompositeChunkBuffers(List<ChunkBuffers> inputs, int maxChunkBytes) {
    super(0);
    chunks = new ArrayList<>();
    boundaries = new ArrayList<>();
    List<Segment> currentChunk = new ArrayList<>();
    int currentChunkBytes = 0;
    long totalBytes = 0;
    List<long[]> byteBoundaries = new ArrayList<>();

    for (ChunkBuffers input : inputs) {
      long startOffset = totalBytes;
      // Inputs remain separate logical reducer files. We only pack their physical chunks into a
      // denser worker stream, then record each input's byte interval so the client can slice the
      // original reducer boundaries back out later.
      for (int chunkIndex = 0; chunkIndex < input.numChunks(); chunkIndex++) {
        int chunkLength = Math.toIntExact(input.getChunkLength(chunkIndex));
        if (!currentChunk.isEmpty() && currentChunkBytes + chunkLength > maxChunkBytes) {
          chunks.add(currentChunk);
          currentChunk = new ArrayList<>();
          currentChunkBytes = 0;
        }
        currentChunk.add(new Segment(input, chunkIndex, 0, chunkLength));
        currentChunkBytes += chunkLength;
        totalBytes += chunkLength;
      }
      byteBoundaries.add(new long[] {startOffset, totalBytes});
    }

    if (!currentChunk.isEmpty()) {
      chunks.add(currentChunk);
    }
    numChunks = chunks.size();
    offsets = new long[numChunks + 1];
    for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
      offsets[chunkIndex + 1] = offsets[chunkIndex] + chunkSize(chunks.get(chunkIndex));
    }
    for (long[] byteBoundary : byteBoundaries) {
      boundaries.add(toBoundary(byteBoundary[0], byteBoundary[1]));
    }
  }

  public List<Boundary> boundaries() {
    return boundaries;
  }

  private Boundary toBoundary(long startOffset, long endOffset) {
    int startChunkIndex = chunkIndexForOffset(startOffset);
    int endChunkIndex = chunkIndexForOffset(endOffset);
    return new Boundary(
        startChunkIndex,
        (int) (startOffset - offsets[startChunkIndex]),
        endChunkIndex,
        endChunkIndex == numChunks ? 0 : (int) (endOffset - offsets[endChunkIndex]));
  }

  private int chunkIndexForOffset(long offset) {
    int exactIndex = Arrays.binarySearch(offsets, offset);
    if (exactIndex >= 0) {
      return exactIndex;
    }
    return -exactIndex - 2;
  }

  @Override
  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    List<Segment> segments = chunks.get(chunkIndex);
    int chunkSize = chunkSize(segments);
    if (segments.size() == 1) {
      Segment segment = segments.get(0);
      int segmentLength = Math.min(segment.length - offset, len);
      return segment.buffers.chunk(segment.chunkIndex, segment.offset + offset, segmentLength);
    }
    // A packed chunk may straddle reducer-file boundaries. Materialize only that rare composite
    // case; the single-segment path above preserves the original zero-copy buffer.
    int remainingOffset = offset;
    int remainingLength = Math.min(len, chunkSize - offset);
    ByteBuffer buffer = ByteBuffer.allocate(remainingLength);
    for (Segment segment : segments) {
      if (remainingLength == 0) {
        break;
      }
      if (remainingOffset >= segment.length) {
        remainingOffset -= segment.length;
        continue;
      }
      int segmentOffset = segment.offset + remainingOffset;
      int segmentLength = Math.min(segment.length - remainingOffset, remainingLength);
      ManagedBuffer managedBuffer =
          segment.buffers.chunk(segment.chunkIndex, segmentOffset, segmentLength);
      try {
        ByteBuffer data = managedBuffer.nioByteBuffer();
        buffer.put(data);
      } catch (IOException e) {
        throw new RuntimeException("Failed to materialize coalesced chunk", e);
      } finally {
        managedBuffer.release();
      }
      remainingLength -= segmentLength;
      remainingOffset = 0;
    }
    buffer.flip();
    return new NioManagedBuffer(buffer);
  }

  private int chunkSize(List<Segment> segments) {
    return segments.stream().mapToInt(segment -> segment.length).sum();
  }
}
