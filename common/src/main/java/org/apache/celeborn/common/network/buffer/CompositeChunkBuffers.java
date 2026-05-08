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

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import org.apache.celeborn.common.meta.ReduceFileMeta;

/**
 * Chunk buffers that concatenate source chunks without materializing the full stream at open time.
 */
public class CompositeChunkBuffers extends ChunkBuffers {
  private final List<Segment> segments;

  public CompositeChunkBuffers(List<ChunkBuffers> children, long chunkSize) {
    super(new ReduceFileMeta(buildChunkOffsets(children, chunkSize), chunkSize));
    this.segments = buildSegments(children);
  }

  @Override
  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    Tuple2<Long, Long> offsetLen = getChunkOffsetLength(chunkIndex, offset, len);
    long start = offsetLen._1;
    long end = start + offsetLen._2;
    List<ManagedBuffer> buffers = new ArrayList<>();
    for (Segment segment : segments) {
      if (segment.end <= start) {
        continue;
      }
      if (segment.start >= end) {
        break;
      }
      long segmentStart = Math.max(start, segment.start);
      long segmentEnd = Math.min(end, segment.end);
      int childOffset = (int) (segmentStart - segment.start);
      int childLength = (int) (segmentEnd - segmentStart);
      buffers.add(segment.child.chunk(segment.chunkIndex, childOffset, childLength));
    }
    if (buffers.size() == 1) {
      return buffers.get(0);
    }
    return new CompositeManagedBuffer(buffers);
  }

  private static List<Long> buildChunkOffsets(List<ChunkBuffers> children, long chunkSize) {
    List<Long> chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
    long emittedOffset = 0L;
    long emittedChunkBytes = 0L;
    // Repack child chunks into the configured stream chunk size without splitting a child chunk.
    // This preserves the original child chunk boundaries used by FileChunkBuffers.
    for (ChunkBuffers child : children) {
      for (int chunkIndex = 0; chunkIndex < child.numChunks(); chunkIndex++) {
        long childChunkSize = child.offsets[chunkIndex + 1] - child.offsets[chunkIndex];
        if (emittedChunkBytes > 0 && emittedChunkBytes + childChunkSize > chunkSize) {
          chunkOffsets.add(emittedOffset);
          emittedChunkBytes = 0L;
        }
        emittedOffset += childChunkSize;
        emittedChunkBytes += childChunkSize;
      }
    }
    if (chunkOffsets.get(chunkOffsets.size() - 1) != emittedOffset) {
      chunkOffsets.add(emittedOffset);
    }
    return chunkOffsets;
  }

  private static List<Segment> buildSegments(List<ChunkBuffers> children) {
    List<Segment> segments = new ArrayList<>();
    long globalOffset = 0L;
    for (ChunkBuffers child : children) {
      for (int chunkIndex = 0; chunkIndex < child.numChunks(); chunkIndex++) {
        long childChunkSize = child.offsets[chunkIndex + 1] - child.offsets[chunkIndex];
        segments.add(new Segment(child, chunkIndex, globalOffset, childChunkSize));
        globalOffset += childChunkSize;
      }
    }
    return segments;
  }

  private static class Segment {
    private final ChunkBuffers child;
    private final int chunkIndex;
    private final long start;
    private final long end;

    private Segment(ChunkBuffers child, int chunkIndex, long start, long length) {
      this.child = child;
      this.chunkIndex = chunkIndex;
      this.start = start;
      this.end = start + length;
    }
  }
}
