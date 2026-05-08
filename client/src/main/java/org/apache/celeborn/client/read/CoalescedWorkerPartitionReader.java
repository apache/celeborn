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

import java.io.IOException;
import java.util.Optional;

import io.netty.buffer.ByteBuf;

import org.apache.celeborn.client.read.checkpoint.PartitionReaderCheckpointMetadata;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbCoalescedChunkBoundary;

public class CoalescedWorkerPartitionReader implements PartitionReader {
  private final PartitionLocation location;
  private final SharedCoalescedStream stream;
  private final PbCoalescedChunkBoundary boundary;
  private final PartitionReaderCheckpointMetadata checkpointMetadata;
  private int chunkIndex;
  private int chunkOffset;
  private int lastReturnedChunkIndex = -1;

  public CoalescedWorkerPartitionReader(
      PartitionLocation location,
      SharedCoalescedStream stream,
      PbCoalescedChunkBoundary boundary,
      Optional<PartitionReaderCheckpointMetadata> checkpointMetadata) {
    this.location = location;
    this.stream = stream;
    this.boundary = boundary;
    this.checkpointMetadata = checkpointMetadata.orElseGet(PartitionReaderCheckpointMetadata::new);
    // Checkpoints are in the shared coalesced-stream chunk space, not the original per-file chunk
    // space. Retries must therefore stay on the coalesced path once reading has started.
    this.chunkIndex = boundary.getStartChunkIndex();
    this.chunkOffset = boundary.getStartChunkOffset();
    skipCheckpointedChunks();
  }

  @Override
  public boolean hasNext() {
    return chunkIndex < boundary.getEndChunkIndex()
        || (chunkIndex == boundary.getEndChunkIndex()
            && chunkOffset < boundary.getEndChunkOffset());
  }

  @Override
  public ByteBuf next() throws IOException, InterruptedException {
    checkpointLastReturnedChunk();
    ByteBuf chunk = stream.getChunk(chunkIndex);
    int returnedChunkIndex = chunkIndex;
    int chunkStart = chunk.readerIndex();
    int endOffset =
        chunkIndex == boundary.getEndChunkIndex()
            ? boundary.getEndChunkOffset()
            : chunk.readableBytes();
    // Reducer boundaries may begin or end in the middle of a packed shared chunk. Return only the
    // byte range owned by this logical reducer reader.
    ByteBuf slice = chunk.retainedSlice(chunkStart + chunkOffset, endOffset - chunkOffset);
    if (chunkIndex == boundary.getEndChunkIndex()) {
      chunkOffset = endOffset;
    } else {
      chunkIndex++;
      chunkOffset = 0;
    }
    lastReturnedChunkIndex = returnedChunkIndex;
    return slice;
  }

  @Override
  public void close() {}

  @Override
  public PartitionLocation getLocation() {
    return location;
  }

  @Override
  public Optional<PartitionReaderCheckpointMetadata> getPartitionReaderCheckpointMetadata() {
    return Optional.of(checkpointMetadata);
  }

  private void checkpointLastReturnedChunk() {
    if (lastReturnedChunkIndex != -1) {
      checkpointMetadata.checkpoint(lastReturnedChunkIndex);
    }
  }

  private void skipCheckpointedChunks() {
    while (chunkIndex < boundary.getEndChunkIndex()
        && checkpointMetadata.isCheckpointed(chunkIndex)) {
      chunkIndex++;
      chunkOffset = 0;
    }
    if (chunkIndex == boundary.getEndChunkIndex()
        && checkpointMetadata.isCheckpointed(chunkIndex)) {
      chunkOffset = boundary.getEndChunkOffset();
    }
  }
}
