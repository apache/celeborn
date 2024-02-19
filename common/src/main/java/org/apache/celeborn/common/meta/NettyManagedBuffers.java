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

package org.apache.celeborn.common.meta;

import java.util.List;

import io.netty.buffer.CompositeByteBuf;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;

public class NettyManagedBuffers implements ManagedBuffers {
  private final long[] offsets;
  private final int numChunks;
  private final CompositeByteBuf buffer;

  public NettyManagedBuffers(MemoryFileInfo memoryFileInfo) {
    ReduceFileMeta meta = (ReduceFileMeta) memoryFileInfo.getFileMeta();
    buffer = memoryFileInfo.getBuffer();
    numChunks = meta.getNumChunks();
    if (numChunks > 0) {
      offsets = new long[numChunks + 1];
      List<Long> chunkOffsets = meta.getChunkOffsets();
      for (int i = 0; i < numChunks + 1; i++) {
        offsets[i] = chunkOffsets.get(i);
      }
    } else {
      offsets = new long[0];
    }
  }

  @Override
  public int numChunks() {
    return numChunks;
  }

  @Override
  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    final long chunkOffset = offsets[chunkIndex];
    final long chunkLength = offsets[chunkIndex + 1] - chunkOffset;
    assert offset < chunkLength;
    int length = (int) Math.min(chunkLength - offset, len);
    return new NettyManagedBuffer(buffer.slice(offset, length));
  }
}
