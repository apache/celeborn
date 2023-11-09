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

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.util.TransportConf;
import org.apache.celeborn.common.util.MemCacheManager;

public class MemoryBuffers implements Buffers {
  private static final Logger logger = LoggerFactory.getLogger(FileManagedBuffers.class);

  private final long[] offsets;
  private final int numChunks;
  private final String filePath;

  private final TransportConf conf;
  MemCacheManager memCacheManager;

  public MemoryBuffers(FileInfo fileInfo, TransportConf conf) {
    filePath = fileInfo.getFilePath();
    numChunks = fileInfo.numChunks();
    if (numChunks > 0) {
      offsets = new long[numChunks + 1];
      List<Long> chunkOffsets = fileInfo.getChunkOffsets();
      for (int i = 0; i <= numChunks; i++) {
        offsets[i] = chunkOffsets.get(i);
      }
    } else {
      offsets = new long[] {0};
    }
    this.conf = conf;
    memCacheManager = MemCacheManager.getMemCacheManager(conf.getCelebornConf());
  }

  public int numChunks() {
    return numChunks;
  }

  public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
    // offset of the beginning of the chunk in the file
    final long chunkOffset = offsets[chunkIndex];
    final long chunkLength = offsets[chunkIndex + 1] - chunkOffset;
    assert offset < chunkLength;
    long length = Math.min(chunkLength - offset, len);
    ByteBuf fileBuffer = memCacheManager.getCache(filePath);
    assert chunkOffset + offset + length <= fileBuffer.readableBytes();
    ByteBuf buffer = fileBuffer.retainedSlice((int) (chunkOffset + offset), (int) length);
    logger.info(
        "fetch chunk form memory cache for "
            + filePath
            + ", start point is "
            + (chunkOffset + offset)
            + ", fetch size is "
            + length);
    return new NettyManagedBuffer(buffer);
  }
}
