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

package org.apache.celeborn.service.deploy.worker.file.chunk.compressed;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.github.luben.zstd.Zstd;

import org.apache.celeborn.common.CelebornConf;

/**
 * Pool of reusable (chunkBuffer, compressedBuffer) pairs for ChunkCompressedFileChannelWriter,
 * bucketed by chunkSize so every acquired pair is exactly the right capacity.
 *
 * <p>Owns and manages the lifecycle of its internal {@link MmapMemoryManager}. Call {@link #close}
 * when the pool is no longer needed to release the mmap backing files.
 */
public class ChunkBufferPool {

  public static class BufferPair {
    public final ByteBuffer chunkBuffer;
    public final ByteBuffer compressedBuffer;
    public final long chunkSize;

    public BufferPair(ByteBuffer chunkBuffer, ByteBuffer compressedBuffer, long chunkSize) {
      this.chunkBuffer = chunkBuffer;
      this.compressedBuffer = compressedBuffer;
      this.chunkSize = chunkSize;
    }
  }

  private final MmapMemoryManager mmapMemoryManager;
  private final ConcurrentHashMap<Long, ConcurrentLinkedDeque<BufferPair>> poolMap =
      new ConcurrentHashMap<>();

  public ChunkBufferPool(CelebornConf conf) {
    this.mmapMemoryManager = new MmapMemoryManager(conf.chunkCompressionMmapTmpDir());
  }

  public BufferPair acquire(long chunkSize) {
    ConcurrentLinkedDeque<BufferPair> bucket =
        poolMap.computeIfAbsent(chunkSize, k -> new ConcurrentLinkedDeque<>());
    BufferPair pair = bucket.pollFirst();
    if (pair != null) {
      pair.chunkBuffer.clear();
      pair.compressedBuffer.clear();
      return pair;
    }
    int chunkBufSize = Math.toIntExact(chunkSize);
    int compressedBufSize = Math.toIntExact(Zstd.compressBound(chunkSize));
    ByteBuffer chunkBuf = mmapMemoryManager.allocateBuffer(chunkBufSize);
    ByteBuffer compressedBuf = mmapMemoryManager.allocateBuffer(compressedBufSize);
    return new BufferPair(chunkBuf, compressedBuf, chunkSize);
  }

  /** Returns the pair to the bucket matching its chunkSize. */
  public void release(BufferPair pair) {
    pair.chunkBuffer.clear();
    pair.compressedBuffer.clear();
    poolMap.computeIfAbsent(pair.chunkSize, k -> new ConcurrentLinkedDeque<>()).offerFirst(pair);
  }

  public void close() {
    poolMap.clear();
    mmapMemoryManager.close();
  }
}
