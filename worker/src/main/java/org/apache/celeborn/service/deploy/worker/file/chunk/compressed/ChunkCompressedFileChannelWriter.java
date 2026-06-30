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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import com.github.luben.zstd.Zstd;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.CompositeByteBuf;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.service.deploy.worker.file.FileChannelWriter;

public class ChunkCompressedFileChannelWriter extends FileChannelWriter {
  private final FileChannel channel;
  private final DiskFileInfo diskFileInfo;
  private final int compressionLevel;
  private final ChunkBufferPool chunkBufferPool;
  private final ChunkBufferPool.BufferPair bufferPair;
  private ByteBuffer chunkBuffer;
  private ByteBuffer compressedChunkBuffer;
  private final List<Long> chunkOffsets;
  private final List<Boolean> chunkCompressed;
  private final long chunkSize;
  private boolean closed = false;

  public ChunkCompressedFileChannelWriter(
      DiskFileInfo diskFileInfo,
      long chunkSize,
      int compressionLevel,
      ChunkBufferPool chunkBufferPool)
      throws IOException {
    this.diskFileInfo = diskFileInfo;
    this.chunkSize = chunkSize;
    channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
    this.compressionLevel = compressionLevel;
    this.chunkBufferPool = chunkBufferPool;
    bufferPair = chunkBufferPool.acquire(chunkSize);
    chunkBuffer = bufferPair.chunkBuffer;
    compressedChunkBuffer = bufferPair.compressedBuffer;
    chunkOffsets = new ArrayList<>();
    chunkOffsets.add(0L);
    chunkCompressed = new ArrayList<>();
  }

  @Override
  public void write(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException {
    if (buffer.readableBytes() > chunkSize) {
      // Flush any pending accumulated data before writing the large record so file offsets
      // remain consistent.
      compressAndFlush();
      flushLargeRecord(buffer);
      return;
    }

    if (buffer.readableBytes() > chunkBuffer.remaining()) {
      compressAndFlush();
    }

    ByteBuffer[] buffers = buffer.nioBuffers();
    for (ByteBuffer byteBuffer : buffers) {
      while (byteBuffer.hasRemaining()) {
        chunkBuffer.put(byteBuffer);
      }
    }
  }

  /**
   * Writes the large record directly to the channel without compression. Large records span a full
   * chunk on their own, so the decompression overhead would be paid all at once anyway; skipping
   * compression avoids the ZstdOutputStream frame overhead and simplifies the write path.
   */
  private void flushLargeRecord(CompositeByteBuf buffer) throws IOException {
    ByteBuffer[] buffers = buffer.nioBuffers();
    for (ByteBuffer buf : buffers) {
      while (buf.hasRemaining()) {
        channel.write(buf);
      }
    }
    chunkCompressed.add(false);
    chunkOffsets.add(channel.position());
  }

  @VisibleForTesting
  public void compressAndFlush() throws IOException {
    int size = chunkBuffer.position();
    if (size == 0) return;
    chunkBuffer.position(0);
    chunkBuffer.limit(size);
    compressedChunkBuffer.clear();
    int compressedSize;
    try {
      compressedSize =
          (int)
              Zstd.compressDirectByteBuffer(
                  compressedChunkBuffer,
                  0,
                  compressedChunkBuffer.capacity(),
                  chunkBuffer,
                  0,
                  size,
                  compressionLevel);
    } catch (RuntimeException e) {
      throw new IOException("Failed to compress chunk with ZSTD.", e);
    }
    if (Zstd.isError(compressedSize)) {
      throw new IOException("ZSTD compression failed: " + Zstd.getErrorName(compressedSize));
    }
    compressedChunkBuffer.position(0);
    compressedChunkBuffer.limit(compressedSize);

    long written = 0L;
    while (written < compressedSize) {
      written += channel.write(compressedChunkBuffer);
    }
    chunkCompressed.add(true);
    chunkOffsets.add((chunkOffsets.get(chunkOffsets.size() - 1) + written));
    chunkBuffer.clear();
  }

  @Override
  public void close(boolean commitFilesFsync) throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    IOException failure = null;
    try {
      compressAndFlush();
      if (commitFilesFsync) {
        channel.force(false);
      }
    } catch (IOException e) {
      failure = e;
    } finally {
      chunkBufferPool.release(bufferPair);
      try {
        channel.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = e;
        }
      }
    }

    if (failure != null) {
      throw failure;
    }
    diskFileInfo.setBytesFlushed(chunkOffsets.get(chunkOffsets.size() - 1));
    diskFileInfo.replaceFileMeta(new ReduceFileMeta(chunkOffsets, chunkCompressed, chunkSize));
  }
}
