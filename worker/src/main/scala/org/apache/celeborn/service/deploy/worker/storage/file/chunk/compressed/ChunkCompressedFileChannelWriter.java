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

package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import com.google.common.annotations.VisibleForTesting;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import io.netty.buffer.CompositeByteBuf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.service.deploy.worker.storage.file.FileChannelWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class ChunkCompressedFileChannelWriter extends FileChannelWriter {
    private final FileChannel channel;
    private final DiskFileInfo diskFileInfo;
    private final ZstdCompressCtx zstdCtx;
    private final ChunkBufferPool.BufferPair bufferPair;
    private ByteBuffer chunkBuffer;
    private ByteBuffer compressedChunkBuffer;
    private final List<Long> chunkOffsets;
    private final long chunkSize;
    // Reusable direct buffers for the flushLargeRecord path; lazily allocated and grown on
    // demand, retained for the lifetime of the writer to amortize allocation across records.
    private ByteBuffer largeInputDirect;
    private ByteBuffer largeOutputDirect;

    public ChunkCompressedFileChannelWriter(DiskFileInfo diskFileInfo, long chunkSize, int compressionLevel) throws IOException {
        this.diskFileInfo = diskFileInfo;
        this.chunkSize = chunkSize;
        channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
        zstdCtx = new ZstdCompressCtx();
        zstdCtx.setLevel(compressionLevel);
        bufferPair = ChunkBufferPool.getInstance().acquire(chunkSize);
        chunkBuffer = bufferPair.chunkBuffer;
        compressedChunkBuffer = bufferPair.compressedBuffer;
        chunkOffsets = new ArrayList<>();
        chunkOffsets.add(0L);
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
     * Compresses the whole large record as a single ZSTD frame in one JNI call using the
     * writer's owned {@link ZstdCompressCtx}, then writes the compressed bytes to the channel.
     *
     * If the source {@link CompositeByteBuf} is already backed by a single direct
     * {@link ByteBuffer}, that buffer is fed to ZSTD with zero copy. Otherwise the data is
     * consolidated into a reusable direct staging buffer first. The output direct buffer is
     * also reused across calls and grown on demand.
     */
    private void flushLargeRecord(CompositeByteBuf buffer) throws IOException {
        int srcLen = buffer.readableBytes();

        ByteBuffer src;
        int srcPos;
        if (buffer.nioBufferCount() == 1) {
            ByteBuffer single = buffer.nioBuffer();
            if (single.isDirect()) {
                src = single;
                srcPos = src.position();
            } else {
                src = consolidateIntoDirectInput(buffer, srcLen);
                srcPos = 0;
            }
        } else {
            src = consolidateIntoDirectInput(buffer, srcLen);
            srcPos = 0;
        }

        int boundLen = (int) Zstd.compressBound(srcLen);
        ByteBuffer dst = ensureLargeOutputCapacity(boundLen);

        int compressedSize;
        try {
            compressedSize = (int) zstdCtx.compressDirectByteBuffer(
                dst, 0, boundLen,
                src, srcPos, srcLen);
        } catch (RuntimeException e) {
            throw new IOException("Failed to compress large record with ZSTD.", e);
        }

        dst.position(0).limit(compressedSize);
        while (dst.hasRemaining()) {
            channel.write(dst);
        }

        chunkOffsets.add(channel.position());
    }

    private ByteBuffer consolidateIntoDirectInput(CompositeByteBuf buffer, int srcLen) {
        ByteBuffer dst = ensureLargeInputCapacity(srcLen);
        for (ByteBuffer component : buffer.nioBuffers()) {
            dst.put(component);
        }
        dst.flip();
        return dst;
    }

    private ByteBuffer ensureLargeInputCapacity(int n) {
        if (largeInputDirect == null || largeInputDirect.capacity() < n) {
            largeInputDirect = ByteBuffer.allocateDirect(n);
        } else {
            largeInputDirect.clear();
        }
        return largeInputDirect;
    }

    private ByteBuffer ensureLargeOutputCapacity(int n) {
        if (largeOutputDirect == null || largeOutputDirect.capacity() < n) {
            largeOutputDirect = ByteBuffer.allocateDirect(n);
        } else {
            largeOutputDirect.clear();
        }
        return largeOutputDirect;
    }

    @VisibleForTesting
    void compressAndFlush() throws IOException {
        int size = chunkBuffer.position();
        if (size == 0) return;
        chunkBuffer.position(0);
        chunkBuffer.limit(size);
        compressedChunkBuffer.clear();
        int compressedSize;
        try {
            compressedSize = (int) zstdCtx.compressDirectByteBuffer(
                    compressedChunkBuffer,
                    0,
                    compressedChunkBuffer.capacity(),
                    chunkBuffer,
                    0,
                    size);
        } catch (RuntimeException e) {
            throw new IOException("Failed to compress chunk with ZSTD.", e);
        }
        compressedChunkBuffer.position(0);
        compressedChunkBuffer.limit(compressedSize);

        long written = 0L;
        while (written < compressedSize) {
            written += channel.write(compressedChunkBuffer);
        }
        chunkOffsets.add((chunkOffsets.get(chunkOffsets.size() - 1) + written));
        chunkBuffer.clear();
    }

    @Override
    public void close(boolean commitFilesFsync) {
        try {
            compressAndFlush();
            if (commitFilesFsync) {
                channel.force(false);
            }
        } catch (IOException e) {
            // log and ignore
        } finally {
            try {
                channel.close();
            } catch (IOException e) {
                // log and ignore
            }
            zstdCtx.close();
        }

        diskFileInfo.setBytesFlushed(chunkOffsets.get(chunkOffsets.size() - 1));
        diskFileInfo.replaceFileMeta(new ReduceFileMeta(chunkOffsets, chunkSize));
        ChunkBufferPool.getInstance().release(bufferPair);
    }
}
