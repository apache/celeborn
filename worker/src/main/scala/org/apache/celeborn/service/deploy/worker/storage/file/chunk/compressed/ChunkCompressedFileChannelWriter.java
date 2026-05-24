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
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.CompositeByteBuf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.service.deploy.worker.storage.file.FileChannelWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class ChunkCompressedFileChannelWriter extends FileChannelWriter {
    private static final int ZSTD_COMPRESSION_LEVEL = Zstd.defaultCompressionLevel();

    private final FileChannel channel;
    private final DiskFileInfo diskFileInfo;
    private final ZstdCompressCtx zstdCtx;
    private final ChunkBufferPool.BufferPair bufferPair;
    private ByteBuffer chunkBuffer;
    private ByteBuffer compressedChunkBuffer;
    private final List<Long> chunkOffsets;
    private final long chunkSize;

    public ChunkCompressedFileChannelWriter(DiskFileInfo diskFileInfo, long chunkSize) throws IOException {
        this.diskFileInfo = diskFileInfo;
        this.chunkSize = chunkSize;
        channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
        zstdCtx = new ZstdCompressCtx().setLevel(ZSTD_COMPRESSION_LEVEL);
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
     * Compresses the entire buffer as a single chunk and writes it to the channel.
     * Uses ZstdOutputStream for streaming compression without an intermediate compressed buffer.
     * The OutputStream wrapper prevents ZstdOutputStream.close() from closing the FileChannel.
     */
    private void flushLargeRecord(CompositeByteBuf buffer) throws IOException {
        OutputStream channelOut = new OutputStream() {
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                ByteBuffer buf = ByteBuffer.wrap(b, off, len);
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
            }

            @Override
            public void write(int b) throws IOException {
                channel.write(ByteBuffer.wrap(new byte[]{(byte) b}));
            }
        };

        try (ZstdOutputStream zstdOut = new ZstdOutputStream(channelOut, ZSTD_COMPRESSION_LEVEL)) {
            byte[] buf = new byte[8192];
            while (buffer.isReadable()) {
                int toRead = Math.min(buffer.readableBytes(), buf.length);
                buffer.readBytes(buf, 0, toRead);
                zstdOut.write(buf, 0, toRead);
            }
        } // close() finalizes the ZSTD frame and flushes all bytes to the channel

        chunkOffsets.add(channel.position());
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
            compressedSize =
                zstdCtx.compressDirectByteBuffer(
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
