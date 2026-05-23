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
            // Flush large record, uncompressed
            flushLargeRecord(buffer, gatherApiEnabled);
            return;
        }

        if (buffer.readableBytes() > chunkBuffer.capacity()) {
            compressAndFlush();
        }

        ByteBuffer[] buffers = buffer.nioBuffers();
        for (ByteBuffer byteBuffer : buffers) {
            while (byteBuffer.hasRemaining()) {
                chunkBuffer.put(byteBuffer);
            }
        }
    }

    private void flushLargeRecord(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException {
        ByteBuffer[] buffers = buffer.nioBuffers();
        long size = buffer.readableBytes();
        if (gatherApiEnabled) {
            int readableBytes = buffer.readableBytes();
            long written = 0L;
            do {
                written = channel.write(buffers) + written;
            } while (written != readableBytes);
        } else {
            for (ByteBuffer byteBuffer : buffers) {
                while (byteBuffer.hasRemaining()) {
                    channel.write(byteBuffer);
                }
            }
        }
        chunkOffsets.add(chunkOffsets.get(chunkOffsets.size() - 1) + size);
    }

    @VisibleForTesting
    void compressAndFlush() throws IOException {
        // Compress the data in chunkBuffer and write to channel, and also update chunkOffsets
        // Then clear chunkBuffer and make it ready for the new data of size newDataSize
        // Note that we may need to call this method multiple times if newDataSize is larger than chunkBuffer.capacity()
        int size = chunkBuffer.position();
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
        // Update offsets etc for diskFileInfo
        // Also set a new ReduceFileMeta with updated chunkOffsets
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
