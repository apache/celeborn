package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import io.netty.buffer.CompositeByteBuf;
import org.apache.celeborn.common.meta.DiskFileInfo;
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
    private ByteBuffer chunkBuffer;
    private final List<Integer> chunkOffsets;

    public ChunkCompressedFileChannelWriter(DiskFileInfo diskFileInfo) throws IOException {
        this.diskFileInfo = diskFileInfo;
        channel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getFilePath());
        chunkBuffer = MmapMemoryManager.getInstance().allocateBuffer(/* 8mb */ 8 * 1024 * 1024);
        chunkOffsets = new ArrayList<>();
    }

    @Override
    public void write(CompositeByteBuf buffer, boolean gatherApiEnabled) throws IOException {
        if (buffer.readableBytes() > chunkBuffer.capacity()) {
            compressAndFlush(buffer.readableBytes());
        }

        chunkBuffer.put(buffer.nioBuffer());
    }

    private void compressAndFlush(int newDataSize) throws IOException {
        // Compress the data in chunkBuffer and write to channel, and also update chunkOffsets
        // Then clear chunkBuffer and make it ready for the new data of size newDataSize
        // Note that we may need to call this method multiple times if newDataSize is larger than chunkBuffer.capacity()
        int size = chunkBuffer.position();
        chunkBuffer.position(0);
        chunkBuffer.limit(size);
        long written = 0L;
        do {
            written += channel.write(chunkBuffer);
        } while (written != size);

        chunkBuffer.position(0);
        if (newDataSize > chunkBuffer.limit()) {
            chunkBuffer = MmapMemoryManager.getInstance().allocateBuffer(newDataSize);
        }
    }

    @Override
    public void close(boolean commitFilesFsync) {
        // Update offsets etc for diskFileInfo
        // Also set a new ReduceFileMeta with updated chunkOffsets
        try {
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
        }
    }
}
