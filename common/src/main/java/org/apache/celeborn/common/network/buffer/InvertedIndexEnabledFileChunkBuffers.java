package org.apache.celeborn.common.network.buffer;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import java.nio.ByteBuffer;

public class InvertedIndexEnabledFileChunkBuffers extends ChunkBuffers {

    private final InvertedIndexReducePartitionDataReader reader;
    private final int[] chunkIds;

    public InvertedIndexEnabledFileChunkBuffers(DiskFileInfo diskFileInfo, int startIndex, int endIndex) throws Exception {
        super(new ReduceFileMeta(0));
        reader = InvertedIndexReducePartitionDataReader.getReader(diskFileInfo);
        chunkIds = reader.getChunkIds(startIndex, endIndex);
    }

    public int getNumChunks() {
        return chunkIds.length;
    }

    @Override
    public ManagedBuffer chunk(int chunkIndex, int offset, int len) {
        try {
            ByteBuffer chunkBuffer = reader.getChunkBuffer(chunkIds[chunkIndex]);
            chunkBuffer.position(offset);
            chunkBuffer.limit(Math.min(chunkBuffer.capacity(), offset + len));
            return new NioManagedBuffer(chunkBuffer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get chunk buffer for chunk index: " + chunkIndex, e);
        }

    }
}
