package org.apache.celeborn.service.deploy.worker.storage.file.chunk.compressed;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Pool of reusable (chunkBuffer, compressedBuffer) pairs for ChunkCompressedFileChannelWriter,
 * bucketed by chunkSize so every acquired pair is exactly the right capacity.
 */
public class ChunkBufferPool {

    public static class BufferPair {
        final ByteBuffer chunkBuffer;
        final ByteBuffer compressedBuffer;
        final long chunkSize;

        BufferPair(ByteBuffer chunkBuffer, ByteBuffer compressedBuffer, long chunkSize) {
            this.chunkBuffer = chunkBuffer;
            this.compressedBuffer = compressedBuffer;
            this.chunkSize = chunkSize;
        }
    }

    private static final ChunkBufferPool INSTANCE = new ChunkBufferPool();

    private final ConcurrentHashMap<Long, ConcurrentLinkedDeque<BufferPair>> poolMap =
            new ConcurrentHashMap<>();

    private ChunkBufferPool() {}

    public static ChunkBufferPool getInstance() {
        return INSTANCE;
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
        ByteBuffer chunkBuf = MmapMemoryManager.getInstance().allocateBuffer((int) chunkSize);
        // allocateDirect, NOT MmapMemoryManager: mmap duplicates share one backing region, so
        // after clear() both chunkBuf and a mmap-backed compressedBuf would have position=0
        // pointing to the same physical address. ZSTD would then write its frame header to
        // mmap[0..N] before reading mmap[0..N] as input, silently corrupting the source.
        ByteBuffer compressedBuf = MmapMemoryManager.getInstance().allocateBuffer((int) chunkSize);
        return new BufferPair(chunkBuf, compressedBuf, chunkSize);
    }

    /** Returns the pair to the bucket matching its chunkSize. */
    public void release(BufferPair pair) {
        pair.chunkBuffer.clear();
        pair.compressedBuffer.clear();
        poolMap.computeIfAbsent(pair.chunkSize, k -> new ConcurrentLinkedDeque<>())
               .offerFirst(pair);
    }
}
