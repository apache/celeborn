package org.apache.celeborn.common.network.buffer;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InvertedIndexReducePartitionDataReader {
    private final File file;
    private final ByteBuffer footerBuffer;
    private final ByteBuffer invIndexOffsetBuffer;
    private final int numMappers;
    private final ByteBuffer invIndexBitmapBuffer;

    public InvertedIndexReducePartitionDataReader(File file) {
        // Mmap file
        try {
            this.file = file;
            FileChannel channel = new RandomAccessFile(file, "r").getChannel();

            // TODO it should be possible to mmap the file only once, and reuse across different readers to reduce number of open FDs
            ByteBuffer headerBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Long.BYTES * 2 + Integer.BYTES);
            long footerStartOffset = headerBuffer.getLong(0);
            long footerEndOffset = headerBuffer.getLong(Long.BYTES);
            numMappers = headerBuffer.getInt(Long.BYTES + Long.BYTES);

            // TODO we can mmap once, and all buffers could be views / duplicates to reduce FD usage
            footerBuffer = channel.map(FileChannel.MapMode.READ_ONLY, footerStartOffset, footerEndOffset - footerStartOffset);
            invIndexOffsetBuffer = channel.map(FileChannel.MapMode.READ_ONLY, footerEndOffset, Long.BYTES * (numMappers + 1));
            invIndexBitmapBuffer = channel.map(FileChannel.MapMode.READ_ONLY,
                    footerEndOffset + Long.BYTES * (numMappers + 1), channel.size() - (footerEndOffset + Long.BYTES * (numMappers + 1)));
        } catch (Exception e) {
            throw new RuntimeException("Failed to map file: " + file.getAbsolutePath(), e);
        }
    }

    public ByteBuffer getChunkBuffer(int chunkId) throws IOException {
        // Read the chunk data from the file
        long chunkStartOffset = footerBuffer.getLong(chunkId * Long.BYTES);
        long chunkEndOffset = footerBuffer.getLong((chunkId + 1) * Long.BYTES);

        // Check if the chunk size is valid
        FileChannel channel = new RandomAccessFile(file, "r").getChannel();

        ByteBuffer buf = ByteBuffer.allocate((int) (chunkEndOffset - chunkStartOffset));
        channel.position(chunkStartOffset);
        while (buf.remaining() != 0) {
            if (channel.read(buf) == -1) {
                throw new IOException(
                        String.format(
                                "Reached EOF before filling buffer\n" + "offset=%s\nfile=%s\nbuf.remaining=%s",
                                chunkStartOffset, file.getAbsoluteFile(), buf.remaining()));
            }
        }
        buf.flip();
        return buf;
    }

    public int[] getChunkIds(int startMapperId, int endMapperId) throws Exception {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int i = Math.max(0, startMapperId); i <= Math.min(endMapperId, numMappers - 1); i++) {
            bitmap.or(getBitmapForMapperId(i));
        }

        return bitmap.stream().toArray();
    }

    public int[] getChunkIds(int[] mapperIds) throws Exception {
        // 1) Get bitmap offsets
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int mapperId : mapperIds) {
            long startOffset = invIndexOffsetBuffer.getLong(mapperId * Long.BYTES);
            long endOffset = invIndexOffsetBuffer.getLong((mapperId + 1) * Long.BYTES);

            ByteBuffer data = invIndexBitmapBuffer.duplicate();
            data.position((int) startOffset);
            data.limit((int) endOffset);
            bitmap.or(new ImmutableRoaringBitmap(data));
        }

        // 2) Return the chunk IDs as an array
        return bitmap.stream().toArray();
    }

    private ImmutableRoaringBitmap getBitmapForMapperId(int mapperId) {
        long startOffset = invIndexOffsetBuffer.getLong(mapperId * Long.BYTES);
        long endOffset = invIndexOffsetBuffer.getLong((mapperId + 1) * Long.BYTES);

        ByteBuffer data = invIndexBitmapBuffer.duplicate();
        data.position((int) startOffset);
        data.limit((int) endOffset);
        return new ImmutableRoaringBitmap(data);
    }


    // FilePath -> reader
    private static Map<String, InvertedIndexReducePartitionDataReader> readers = new ConcurrentHashMap<>();
    public static InvertedIndexReducePartitionDataReader getReader(DiskFileInfo diskFileInfo) {
        return readers.computeIfAbsent(diskFileInfo.getFilePath(), filePath -> {
            try {
                return new InvertedIndexReducePartitionDataReader(diskFileInfo.getFile());
            } catch (Exception e) {
                throw new RuntimeException("Failed to create InvertedIndexReducePartitionDataReader for file: " + filePath, e);
            }
        });
    }


}
