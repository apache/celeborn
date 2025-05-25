package org.apache.celeborn.service.deploy.worker.storage.invertedindexreduce;

import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.service.deploy.worker.storage.DeviceMonitor;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriterContext;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InvertedIndexReducePartitionDataWriter extends PartitionDataWriter {
    // Footer size assuming 10k chunks will be written, this can be resized during runtime
    private static int INITIALIZE_FOOTER_SIZE = Integer.BYTES * 10000;
    private static int DEFAULT_CHUNK_INITIAL_SIZE = 2 * 1024 * 1024; // 2MB

    private long currentDocId;
    private long currentDocOffset;

    private final Map<Integer, ByteBuffer> mapLevelBuffers;
    private final Map<Integer, RoaringBitmap> mapIdInvertecIndex;
    private final ByteBuffer footerBuffer;

    public InvertedIndexReducePartitionDataWriter(StorageManager storageManager, AbstractSource workerSource,
                                                  CelebornConf conf, DeviceMonitor deviceMonitor,
                                                  PartitionDataWriterContext writerContext)
            throws IOException {
        super(storageManager, workerSource, conf, deviceMonitor, writerContext, false);
        mapIdInvertecIndex = new HashMap<>();
        currentDocId = 0;
        // The first 8 + 8 + 4 bytes reserved for footer start offset, footer end offset, and number of bitmaps
        channel.position(Long.BYTES + Long.BYTES + Integer.BYTES);
        footerBuffer = ByteBuffer.allocateDirect(INITIALIZE_FOOTER_SIZE);
        mapLevelBuffers = new ConcurrentHashMap<>();
    }

    @Override
    public void write(ByteBuf data) throws IOException {
        data.markReaderIndex();
        byte[] header = new byte[4];
        data.markReaderIndex();
        data.readBytes(header);
        data.resetReaderIndex();
        int mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);

        data.resetReaderIndex();

        // Write into current chunk and increment docId
        writeInternal(mapId, data);
    }

    private void writeInternal(int mapId, ByteBuf data) throws IOException {
        mapLevelBuffers.compute(mapId, (key, value) -> {
            if (value == null) {
                if (data.readableBytes() > DEFAULT_CHUNK_INITIAL_SIZE) {
                    value = ByteBuffer.allocateDirect(data.readableBytes());
                } else {
                    value = ByteBuffer.allocateDirect(DEFAULT_CHUNK_INITIAL_SIZE);
                }
            }
            // Try to add to current buffer if possible
            if (value.remaining() >= data.readableBytes()) {
                value.put(data.nioBuffer());
            } else {
                // Flush and recreate
                try {
                    flushMapIdBuffer(key, value);
                } catch (IOException e) {
                    // TODO
                }
                if (data.readableBytes() > DEFAULT_CHUNK_INITIAL_SIZE) {
                    value = ByteBuffer.allocateDirect(data.readableBytes());
                } else {
                    value = ByteBuffer.allocateDirect(DEFAULT_CHUNK_INITIAL_SIZE);
                }
                value.put(data.nioBuffer());
            }

            return value;
        });
    }

    // This is always called from the main thread, so we don't need to worry about concurrency
    private void flushMapIdBuffer(int mapId, ByteBuffer buffer) throws IOException {
        mapIdInvertecIndex.compute(
                mapId,
                (key, value) -> {
                    if (value == null) {
                        value = new RoaringBitmap();
                    }
                    try {
                        long docId = writeBufferToFile(buffer);
                        value.add(docId, docId + 1);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return value;
                });
    }

    private synchronized long writeBufferToFile(ByteBuffer buffer) throws IOException {
        buffer.flip();
        long start = channel.position();
        channel.write(buffer);
        footerBuffer.putLong(start);
        return currentDocId++;
    }

    @Override
    public long close() throws IOException {
        return super.close(() -> {
            try {
                closeInternal();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close InvertedIndexReducePartitionDataWriter", e);
            }
        }, () -> {}, () -> {});
    }

    private void closeInternal() throws Exception {
        for (Map.Entry<Integer, ByteBuffer> entry : mapLevelBuffers.entrySet()) {
            if (entry.getValue().position() != 0) {
                flushMapIdBuffer(entry.getKey(), entry.getValue());
            }
        }

//        System.out.println("Docs size: " + channel.position());
        long footerStartOffset = channel.position();
        footerBuffer.putLong(channel.position());
        footerBuffer.flip();
        channel.write(footerBuffer);

        long footerEndPosition = channel.position();
//        System.out.println("Footer size: " + (channel.position() - footerStartOffset));
        // Write inverted index
        // Layout
        /*
         * [number of bitmaps]
         * [start offset bitmap1]
         * [start offset bitmap2]
         * ...
         * [start offset bitmapN]
         * [end offset of bitmapN]
         * [bitmap1]
         * [bitmap2]
         * ...
         * [bitmapN]
         * */
        long invIndexOffsetPosition = channel.position();
        ByteBuffer invOffsetBuffer = ByteBuffer.allocate(Long.BYTES * (mapIdInvertecIndex.size() + 1));
        channel.position(channel.position() + Long.BYTES * (mapIdInvertecIndex.size() + 1));
        long footerStartOffsetForInvIndex = channel.position();
        invOffsetBuffer.putLong(0);

        for (Map.Entry<Integer, RoaringBitmap> entry : mapIdInvertecIndex.entrySet()) {
            RoaringBitmap bitmap = entry.getValue();
            int size = bitmap.serializedSizeInBytes();

            ByteBuffer buffer = ByteBuffer.allocate(size);
            bitmap.serialize(buffer);
            buffer.flip();
            channel.write(buffer);
            long offsetValue = channel.position();
            invOffsetBuffer.putLong(offsetValue - footerStartOffsetForInvIndex);
        }

        // Write the inverted index offsets
        channel.position(invIndexOffsetPosition);
        invOffsetBuffer.flip();
        channel.write(invOffsetBuffer);

//        System.out.println("Inv index size: " + (channel.position() - footerEndPosition));

        channel.position(0);

        // Write the footer offset
        ByteBuffer offsetValBuffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Integer.BYTES);
        offsetValBuffer.putLong(footerStartOffset);
        offsetValBuffer.putLong(footerEndPosition);
        offsetValBuffer.putInt(mapIdInvertecIndex.size());
        offsetValBuffer.flip();

        channel.write(offsetValBuffer);
    }

}
