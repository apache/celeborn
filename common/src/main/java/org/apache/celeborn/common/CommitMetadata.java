package org.apache.celeborn.common;

import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CommitMetadata {

    private AtomicLong bytes = new AtomicLong();
    private CelebornCRC32 crc = new CelebornCRC32();

    public CommitMetadata() {

    }
    public CommitMetadata(long checksum, long numBytes) {
       this.bytes = new AtomicLong(numBytes);
       this.crc = new CelebornCRC32((int) checksum);
    }

    void addBytes(long bytes) {
        while (true) {
            long val = this.bytes.get();
            long newVal = val + bytes;
            if (this.bytes.compareAndSet(val, newVal)) {
                break;
            }
        }
    }

    public void addDataWithOffsetAndLength(byte[] rawDataBuf, int offset, int length) {
        addBytes(length);
        this.crc.addData(rawDataBuf, offset, length);
    }

    public void addCommitData(CommitMetadata commitMetadata) {
        addBytes(commitMetadata.bytes.longValue());
        this.crc.addChecksum(commitMetadata.getChecksum());
    }

    public int getChecksum() {
        return crc.get();
    }

    public long getBytes() {
        return bytes.get();
    }

    public void encode(ByteBuf buf) {
        buf.writeLong(this.getChecksum());
        buf.writeLong(this.bytes.get());
    }

    public static CommitMetadata decode(ByteBuf buf) {
        long checksum = buf.readLong();
        long numBytes = buf.readLong();
        return new CommitMetadata(checksum, numBytes);
    }

    public static boolean checkCommitMetadata(CommitMetadata expected, CommitMetadata actual) {
        boolean bytesMatch = expected.getBytes() == actual.getBytes();
        boolean checksumsMatch = expected.getChecksum() == actual.getChecksum();
        return bytesMatch && checksumsMatch;
    }

    public static List<String> checkMissingCommitMetadatas(
        int startMapIndex, int endMapIndex, int[] attempts, int shuffleId,
        Map<String, CommitMetadata> expectedCommitMetadataMap) {

        int end = (endMapIndex == Integer.MAX_VALUE) ? attempts.length : endMapIndex;
        List<String> missingKeys = new ArrayList<>();

        for (int index = startMapIndex; index < end; index++) {
            String key = Utils.makeMapKey(shuffleId, index, attempts[index]);
            CommitMetadata commitMetadata = expectedCommitMetadataMap.get(key);
            if (commitMetadata == null) {
                missingKeys.add(key);
            }
        }

        return missingKeys;
    }

    @Override
    public String toString() {
        return "CommitMetadata{" +
            "bytes=" + bytes.get() +
            ", crc=" + crc +
            '}';
    }
}
