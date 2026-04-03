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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.Utils;

/**
 * Sort-based pusher for MapReduce shuffle data to Celeborn.
 *
 * <p>This implementation uses primitive int arrays to store record metadata (offsets, key lengths,
 * value lengths) during data collection to minimize object allocation. During flush, temporary
 * Record objects are created for sorting and immediately garbage collected in Young Gen.
 *
 * <p>To prevent memory pressure during sorting, the implementation triggers early spill when record
 * count exceeds a threshold (5M records by default), ensuring temporary objects fit within Young
 * Gen capacity.
 */
public class CelebornSortBasedPusher<K, V> extends OutputStream {
  private final Logger logger = LoggerFactory.getLogger(CelebornSortBasedPusher.class);
  private final int mapId;
  private final int attempt;
  private final int numMappers;
  private final int numReducers;
  private final ShuffleClient shuffleClient;
  private final int maxIOBufferSize;
  private final int spillIOBufferSize;
  private final Serializer<K> kSer;
  private final Serializer<V> vSer;
  private final RawComparator<K> comparator;
  private final AtomicReference<Exception> exception = new AtomicReference<>();
  private final Counters.Counter mapOutputByteCounter;
  private final Counters.Counter mapOutputRecordCounter;
  private final Map<Integer, KVBufferInfo> partitionedKVBuffers;
  private int writePos;
  private byte[] serializedKV;
  private final int maxPushDataSize;

  public CelebornSortBasedPusher(
      int numMappers,
      int numReducers,
      int mapId,
      int attemptId,
      Serializer<K> kSer,
      Serializer<V> vSer,
      int maxIOBufferSize,
      int spillIOBufferSize,
      RawComparator<K> comparator,
      Counters.Counter mapOutputByteCounter,
      Counters.Counter mapOutputRecordCounter,
      ShuffleClient shuffleClient,
      CelebornConf celebornConf) {
    this.numMappers = numMappers;
    this.numReducers = numReducers;
    this.mapId = mapId;
    this.attempt = attemptId;
    this.kSer = kSer;
    this.vSer = vSer;
    this.maxIOBufferSize = maxIOBufferSize;
    this.spillIOBufferSize = spillIOBufferSize;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
    this.comparator = comparator;
    this.shuffleClient = shuffleClient;
    partitionedKVBuffers = new HashMap<>();
    serializedKV = new byte[maxIOBufferSize];
    maxPushDataSize = (int) celebornConf.clientMrMaxPushData();
    logger.info(
        "Sort based push initialized with"
            + " numMappers:{} numReducers:{} mapId:{} attemptId:{}"
            + " maxIOBufferSize:{} spillIOBufferSize:{}",
        numMappers,
        numReducers,
        mapId,
        attemptId,
        maxIOBufferSize,
        spillIOBufferSize);
    try {
      kSer.open(this);
      vSer.open(this);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  public void insert(K key, V value, int partition) {
    try {
      // Check if we should spill based on buffer size
      if (writePos >= spillIOBufferSize) {
        // needs to sort and flush data
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Data is large enough {}/{}/{}, trigger sort and flush",
              Utils.bytesToString(writePos),
              Utils.bytesToString(spillIOBufferSize),
              Utils.bytesToString(maxIOBufferSize));
        }
        sortKVs();
        sendKVAndUpdateWritePos();
      }

      // Additional check: limit total record count to avoid memory pressure during sort
      // If total records exceed safe threshold, force an early spill
      int totalRecords = getTotalRecordCount();
      final int MAX_RECORDS_BEFORE_SPILL = 5_000_000; // 5M records = ~120MB temporary objects

      if (totalRecords >= MAX_RECORDS_BEFORE_SPILL && writePos > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Record count {} exceeds safe threshold {}, forcing early spill",
              totalRecords,
              MAX_RECORDS_BEFORE_SPILL);
        }
        sortKVs();
        sendKVAndUpdateWritePos();
      }

      int dataLen = insertRecordInternal(key, value, partition);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Sort based pusher insert into partition:{} with {} bytes", partition, dataLen);
      }
      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(dataLen);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  /** Get total record count across all partitions. */
  private int getTotalRecordCount() {
    int total = 0;
    for (KVBufferInfo bufferInfo : partitionedKVBuffers.values()) {
      total += bufferInfo.count;
    }
    return total;
  }

  private void sendKVAndUpdateWritePos() throws IOException {
    Iterator<Map.Entry<Integer, KVBufferInfo>> entryIter =
        partitionedKVBuffers.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Integer, KVBufferInfo> entry = entryIter.next();
      entryIter.remove();
      int partition = entry.getKey();
      KVBufferInfo bufferInfo = entry.getValue();
      int partitionKVTotalLen = 0;
      int batchStartIdx = 0;
      // process buffers for specific partition (arrays are already sorted in-place)
      for (int i = 0; i < bufferInfo.count; i++) {
        partitionKVTotalLen += bufferInfo.keyLens[i] + bufferInfo.valueLens[i];
        if (partitionKVTotalLen > maxPushDataSize) {
          // limit max size of pushdata to avoid possible memory issue in Celeborn worker
          // data layout
          // pushdata header (16) + pushDataLen(4) +
          // [varKeyLen+varValLen+serializedRecord(x)][...]
          sendSortedBuffersPartition(
              partition, bufferInfo, batchStartIdx, i - batchStartIdx + 1, partitionKVTotalLen);
          // move batch start
          partitionKVTotalLen = 0;
          batchStartIdx = i + 1;
        }
      }
      // send remaining records
      if (batchStartIdx < bufferInfo.count) {
        // recalculate total length for remaining records
        partitionKVTotalLen = 0;
        for (int i = batchStartIdx; i < bufferInfo.count; i++) {
          partitionKVTotalLen += bufferInfo.keyLens[i] + bufferInfo.valueLens[i];
        }
        sendSortedBuffersPartition(
            partition,
            bufferInfo,
            batchStartIdx,
            bufferInfo.count - batchStartIdx,
            partitionKVTotalLen);
      }
      // Clear buffer info for reuse
      bufferInfo.clear();
    }
    // all data sent
    partitionedKVBuffers.clear();
    writePos = 0;
  }

  private void sendSortedBuffersPartition(
      int partition, KVBufferInfo bufferInfo, int startIdx, int count, int partitionKVTotalLen)
      throws IOException {
    int extraSize = 0;
    for (int i = startIdx; i < startIdx + count; i++) {
      extraSize += WritableUtils.getVIntSize(bufferInfo.keyLens[i]);
      extraSize += WritableUtils.getVIntSize(bufferInfo.valueLens[i]);
    }
    // copied from hadoop logic
    extraSize += WritableUtils.getVIntSize(-1);
    extraSize += WritableUtils.getVIntSize(-1);
    // whole buffer's size + [(keyLen+valueLen)+(serializedKey+serializedValue)]
    byte[] pkvs = new byte[4 + extraSize + partitionKVTotalLen];
    int pkvsPos = 4;
    Platform.putInt(pkvs, Platform.BYTE_ARRAY_OFFSET, partitionKVTotalLen + extraSize);
    for (int i = startIdx; i < startIdx + count; i++) {
      int kLen = bufferInfo.keyLens[i];
      int vLen = bufferInfo.valueLens[i];
      int recordLen = kLen + vLen;
      // write key len
      pkvsPos = writeVLong(pkvs, pkvsPos, kLen);
      // write value len
      pkvsPos = writeVLong(pkvs, pkvsPos, vLen);
      // write serialized record
      System.arraycopy(serializedKV, bufferInfo.offsets[i], pkvs, pkvsPos, recordLen);
      pkvsPos += recordLen;
    }
    // finally write -1 two times
    pkvsPos = writeVLong(pkvs, pkvsPos, -1);
    writeVLong(pkvs, pkvsPos, -1);
    int compressedSize =
        shuffleClient.pushData(
            // there is only 1 shuffle for a mr application
            0,
            mapId,
            attempt,
            partition,
            pkvs,
            0,
            4 + extraSize + partitionKVTotalLen,
            numMappers,
            numReducers);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Send sorted buffer mapId:{} attemptId:{} to partition:{} uncompressed size:{} compressed size:{}",
          mapId,
          attempt,
          partition,
          Utils.bytesToString(4 + extraSize + partitionKVTotalLen),
          Utils.bytesToString(compressedSize));
    }
  }

  /**
   * Write variable length int to array Modified from
   * org.apache.hadoop.io.WritableUtils#writeVLong(java.io.DataOutput, long)
   */
  private int writeVLong(byte[] data, int offset, long dataInt) {
    if (dataInt >= -112L && dataInt <= 127L) {
      data[offset++] = (byte) ((int) dataInt);
      return offset;
    }

    int len = -112;
    if (dataInt < 0L) {
      dataInt ^= -1L;
      len = -120;
    }

    long tmp = dataInt;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    data[offset++] = (byte) len;

    len = len < -120 ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; --idx) {
      int shiftBits = (idx - 1) * 8;
      long mask = 0xFFL << shiftBits;
      data[offset++] = ((byte) ((int) ((dataInt & mask) >> shiftBits)));
    }
    return offset;
  }

  private void sortKVs() {
    // Maximum number of temporary Record objects to create at once.
    // This limits Young Gen pressure and prevents Full GC.
    // Record size ~24 bytes, so 5M records = 120MB at peak
    final int MAX_SORT_RECORDS = 5_000_000;

    for (Map.Entry<Integer, KVBufferInfo> partitionKVEntry : partitionedKVBuffers.entrySet()) {
      KVBufferInfo bufferInfo = partitionKVEntry.getValue();
      if (bufferInfo.count <= 1) {
        continue;
      }

      // If too many records, split into batches
      if (bufferInfo.count > MAX_SORT_RECORDS) {
        // Sort and flush each batch immediately
        int remaining = bufferInfo.count;
        int start = 0;

        while (remaining > 0) {
          int batchSize = Math.min(remaining, MAX_SORT_RECORDS);
          int end = start + batchSize;

          // Sort this batch
          sortBatch(bufferInfo, start, end);

          // Send this batch immediately to free memory
          sendPartialBuffer(bufferInfo, start, batchSize);

          start = end;
          remaining -= batchSize;
        }

        // After all batches sent, clear the buffer
        bufferInfo.clear();
      } else {
        // Full sort (single batch)
        sortBatch(bufferInfo, 0, bufferInfo.count);
      }
    }
  }

  /** Sort a batch of records from start (inclusive) to end (exclusive). */
  private void sortBatch(KVBufferInfo bufferInfo, int start, int end) {
    int size = end - start;

    // Create temporary Record objects
    Record[] records = new Record[size];
    for (int i = 0; i < size; i++) {
      records[i] =
          new Record(
              serializedKV,
              comparator,
              bufferInfo.offsets[start + i],
              bufferInfo.keyLens[start + i],
              bufferInfo.valueLens[start + i]);
    }

    // Sort using Arrays.sort
    Arrays.sort(records);

    // Write back sorted results
    for (int i = 0; i < size; i++) {
      bufferInfo.offsets[start + i] = records[i].offset;
      bufferInfo.keyLens[start + i] = records[i].kLen;
      bufferInfo.valueLens[start + i] = records[i].vLen;
    }
  }

  /**
   * Send a portion of the buffer (after it's been sorted). This method requires careful re-design
   * of sendKVAndUpdateWritePos to work with partial sends.
   *
   * <p>For simplicity, we revert to the original approach of sorting the entire buffer when it's
   * safe (within MAX_SORT_RECORDS). But if we exceed the limit, we need to handle partial sends
   * differently.
   *
   * <p>For the first version, let's just throw if we exceed MAX_SORT_RECORDS, and let the user
   * adjust heap size or spill.percent instead.
   */
  private void sendPartialBuffer(KVBufferInfo bufferInfo, int start, int count) {
    // TODO: This needs careful redesign of sendKVAndUpdateWritePos
    // For now, throw to force user to adjust configuration
    throw new UnsupportedOperationException(
        "Buffer too large for single batch sorting. "
            + "Please reduce mapreduce.task.io.sort.mb or increase mapreduce.map.java.opts heap.");
  }

  /**
   * Temporary record for sorting. These objects are created only during sort, then garbage
   * collected in Young Gen. Static class to avoid holding reference to outer class instance.
   */
  private static class Record implements Comparable<Record> {
    private final byte[] serializedKV;
    private final RawComparator comparator;
    final int offset;
    final int kLen;
    final int vLen;

    Record(byte[] serializedKV, RawComparator comparator, int offset, int kLen, int vLen) {
      this.serializedKV = serializedKV;
      this.comparator = comparator;
      this.offset = offset;
      this.kLen = kLen;
      this.vLen = vLen;
    }

    @Override
    public int compareTo(Record other) {
      return comparator.compare(
          serializedKV, offset, kLen, other.serializedKV, other.offset, other.kLen);
    }
  }

  private int insertRecordInternal(K key, V value, int partition) throws IOException {
    int offset = writePos;
    int keyLen;
    int valLen;
    kSer.serialize(key);
    keyLen = writePos - offset;
    vSer.serialize(value);
    valLen = writePos - keyLen - offset;
    KVBufferInfo bufferInfo =
        partitionedKVBuffers.computeIfAbsent(
            partition, v -> new KVBufferInfo(1024)); // Initial capacity: 1024 records
    // Store metadata directly in primitive arrays, no object allocation
    bufferInfo.add(offset, keyLen, valLen);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Pusher insert into buffer partition:{} offset:{} keyLen:{} valueLen:{} size:{}",
          partition,
          offset,
          keyLen,
          valLen,
          partitionedKVBuffers.size());
    }
    return keyLen + valLen;
  }

  public void checkException() throws IOException {
    if (exception.get() != null) {
      throw new IOException("Write data to celeborn failed", exception.get());
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (writePos < maxIOBufferSize) {
      serializedKV[writePos] = (byte) b;
      writePos++;
    } else {
      logger.warn("Sort push memory high, write pos {} max size {}", writePos, maxIOBufferSize);
      throw new IOException("Sort pusher memory exhausted.");
    }
  }

  @Override
  public void flush() {
    logger.info("Sort based pusher called flush");
    try {
      sortKVs();
      sendKVAndUpdateWritePos();
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  @Override
  public void close() {
    flush();
    try {
      logger.info(
          "Call mapper end shuffleId:{} mapId:{} attemptId:{} numMappers:{}",
          0,
          mapId,
          attempt,
          numMappers);
      shuffleClient.mapperEnd(0, mapId, attempt, numMappers, numReducers);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
    partitionedKVBuffers.clear();
    serializedKV = null;
  }

  /**
   * Buffer info to manage serialized key-value records for each partition. Uses primitive int
   * arrays to store metadata instead of object arrays, significantly reducing memory overhead.
   *
   * <p>Memory comparison for 1 million records: - ArrayList<SerializedKV>: ~32MB (8MB references +
   * 24MB objects) - This approach: ~12MB (4MB×3 int arrays)
   *
   * <p>Saves 62.5% memory!
   */
  static class KVBufferInfo {
    int[] offsets; // Store key offset in serializedKV buffer
    int[] keyLens; // Store key length
    int[] valueLens; // Store value length
    int count;
    int capacity;

    KVBufferInfo(int initialCapacity) {
      this.offsets = new int[initialCapacity];
      this.keyLens = new int[initialCapacity];
      this.valueLens = new int[initialCapacity];
      this.capacity = initialCapacity;
      this.count = 0;
    }

    void add(int offset, int kLen, int vLen) {
      if (count >= capacity) {
        // Expand arrays with 2x growth strategy
        int newCapacity = capacity * 2;

        int[] newOffsets = new int[newCapacity];
        int[] newKeyLens = new int[newCapacity];
        int[] newValueLens = new int[newCapacity];

        System.arraycopy(offsets, 0, newOffsets, 0, count);
        System.arraycopy(keyLens, 0, newKeyLens, 0, count);
        System.arraycopy(valueLens, 0, newValueLens, 0, count);

        offsets = newOffsets;
        keyLens = newKeyLens;
        valueLens = newValueLens;
        capacity = newCapacity;
      }
      offsets[count] = offset;
      keyLens[count] = kLen;
      valueLens[count] = vLen;
      count++;
    }

    void clear() {
      count = 0;
      // Note: We don't clear the arrays themselves to avoid overhead
      // They will be overwritten as new data is added
    }
  }
}
