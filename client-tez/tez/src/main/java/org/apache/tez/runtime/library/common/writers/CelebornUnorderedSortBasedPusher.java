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

package org.apache.tez.runtime.library.common.writers;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.Utils;

public class CelebornUnorderedSortBasedPusher<K, V> extends OutputStream {
  private final Logger logger = LoggerFactory.getLogger(CelebornUnorderedSortBasedPusher.class);
  private final int mapId;
  private final int attempt;
  private final int numMappers;
  private final int numPartitions;
  private final int numOutputs;
  private final ShuffleClient shuffleClient;
  private final int maxIOBufferSize;
  private final int spillIOBufferSize;
  private final Serializer<K> kSer;
  private final Serializer<V> vSer;
  private final AtomicReference<Exception> exception = new AtomicReference<>();
  private final LongAdder mapOutputByteCounter = new LongAdder();
  private final LongAdder mapOutputRecordCounter = new LongAdder();
  private final Map<Integer, List<SerializedKV>> partitionedKVs;
  private int writePos;
  private byte[] serializedKV;
  private final int maxPushDataSize;
  private Map<Integer, AtomicInteger> recordsPerPartition;
  private Map<Integer, AtomicLong> bytesPerPartition;
  private int shuffleId;

  public CelebornUnorderedSortBasedPusher(
      int shuffleId,
      int numMappers,
      int numPartitions,
      int numOutputs,
      int mapId,
      int attemptId,
      Serializer<K> kSer,
      Serializer<V> vSer,
      int maxIOBufferSize,
      int spillIOBufferSize,
      ShuffleClient shuffleClient,
      CelebornConf celebornConf) {
    this.shuffleId = shuffleId;
    this.numMappers = numMappers;
    this.numOutputs = numOutputs;
    this.numPartitions = numPartitions;
    this.mapId = mapId;
    this.attempt = attemptId;
    this.kSer = kSer;
    this.vSer = vSer;
    this.maxIOBufferSize = maxIOBufferSize;
    this.spillIOBufferSize = spillIOBufferSize;
    this.shuffleClient = shuffleClient;
    partitionedKVs = new HashMap<>();
    serializedKV = new byte[maxIOBufferSize];
    maxPushDataSize = (int) celebornConf.clientMrMaxPushData();
    logger.info(
        "Sort based push initialized with"
            + " numMappers:{} numReducers:{} mapId:{} attemptId:{}"
            + " maxIOBufferSize:{} spillIOBufferSize:{}",
        numMappers,
        numPartitions,
        mapId,
        attemptId,
        maxIOBufferSize,
        spillIOBufferSize);
    recordsPerPartition = new HashMap<>();
    bytesPerPartition = new HashMap<>();
    try {
      kSer.open(this);
      vSer.open(this);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  public void insert(K key, V value, int partition) {
    try {
      if (writePos >= spillIOBufferSize) {
        // needs to sort and flush data
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Data is large enough {}/{}/{}, trigger sort and flush",
              Utils.bytesToString(writePos),
              Utils.bytesToString(spillIOBufferSize),
              Utils.bytesToString(maxIOBufferSize));
        }
        sendKVAndUpdateWritePos();
      }
      int dataLen = insertRecordInternal(key, value, partition);
      if (numOutputs == 1) {
        recordsPerPartition.putIfAbsent(0, new AtomicInteger());
        bytesPerPartition.putIfAbsent(0, new AtomicLong());
        recordsPerPartition.get(0).incrementAndGet();
        bytesPerPartition.get(0).incrementAndGet();
      } else {
        recordsPerPartition.computeIfAbsent(partition, p -> new AtomicInteger());
        bytesPerPartition.computeIfAbsent(partition, p -> new AtomicLong());
        recordsPerPartition.get(partition).incrementAndGet();
        bytesPerPartition.get(partition).incrementAndGet();
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Sort based pusher insert into partition:{} with {} bytes", partition, dataLen);
      }
      mapOutputRecordCounter.add(1);
      mapOutputByteCounter.add(dataLen);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  private void sendKVAndUpdateWritePos() throws IOException {
    Iterator<Map.Entry<Integer, List<SerializedKV>>> entryIter =
        partitionedKVs.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Integer, List<SerializedKV>> entry = entryIter.next();
      entryIter.remove();
      int partition = entry.getKey();
      List<SerializedKV> kvs = entry.getValue();
      List<SerializedKV> localKVs = new ArrayList<>();
      int partitionKVTotalLen = 0;
      // process buffers for specific partition
      for (SerializedKV kv : kvs) {
        partitionKVTotalLen += kv.kLen + kv.vLen;
        localKVs.add(kv);
        if (partitionKVTotalLen > maxPushDataSize) {
          // limit max size of pushdata to avoid possible memory issue in Celeborn worker
          // data layout
          // pushdata header (16) + pushDataLen(4) +
          // [varKeyLen+varValLen+serializedRecord(x)][...]
          sendSortedBuffersPartition(partition, localKVs, partitionKVTotalLen);
          localKVs.clear();
          partitionKVTotalLen = 0;
        }
      }
      if (!localKVs.isEmpty()) {
        sendSortedBuffersPartition(partition, localKVs, partitionKVTotalLen);
      }
      kvs.clear();
    }
    // all data sent
    partitionedKVs.clear();
    writePos = 0;
  }

  private void sendSortedBuffersPartition(
      int partition, List<SerializedKV> localKVs, int partitionKVTotalLen) throws IOException {
    int extraSize = 0;
    for (SerializedKV localKV : localKVs) {
      extraSize += WritableUtils.getVIntSize(localKV.kLen);
      extraSize += WritableUtils.getVIntSize(localKV.vLen);
    }
    // copied from hadoop logic
    extraSize += WritableUtils.getVIntSize(-1);
    extraSize += WritableUtils.getVIntSize(-1);
    // whole buffer's size + [(keyLen+valueLen)+(serializedKey+serializedValue)]
    byte[] pkvs = new byte[4 + extraSize + partitionKVTotalLen];
    int pkvsPos = 4;
    Platform.putInt(pkvs, Platform.BYTE_ARRAY_OFFSET, partitionKVTotalLen + extraSize);
    for (SerializedKV kv : localKVs) {
      int recordLen = kv.kLen + kv.vLen;
      // write key len
      pkvsPos = writeVLong(pkvs, pkvsPos, kv.kLen);
      // write value len
      pkvsPos = writeVLong(pkvs, pkvsPos, kv.vLen);
      // write serialized record
      System.arraycopy(serializedKV, kv.offset, pkvs, pkvsPos, recordLen);
      pkvsPos += recordLen;
    }
    // finally write -1 two times
    pkvsPos = writeVLong(pkvs, pkvsPos, -1);
    writeVLong(pkvs, pkvsPos, -1);
    int compressedSize =
        shuffleClient.pushData(
            shuffleId,
            mapId,
            attempt,
            partition,
            pkvs,
            0,
            4 + extraSize + partitionKVTotalLen,
            numMappers,
            numPartitions);
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

  private int insertRecordInternal(K key, V value, int partition) throws IOException {
    int offset = writePos;
    int keyLen;
    int valLen;
    kSer.serialize(key);
    keyLen = writePos - offset;
    vSer.serialize(value);
    valLen = writePos - keyLen - offset;
    List<SerializedKV> serializedKVs =
        partitionedKVs.computeIfAbsent(partition, v -> new ArrayList<>());
    serializedKVs.add(new SerializedKV(offset, keyLen, valLen));
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Pusher insert into buffer partition:{} offset:{} keyLen:{} valueLen:{} size:{}",
          partition,
          offset,
          keyLen,
          valLen,
          partitionedKVs.size());
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
      shuffleClient.mapperEnd(shuffleId, mapId, attempt, numMappers);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
    partitionedKVs.clear();
    serializedKV = null;
  }

  public LongAdder getMapOutputByteCounter() {
    return mapOutputByteCounter;
  }

  public LongAdder getMapOutputRecordCounter() {
    return mapOutputRecordCounter;
  }

  public int[] getRecordsPerPartition() {
    int[] values = new int[numOutputs];
    for (int i = 0; i < numOutputs; i++) {
      AtomicInteger records = recordsPerPartition.get(i);
      if (records != null) {
        values[i] = recordsPerPartition.get(i).get();
      } else {
        values[i] = 0;
      }
    }
    return values;
  }

  public long[] getBytesPerPartition() {
    long[] values = new long[numOutputs];
    for (int i = 0; i < numOutputs; i++) {
      AtomicLong bytes = bytesPerPartition.get(i);
      if (bytes != null) {
        values[i] = bytes.get();
      } else {
        values[i] = 0;
      }
    }
    return values;
  }

  static class SerializedKV {
    final int offset;
    final int kLen;
    final int vLen;

    public SerializedKV(int offset, int kLen, int vLen) {
      this.offset = offset;
      this.kLen = kLen;
      this.vLen = vLen;
    }
  }
}
