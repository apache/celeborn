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

public class CelebornSortBasedPusher<K, V> extends OutputStream {
  Logger logger = LoggerFactory.getLogger(CelebornSortBasedPusher.class);
  private int mapId;
  private int attempt;
  private int numMappers;
  private int numReducers;
  private ShuffleClient shuffleClient;
  private int maxIOBufferSize;
  private int spillIOBufferSize;
  private Serializer<K> kSer;
  private Serializer<V> vSer;
  private RawComparator<K> comparator;
  private AtomicReference<Exception> exception = new AtomicReference<>();
  private Counters.Counter mapOutputByteCounter;
  private Counters.Counter mapOutputRecordCounter;
  private Map<Integer, List<SerializedKV>> currentSerializedKVs;
  private int writePos;
  private byte[] serializedKV;
  private int maxPushDataSize;

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
    currentSerializedKVs = new HashMap<>();
    serializedKV = new byte[maxIOBufferSize];
    maxPushDataSize = (int) celebornConf.clientMrMaxPushData();
    logger.info(
        "Sort based push initialized with params:"
            + " numMappers {}, numReducers {}, mapId {}, attemptId {},"
            + " maxIOBufferSize {}, spillIOBufferSize {} ",
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
      if (writePos >= spillIOBufferSize) {
        // needs to sort and flush data
        logger.info(
            "Data is large enough {}/{}/{}, trigger sort and flush",
            Utils.bytesToString(writePos),
            Utils.bytesToString(spillIOBufferSize),
            Utils.bytesToString(maxIOBufferSize));
        synchronized (this) {
          sortKVs();
          sendKVAndUpdateWritePos();
        }
      }
      int dataLen = insertRecordInternal(key, value, partition);
      if (logger.isDebugEnabled()) {
        logger.debug("Sort based pusher insert into {} with {} bytes", partition, dataLen);
      }
      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(dataLen);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  private void sendKVAndUpdateWritePos() throws IOException {
    for (Map.Entry<Integer, List<SerializedKV>> partitionKVEntry :
        currentSerializedKVs.entrySet()) {
      List<SerializedKV> kvs = partitionKVEntry.getValue();
      synchronized (kvs) {
        int partition = partitionKVEntry.getKey();
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
            // [keyLen(4)+valLen(4)+serializedRecord(x)][...]
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
    }
    // all data sent
    currentSerializedKVs.clear();
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
      pkvsPos = writeDataInt(pkvs, pkvsPos, kv.kLen);
      // write value len
      pkvsPos = writeDataInt(pkvs, pkvsPos, kv.vLen);
      // write serialized record
      System.arraycopy(serializedKV, kv.offset, pkvs, pkvsPos, recordLen);
      pkvsPos += recordLen;
    }
    // finally write -1 two times
    pkvsPos = writeDataInt(pkvs, pkvsPos, -1);
    writeDataInt(pkvs, pkvsPos, -1);
    int compressedSize =
        shuffleClient.pushData(
            0,
            mapId,
            attempt,
            partition,
            pkvs,
            0,
            4 + extraSize + partitionKVTotalLen,
            numMappers,
            numReducers);
    logger.info(
        "Send sorted buffer on {}-{} to partition {} with {} compressed {}",
        mapId,
        attempt,
        partition,
        Utils.bytesToString(4 + extraSize + partitionKVTotalLen),
        Utils.bytesToString(compressedSize));
  }

  private int writeDataInt(byte[] data, int offset, long dataInt) {
    if (dataInt >= -112L && dataInt <= 127L) {
      data[offset] = (byte) ((int) dataInt);
      offset++;
    } else {
      int len = -112;
      if (dataInt < 0L) {
        dataInt = ~dataInt;
        len = -120;
      }

      for (long tmp = dataInt; tmp != 0L; --len) {
        tmp >>= 8;
      }

      data[offset] = (byte) len;
      offset++;
      len = len < -120 ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; --idx) {
        int shiftBits = (idx - 1) * 8;
        long mask = 255L << shiftBits;
        data[offset] = ((byte) ((int) ((dataInt & mask) >> shiftBits)));
        offset++;
      }
    }
    return offset;
  }

  private void sortKVs() {
    for (Map.Entry<Integer, List<SerializedKV>> partitionKVEntry :
        currentSerializedKVs.entrySet()) {
      partitionKVEntry
          .getValue()
          .sort(
              (o1, o2) ->
                  comparator.compare(
                      serializedKV, o1.offset, o1.kLen, serializedKV, o2.offset, o2.kLen));
    }
  }

  private int insertRecordInternal(K key, V value, int partition) throws IOException {
    int offset = writePos;
    int keyLen = writePos;
    int valLen = 0;
    kSer.serialize(key);
    keyLen = writePos - offset;
    vSer.serialize(value);
    valLen = writePos - keyLen - offset;
    List<SerializedKV> serializedKVs =
        currentSerializedKVs.computeIfAbsent(partition, v -> new ArrayList<>());
    synchronized (serializedKVs) {
      serializedKVs.add(new SerializedKV(offset, keyLen, valLen));
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Pusher insert into buffer {} {} {} {} {}",
          partition,
          offset,
          keyLen,
          valLen,
          currentSerializedKVs.size());
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
      logger.warn("Sort push memory high ,write pos {} max size {}", writePos, maxIOBufferSize);
      throw new IOException("Sort pusher memory exhausted.");
    }
  }

  public void flush() {
    logger.info("Sort based pusher called flush");
    try {
      synchronized (this) {
        sortKVs();
        sendKVAndUpdateWritePos();
      }
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  public void close() {
    flush();
    try {
      logger.info("Call mapper end {} {} {} {}", 0, mapId, attempt, numMappers);
      // make sure that all data has been pushed
      shuffleClient.prepareForMergeData(0, mapId, attempt);
      shuffleClient.mapperEnd(0, mapId, attempt, numMappers);
    } catch (IOException e) {
      logger.error("Mapper end failed, data lost", e);
    }
    currentSerializedKVs.clear();
    serializedKV = null;
  }

  static class SerializedKV {
    int offset;
    int kLen;
    int vLen;

    public SerializedKV(int offset, int kLen, int vLen) {
      this.offset = offset;
      this.kLen = kLen;
      this.vLen = vLen;
    }
  }
}
