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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.unsafe.Platform;

public class SortBasedPusher<K, V> extends OutputStream {
  Logger logger = LoggerFactory.getLogger(SortBasedPusher.class);
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
  private AtomicReference<Exception> exception;
  private Counters.Counter mapOutputByteCounter;
  private Counters.Counter mapOutputRecordCounter;
  private Map<Integer, List<SerializedKV>> currentSerializedKVs;
  private int writePos;
  private byte[] serializedKV;

  public SortBasedPusher(
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

    try {
      kSer.open(this);
      vSer.open(this);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  public void insert(K key, V value, int partition) {
    int dataLen = 0;
    try {
      if (writePos >= spillIOBufferSize) {
        // needs to sort and flush data
        synchronized (this) {
          sortKVs();
          sendKVAndUpdateWritePos();
        }
      }
      dataLen = insertRecordInternal(key, value, partition);
      mapOutputRecordCounter.increment(1);
      mapOutputByteCounter.increment(dataLen);
    } catch (IOException e) {
      exception.compareAndSet(null, e);
    }
  }

  private void sendKVAndUpdateWritePos() throws IOException {
    for (Map.Entry<Integer, List<SerializedKV>> partitionKVEntry :
        currentSerializedKVs.entrySet()) {
      int partition = partitionKVEntry.getKey();
      List<SerializedKV> kvs = partitionKVEntry.getValue();
      int partitionKVTotalLen = 0;
      for (SerializedKV kv : kvs) {
        partitionKVTotalLen += kv.kLen + kv.vLen;
      }
      // I think it's ok to use extra memory here because sendKV
      // was triggered at spill threshold ant it will be sent immediately
      // extra 4 byte is length
      byte[] pkvs = new byte[partitionKVTotalLen + 4];
      int pkvsPos = 4;
      Platform.putInt(serializedKV, Platform.BYTE_ARRAY_OFFSET, partitionKVTotalLen);
      for (SerializedKV kv : kvs) {
        int recordLen = kv.kLen + kv.vLen;
        System.arraycopy(serializedKV, kv.offset, pkvs, pkvsPos, recordLen);
        pkvsPos += recordLen;
      }
      shuffleClient.pushData(
          0, mapId, attempt, partition, pkvs, 0, partitionKVTotalLen, numMappers, numReducers);
    }
    // all data sent
    currentSerializedKVs.clear();
    writePos = 0;
  }

  private void sortKVs() {
    for (Map.Entry<Integer, List<SerializedKV>> partitionKVEntry :
        currentSerializedKVs.entrySet()) {
      List<SerializedKV> kvs = partitionKVEntry.getValue();
      kvs.sort(
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
    keyLen = writePos;
    vSer.serialize(value);
    valLen = writePos - keyLen;
    List<SerializedKV> serializedKVs =
        currentSerializedKVs.computeIfAbsent(partition, v -> new ArrayList<>());
    serializedKVs.add(new SerializedKV(offset, keyLen, valLen));
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
      throw new IOException("Sort pusher memory exhausted.");
    }
  }

  public void flush() {
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
