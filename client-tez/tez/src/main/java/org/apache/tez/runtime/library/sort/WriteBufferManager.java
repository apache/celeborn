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

package org.apache.tez.runtime.library.sort;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.counters.TezCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.exception.CelebornRuntimeException;

public class WriteBufferManager<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);
  private final long maxMemSize;
  private final CelebornTezWriter celebornTezWriter;
  private final ReentrantLock memoryLock = new ReentrantLock();
  private final AtomicLong memoryUsedSize = new AtomicLong(0);
  private final AtomicLong inSendListBytes = new AtomicLong(0);
  private final Condition full = memoryLock.newCondition();
  private final RawComparator<K> comparator;
  private final long maxSegmentSize;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valSerializer;
  private final List<WriteBuffer<K, V>> waitSendBuffers = Lists.newLinkedList();
  private final Map<Integer, WriteBuffer<K, V>> buffers = Maps.newConcurrentMap();
  private final long maxBufferSize;
  private final double memoryThreshold;
  private final double sendThreshold;
  private final int batch;
  private final boolean isNeedSorted;
  private final TezCounter mapOutputByteCounter;
  private final TezCounter mapOutputRecordCounter;

  /** WriteBufferManager */
  public WriteBufferManager(
      long maxMemSize,
      CelebornTezWriter celebornTezWriter,
      RawComparator<K> comparator,
      long maxSegmentSize,
      Serializer<K> keySerializer,
      Serializer<V> valSerializer,
      long maxBufferSize,
      double memoryThreshold,
      double sendThreshold,
      int batch,
      boolean isNeedSorted,
      TezCounter mapOutputByteCounter,
      TezCounter mapOutputRecordCounter) {
    this.maxMemSize = maxMemSize;
    this.celebornTezWriter = celebornTezWriter;
    this.comparator = comparator;
    this.maxSegmentSize = maxSegmentSize;
    this.keySerializer = keySerializer;
    this.valSerializer = valSerializer;
    this.maxBufferSize = maxBufferSize;
    this.memoryThreshold = memoryThreshold;
    this.sendThreshold = sendThreshold;
    this.batch = batch;
    this.isNeedSorted = isNeedSorted;
    this.mapOutputByteCounter = mapOutputByteCounter;
    this.mapOutputRecordCounter = mapOutputRecordCounter;
  }

  /** add record */
  public void addRecord(int partitionId, K key, V value) throws InterruptedException, IOException {
    memoryLock.lock();
    try {
      while (memoryUsedSize.get() > maxMemSize) {
        LOG.warn(
            "memoryUsedSize {} is more than {}, inSendListBytes {}",
            memoryUsedSize,
            maxMemSize,
            inSendListBytes);
        full.await();
      }
    } finally {
      memoryLock.unlock();
    }

    if (!buffers.containsKey(partitionId)) {
      WriteBuffer<K, V> sortWriterBuffer =
          new WriteBuffer(
              isNeedSorted, partitionId, comparator, maxSegmentSize, keySerializer, valSerializer);
      buffers.putIfAbsent(partitionId, sortWriterBuffer);
      waitSendBuffers.add(sortWriterBuffer);
    }
    WriteBuffer<K, V> buffer = buffers.get(partitionId);
    int length = buffer.addRecord(key, value);
    if (length > maxMemSize) {
      throw new CelebornRuntimeException("record is too big");
    }

    memoryUsedSize.addAndGet(length);
    if (buffer.getDataLength() > maxBufferSize) {
      if (waitSendBuffers.remove(buffer)) {
        sendBufferToServers(buffer);
      } else {
        LOG.error("waitSendBuffers don't contain buffer {}", buffer);
      }
    }
    if (memoryUsedSize.get() > maxMemSize * memoryThreshold
        && inSendListBytes.get() <= maxMemSize * sendThreshold) {
      sendBuffersToServers();
    }
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(length);
  }

  private void sendBufferToServers(WriteBuffer<K, V> buffer) throws IOException {
    prepareBufferForSend(buffer);
  }

  void sendBuffersToServers() throws IOException {
    int sendSize = batch;
    if (batch > waitSendBuffers.size()) {
      sendSize = waitSendBuffers.size();
    }

    Iterator<WriteBuffer<K, V>> iterator = waitSendBuffers.iterator();
    int index = 0;
    // todo merge data
    while (iterator.hasNext() && index < sendSize) {
      WriteBuffer<K, V> buffer = iterator.next();
      prepareBufferForSend(buffer);
      iterator.remove();
      index++;
    }
  }

  private void prepareBufferForSend(WriteBuffer buffer) throws IOException {
    buffers.remove(buffer.getPartitionId());
    celebornTezWriter.pushData(buffer.getPartitionId(), buffer.getData());
    memoryLock.lock();
    try {
      memoryUsedSize.addAndGet(buffer.getDataLength());
      full.signalAll();
    } finally {
      memoryLock.unlock();
    }
    buffer.clear();
  }

  /** wait send finished */
  public void waitSendFinished() throws IOException {
    while (!waitSendBuffers.isEmpty()) {
      sendBuffersToServers();
    }
    sendCommit();
  }

  protected void sendCommit() throws IOException {
    celebornTezWriter.close();
  }
}
