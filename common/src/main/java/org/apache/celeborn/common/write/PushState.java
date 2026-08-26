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

package org.apache.celeborn.common.write;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.CommitMetadata;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.JavaUtils;

public class PushState {

  private final int pushBufferMaxSize;
  public AtomicReference<IOException> exception = new AtomicReference<>();
  private final InFlightRequestTracker inFlightRequestTracker;
  // partition id -> CommitMetadata
  private final ConcurrentHashMap<Integer, CommitMetadata> commitMetadataMap =
      JavaUtils.newConcurrentHashMap();

  private final Map<String, LocationPushFailedBatches> failedBatchMap;

  // Per map task write path stats, used to diagnose slow shuffle write.
  // Time a mapper thread is blocked by the push queue backpressure (DataPusher.addTask).
  private final LongAdder queueWaitTimeNanos = new LongAdder();
  // Time push threads are blocked by the in-flight limit (limitMaxInFlight).
  private final LongAdder inflightWaitTimeNanos = new LongAdder();
  // Time mapperEnd waits for all in-flight batches to be done (limitZeroInFlight).
  private final LongAdder drainWaitTimeNanos = new LongAdder();
  // Number of push batches whose round trip time exceeds the slow push threshold.
  private final LongAdder slowPushCount = new LongAdder();
  private final AtomicLong maxPushRttNanos = new AtomicLong(0);

  public PushState(CelebornConf conf) {
    pushBufferMaxSize = conf.clientPushBufferMaxSize();
    inFlightRequestTracker = new InFlightRequestTracker(conf, this);
    failedBatchMap = JavaUtils.newConcurrentHashMap();
  }

  public void addQueueWaitTime(long nanos) {
    queueWaitTimeNanos.add(nanos);
  }

  public void addInflightWaitTime(long nanos) {
    inflightWaitTimeNanos.add(nanos);
  }

  public void addDrainWaitTime(long nanos) {
    drainWaitTimeNanos.add(nanos);
  }

  public void recordPushRtt(long rttNanos, long slowThresholdNanos) {
    maxPushRttNanos.accumulateAndGet(rttNanos, Math::max);
    if (rttNanos > slowThresholdNanos) {
      slowPushCount.increment();
    }
  }

  public long getQueueWaitTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(queueWaitTimeNanos.sum());
  }

  public long getInflightWaitTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(inflightWaitTimeNanos.sum());
  }

  public long getDrainWaitTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(drainWaitTimeNanos.sum());
  }

  public long getSlowPushCount() {
    return slowPushCount.sum();
  }

  public long getMaxPushRttMs() {
    return TimeUnit.NANOSECONDS.toMillis(maxPushRttNanos.get());
  }

  public void cleanup() {
    inFlightRequestTracker.cleanup();
  }

  // key: ${primary addr}, ${replica addr} value: list of data batch
  public final ConcurrentHashMap<Pair<String, String>, DataBatches> batchesMap =
      JavaUtils.newConcurrentHashMap();

  public boolean addBatchData(
      Pair<String, String> addressPair, PartitionLocation loc, int batchId, byte[] body) {
    DataBatches batches = batchesMap.computeIfAbsent(addressPair, (s) -> new DataBatches());
    batches.addDataBatch(loc, batchId, body);
    return batches.getTotalSize() > pushBufferMaxSize;
  }

  public DataBatches takeDataBatches(Pair<String, String> addressPair) {
    return batchesMap.remove(addressPair);
  }

  public int nextBatchId() {
    return inFlightRequestTracker.nextBatchId();
  }

  public void addBatch(int batchId, int batchBytesSize, String hostAndPushPort) {
    inFlightRequestTracker.addBatch(batchId, batchBytesSize, hostAndPushPort);
  }

  public void removeBatch(int batchId, String hostAndPushPort) {
    inFlightRequestTracker.removeBatch(batchId, hostAndPushPort);
  }

  public void onSuccess(String hostAndPushPort) {
    inFlightRequestTracker.onSuccess(hostAndPushPort);
  }

  public void onCongestControl(String hostAndPushPort) {
    inFlightRequestTracker.onCongestControl(hostAndPushPort);
  }

  public boolean limitMaxInFlight(String hostAndPushPort) throws IOException {
    return inFlightRequestTracker.limitMaxInFlight(hostAndPushPort);
  }

  public boolean limitZeroInFlight() throws IOException {
    return inFlightRequestTracker.limitZeroInFlight();
  }

  public int remainingAllowPushes(String hostAndPushPort) {
    return inFlightRequestTracker.remainingAllowPushes(hostAndPushPort);
  }

  public void recordFailedBatch(String partitionId, int mapId, int attemptId, int batchId) {
    this.failedBatchMap
        .computeIfAbsent(partitionId, (s) -> new LocationPushFailedBatches())
        .addFailedBatch(mapId, attemptId, batchId);
  }

  public Map<String, LocationPushFailedBatches> getFailedBatches() {
    return this.failedBatchMap;
  }

  public int[] getCRC32PerPartition(boolean shuffleIntegrityCheckEnabled, int numPartitions) {
    if (!shuffleIntegrityCheckEnabled) {
      return new int[0];
    }

    int[] crc32PerPartition = new int[numPartitions];
    for (Map.Entry<Integer, CommitMetadata> entry : commitMetadataMap.entrySet()) {
      crc32PerPartition[entry.getKey()] = entry.getValue().getChecksum();
    }
    return crc32PerPartition;
  }

  public long[] getBytesWrittenPerPartition(
      boolean shuffleIntegrityCheckEnabled, int numPartitions) {
    if (!shuffleIntegrityCheckEnabled) {
      return new long[0];
    }
    long[] bytesWrittenPerPartition = new long[numPartitions];
    for (Map.Entry<Integer, CommitMetadata> entry : commitMetadataMap.entrySet()) {
      bytesWrittenPerPartition[entry.getKey()] = entry.getValue().getBytes();
    }
    return bytesWrittenPerPartition;
  }

  public void addDataWithOffsetAndLength(int partitionId, byte[] data, int offset, int length) {
    CommitMetadata commitMetadata =
        commitMetadataMap.computeIfAbsent(partitionId, id -> new CommitMetadata());
    commitMetadata.addDataWithOffsetAndLength(data, offset, length);
  }

  public void addData(int partitionId, ByteBuffer data) {
    CommitMetadata commitMetadata =
        commitMetadataMap.computeIfAbsent(partitionId, id -> new CommitMetadata());
    commitMetadata.addData(data);
  }
}
