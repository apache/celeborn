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

package org.apache.celeborn.client.read;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Per input stream read stats, logged when the stream is closed, used to diagnose slow shuffle
 * fetch.
 */
public class ReadStreamStats {
  // Time the reducer thread is blocked waiting for fetched chunks.
  private final LongAdder chunkWaitTimeNanos = new LongAdder();
  // Number of chunks whose fetch round trip time exceeds the slow chunk threshold.
  private final LongAdder slowChunkCount = new LongAdder();
  private final AtomicLong maxChunkRttNanos = new AtomicLong(0);
  private final LongAdder decompressTimeNanos = new LongAdder();
  private final LongAdder retryCount = new LongAdder();
  private final LongAdder retryWaitTimeMs = new LongAdder();
  private final LongAdder peerSwitchCount = new LongAdder();
  private final LongAdder excludeCount = new LongAdder();

  public void addChunkWaitTime(long nanos) {
    chunkWaitTimeNanos.add(nanos);
  }

  public void recordChunkRtt(long rttNanos, long slowThresholdNanos) {
    maxChunkRttNanos.accumulateAndGet(rttNanos, Math::max);
    if (rttNanos > slowThresholdNanos) {
      slowChunkCount.increment();
    }
  }

  public void addDecompressTime(long nanos) {
    decompressTimeNanos.add(nanos);
  }

  public void incRetryCount() {
    retryCount.increment();
  }

  public void addRetryWaitTime(long ms) {
    retryWaitTimeMs.add(ms);
  }

  public void incPeerSwitchCount() {
    peerSwitchCount.increment();
  }

  public void incExcludeCount() {
    excludeCount.increment();
  }

  public long getChunkWaitTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(chunkWaitTimeNanos.sum());
  }

  public long getSlowChunkCount() {
    return slowChunkCount.sum();
  }

  public long getMaxChunkRttMs() {
    return TimeUnit.NANOSECONDS.toMillis(maxChunkRttNanos.get());
  }

  public long getDecompressTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(decompressTimeNanos.sum());
  }

  public long getRetryCount() {
    return retryCount.sum();
  }

  public long getRetryWaitTimeMs() {
    return retryWaitTimeMs.sum();
  }

  public long getPeerSwitchCount() {
    return peerSwitchCount.sum();
  }

  public long getExcludeCount() {
    return excludeCount.sum();
  }
}
