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

import org.junit.Assert;
import org.junit.Test;

/** Verifies the read-side timing accumulators surfaced in the Celeborn UI read metrics. */
public class ReadStreamStatsSuiteJ {

  @Test
  public void testDeserializeAndCopyTimeAccumulate() {
    ReadStreamStats stats = new ReadStreamStats();
    // Deserialization happens per record via TimedDeserializationStream; copies happen per
    // CelebornInputStream.read call. Both accumulate in nanos and report in millis.
    stats.addDeserializeTime(TimeUnit.MILLISECONDS.toNanos(2));
    stats.addDeserializeTime(TimeUnit.MILLISECONDS.toNanos(3));
    stats.addCopyTime(TimeUnit.MILLISECONDS.toNanos(1));
    stats.addCopyTime(TimeUnit.MILLISECONDS.toNanos(4));
    Assert.assertEquals(5L, stats.getDeserializeTimeMs());
    Assert.assertEquals(5L, stats.getCopyTimeMs());
  }

  @Test
  public void testEmptyStatsReportZero() {
    ReadStreamStats stats = new ReadStreamStats();
    Assert.assertEquals(0L, stats.getDeserializeTimeMs());
    Assert.assertEquals(0L, stats.getCopyTimeMs());
    Assert.assertEquals(0L, stats.getChunkWaitTimeMs());
    Assert.assertEquals(0L, stats.getMaxChunkRttMs());
  }

  @Test
  public void testWorkerReadCostMerge() {
    ReadStreamStats stats = new ReadStreamStats();
    stats.recordWorkerChunkRead("host1:9097", 100, TimeUnit.MILLISECONDS.toNanos(5));
    stats.recordWorkerChunkRead("host1:9097", 200, TimeUnit.MILLISECONDS.toNanos(7));
    ReadStreamStats.WorkerReadCost cost = stats.getWorkerReadCosts().get("host1:9097");
    Assert.assertEquals(2L, cost.chunkCount.sum());
    Assert.assertEquals(300L, cost.bytes.sum());
    Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(12), cost.totalRttNanos.sum());
    Assert.assertEquals(TimeUnit.MILLISECONDS.toNanos(7), cost.maxRttNanos.get());
  }
}
