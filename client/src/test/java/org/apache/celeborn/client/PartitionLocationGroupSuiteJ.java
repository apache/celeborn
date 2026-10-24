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

package org.apache.celeborn.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

public class PartitionLocationGroupSuiteJ {

  private PartitionLocation loc(int epoch, String host) {
    return new PartitionLocation(
        0, epoch, host, 1234, 1235, 1236, 1237, PartitionLocation.Mode.PRIMARY);
  }

  @Test
  public void testFastPathSingleLocation() {
    PartitionLocation loc = loc(0, "w1");
    PartitionLocationGroup group = new PartitionLocationGroup(loc);
    assertEquals(1, group.epochs.size());
    assertSame(loc, group.currentFor(0));
    assertSame(loc, group.currentFor(7));
    assertSame(loc, group.latest());
    assertEquals(0, group.maxEpoch());
    assertTrue(group.currentFor(0) != null);

    // Baseline single-location update replaces the previous location.
    PartitionLocation loc1 = loc(1, "w2");
    group.replace(loc1);
    assertEquals(1, group.epochs.size());
    assertSame(loc1, group.currentFor(0));
    assertEquals(1, group.maxEpoch());
  }

  @Test
  public void testSoftSplitStaysWritableHardSplitExcluded() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    assertFalse(group.retire(0, StatusCode.SOFT_SPLIT));
    List<PartitionLocationGroup.EpochState> retires = group.outstandingRetires();
    assertEquals(1, retires.size());
    assertEquals(0, retires.get(0).location.getEpoch());
    assertEquals(StatusCode.SOFT_SPLIT, retires.get(0).cause);
    // Soft-split location stays writable and keeps its routing share.
    assertEquals(0, group.currentFor(0).getEpoch());
    assertTrue(group.currentFor(0) != null);

    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")));
    assertEquals(2, group.maxEpoch());
    assertEquals(3, group.epochs.size());
    assertEquals(0, group.currentFor(0).getEpoch());
    assertEquals(1, group.currentFor(1).getEpoch());
    assertEquals(2, group.currentFor(2).getEpoch());
    assertEquals(0, group.currentFor(3).getEpoch());
    assertEquals(2, group.latest().getEpoch());

    group.retire(1, StatusCode.HARD_SPLIT);
    // Epoch 1 is hard-retired: the writable set shrinks to {0, 2}.
    assertEquals(0, group.currentFor(0).getEpoch());
    assertEquals(2, group.currentFor(1).getEpoch());
    assertEquals(0, group.currentFor(2).getEpoch());
    assertTrue(group.currentFor(0) != null);
  }

  @Test
  public void testMergeConvergesOutOfOrderEpochs() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(5, "w1"));
    group.merge(Arrays.asList(loc(3, "w3"), loc(7, "w7"), loc(1, "w1")));
    assertEquals(4, group.epochs.size());
    assertEquals(7, group.maxEpoch());
    assertEquals(7, group.latest().getEpoch());

    // Locally retired epochs are never re-activated by the full set.
    group.retire(3, StatusCode.HARD_SPLIT);
    group.merge(Arrays.asList(loc(3, "w3"), loc(8, "w8")));
    assertEquals(8, group.maxEpoch());
    for (int mapId = 0; mapId < 16; mapId++) {
      assertNotEquals(3, group.currentFor(mapId).getEpoch());
    }

    // Epoch 3 is evicted once the LM stops reporting it in the full set.
    group.merge(Arrays.asList(loc(7, "w7"), loc(8, "w8")));
    assertEquals(4, group.epochs.size());
  }

  @Test
  public void testAllUnusable() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    group.retire(0, StatusCode.HARD_SPLIT);
    assertTrue(group.hasWritableFor(0));
    group.retire(1, StatusCode.HARD_SPLIT);
    // Nothing writable: currentFor falls back to the latest (possibly retired) location.
    assertFalse(group.hasWritableFor(0));
    assertEquals(1, group.currentFor(0).getEpoch());
  }

  @Test
  public void testRetireCauseUpgrade() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    // Soft-retired location stays writable and keeps its routing share.
    assertEquals(0, group.currentFor(0).getEpoch());

    // A later hard cause upgrades the soft retire (not a first retire).
    assertFalse(group.retire(0, StatusCode.HARD_SPLIT));
    for (int mapId = 0; mapId < 8; mapId++) {
      assertEquals(1, group.currentFor(mapId).getEpoch());
    }

    // A harder cause is never downgraded back to soft.
    assertTrue(group.retire(1, StatusCode.HARD_SPLIT));
    assertFalse(group.retire(1, StatusCode.SOFT_SPLIT));
    // Nothing writable: currentFor falls back to the latest (possibly retired) location.
    assertFalse(group.hasWritableFor(0));
    assertEquals(1, group.currentFor(0).getEpoch());
  }

  @Test
  public void testFullSetMergeEvictsProcessedRetiredEpochs() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    group.retire(0, StatusCode.HARD_SPLIT);

    // The LM still reports epoch 0 (retirement not yet processed): it is kept locally.
    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2")));
    assertEquals(2, group.epochs.size());

    group.merge(Arrays.asList(loc(1, "w2"), loc(2, "w3")));
    assertEquals(2, group.epochs.size());
    assertEquals(1, group.currentFor(0).getEpoch());
    assertEquals(2, group.currentFor(1).getEpoch());

    // A merge evicts retired epochs the LM no longer reports, even to a singleton set.
    group.retire(1, StatusCode.HARD_SPLIT);
    group.merge(Collections.singletonList(loc(3, "w4")));
    assertEquals(2, group.epochs.size());
    group.merge(Arrays.asList(loc(2, "w3"), loc(3, "w4")));
    assertEquals(2, group.epochs.size());
    assertEquals(2, group.currentFor(0).getEpoch());
    assertEquals(3, group.currentFor(1).getEpoch());
  }

  @Test
  public void testConcurrentPickDuringFullSetEviction() throws Exception {
    // Regression: pick()/latest() must not throw ArrayIndexOutOfBoundsException when a
    // concurrent full-set merge shrinks the active list via removeIf.
    int numEpochs = 16;
    List<PartitionLocation> all = new ArrayList<>();
    for (int e = 0; e < numEpochs; e++) {
      all.add(loc(e, "w" + e));
    }
    PartitionLocationGroup group = new PartitionLocationGroup(all.get(0));
    group.merge(all);
    for (int e = 0; e < numEpochs; e += 2) {
      group.retire(e, StatusCode.SOFT_SPLIT);
    }

    AtomicBoolean stop = new AtomicBoolean(false);
    CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
    CountDownLatch startLatch = new CountDownLatch(1);

    Thread writer =
        new Thread(
            () -> {
              try {
                startLatch.await();
                while (!stop.get()) {
                  // Report only odd epochs: even (retired) epochs get evicted from active.
                  List<PartitionLocation> odd = new ArrayList<>();
                  for (int e = 1; e < numEpochs; e += 2) {
                    odd.add(loc(e, "w" + e));
                  }
                  group.merge(odd);
                  group.merge(all);
                }
              } catch (Throwable t) {
                failures.add(t);
              }
            });

    List<Thread> readers = new ArrayList<>();
    for (int r = 0; r < 4; r++) {
      int mapIdBase = r * 1000;
      Thread reader =
          new Thread(
              () -> {
                try {
                  startLatch.await();
                  int i = 0;
                  while (!stop.get()) {
                    group.currentFor(mapIdBase + i);
                    group.latest();
                    i++;
                  }
                } catch (Throwable t) {
                  failures.add(t);
                }
              });
      readers.add(reader);
    }

    writer.start();
    for (Thread reader : readers) {
      reader.start();
    }
    startLatch.countDown();
    Thread.sleep(2000);
    stop.set(true);
    writer.join(TimeUnit.SECONDS.toMillis(10));
    for (Thread reader : readers) {
      reader.join(TimeUnit.SECONDS.toMillis(10));
    }

    assertTrue("Concurrent pick/merge threw: " + failures, failures.isEmpty());
  }

  @Test
  public void testOutstandingRetires() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    assertTrue(group.outstandingRetires().isEmpty());

    group.merge(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")));
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    assertFalse(group.retire(0, StatusCode.HARD_SPLIT)); // soft upgraded to hard
    assertTrue(group.retire(2, StatusCode.HARD_SPLIT));
    List<PartitionLocationGroup.EpochState> retires = group.outstandingRetires();
    assertEquals(2, retires.size());
    assertEquals(0, retires.get(0).location.getEpoch());
    assertEquals(StatusCode.HARD_SPLIT, retires.get(0).cause);
    assertEquals(2, retires.get(1).location.getEpoch());
    assertEquals(StatusCode.HARD_SPLIT, retires.get(1).cause);
    assertEquals(1, group.currentFor(0).getEpoch());

    // Once the LM digests the retires, they are evicted and no longer outstanding.
    group.merge(Collections.singletonList(loc(1, "w2")));
    assertTrue(group.outstandingRetires().isEmpty());
    assertEquals(1, group.epochs.size());
  }
}
