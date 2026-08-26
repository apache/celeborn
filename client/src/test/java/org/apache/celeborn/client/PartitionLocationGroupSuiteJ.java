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
import static org.junit.Assert.assertNull;
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
    assertFalse(group.hasParallelState());
    assertSame(loc, group.currentFor(0));
    assertSame(loc, group.currentFor(7));
    assertSame(loc, group.latest());
    assertEquals(0, group.maxEpoch());
    assertTrue(group.hasUsable());
    assertSame(loc, group.anotherUsableFor(0, 3));
    assertNull(group.anotherUsableFor(0, 0));

    // Single-location revive update keeps thin-wrapper mode.
    PartitionLocation loc1 = loc(1, "w2");
    group.mergeActiveLocations(Collections.singletonList(loc1), true);
    assertFalse(group.hasParallelState());
    assertSame(loc1, group.currentFor(0));
    assertEquals(1, group.maxEpoch());
  }

  @Test
  public void testSoftSplitStaysWritableHardSplitExcluded() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    assertFalse(group.retire(0, StatusCode.SOFT_SPLIT));
    assertTrue(group.hasParallelState());
    // Soft-split location stays writable and keeps its routing share.
    assertEquals(0, group.currentFor(0).getEpoch());
    assertTrue(group.hasUsable());

    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")), true);
    assertEquals(2, group.maxEpoch());
    assertEquals(3, group.activeCount());
    // Writable set = {0 (soft), 1, 2}: mapId % 3 dispatches uniformly, soft epoch 0 included.
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
    assertTrue(group.hasUsable());
  }

  @Test
  public void testMergeConvergesOutOfOrderEpochs() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(5, "w1"));
    // Full active set delivered out of order, including epochs not known locally.
    group.mergeActiveLocations(Arrays.asList(loc(3, "w3"), loc(7, "w7"), loc(1, "w1")), true);
    assertEquals(4, group.activeCount());
    assertEquals(7, group.maxEpoch());
    assertEquals(7, group.latest().getEpoch());

    // Locally retired epochs are never re-activated by the full set.
    group.retire(3, StatusCode.HARD_SPLIT);
    group.mergeActiveLocations(Arrays.asList(loc(3, "w3"), loc(8, "w8")), true);
    assertEquals(8, group.maxEpoch());
    for (int mapId = 0; mapId < 16; mapId++) {
      assertNotEquals(3, group.currentFor(mapId).getEpoch());
    }

    // The full set no longer reports epoch 3 (the LM has processed the retirement), so it is
    // evicted; epochs already active are deduped.
    group.mergeActiveLocations(Arrays.asList(loc(7, "w7"), loc(8, "w8")), true);
    assertEquals(4, group.activeCount());
  }

  @Test
  public void testAllUnusable() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    assertTrue(group.hasUsable());
    group.retire(1, StatusCode.HARD_SPLIT);
    assertFalse(group.hasUsable());
    assertNull(group.currentFor(0));
    assertNull(group.anotherUsableFor(0, 1));
  }

  @Test
  public void testAnotherUsableFor() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    assertEquals(1, group.anotherUsableFor(0, 0).getEpoch());
    assertEquals(0, group.anotherUsableFor(0, 1).getEpoch());
    // No other usable location when the other one is hard-retired.
    group.retire(1, StatusCode.HARD_SPLIT);
    assertNull(group.anotherUsableFor(0, 0));
    // Soft-retired other location is still usable for re-push.
    PartitionLocationGroup group2 = new PartitionLocationGroup(loc(0, "w1"));
    group2.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group2.retire(1, StatusCode.SOFT_SPLIT);
    assertEquals(1, group2.anotherUsableFor(0, 0).getEpoch());
  }

  @Test
  public void testRetireCauseUpgrade() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    // Soft-retired location remains a usable fallback for re-push.
    assertEquals(0, group.anotherUsableFor(0, 1).getEpoch());

    // A later hard cause upgrades the soft retire (not a first retire).
    assertFalse(group.retire(0, StatusCode.HARD_SPLIT));
    // Epoch 0 is no longer usable at all.
    assertNull(group.anotherUsableFor(0, 1));
    for (int mapId = 0; mapId < 8; mapId++) {
      assertEquals(1, group.currentFor(mapId).getEpoch());
    }

    // A harder cause is never downgraded back to soft.
    assertTrue(group.retire(1, StatusCode.HARD_SPLIT));
    assertFalse(group.retire(1, StatusCode.SOFT_SPLIT));
    assertFalse(group.hasUsable());
  }

  @Test
  public void testFullSetMergeEvictsProcessedRetiredEpochs() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group.retire(0, StatusCode.HARD_SPLIT);

    // The LM still reports epoch 0 (retirement not yet processed): it is kept locally.
    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    assertEquals(2, group.activeCount());

    // The LM has processed the retirement and no longer reports epoch 0: it is evicted, so
    // mapId-based routing is uniform over the live locations again.
    group.mergeActiveLocations(Arrays.asList(loc(1, "w2"), loc(2, "w3")), true);
    assertEquals(2, group.activeCount());
    assertEquals(1, group.currentFor(0).getEpoch());
    assertEquals(2, group.currentFor(1).getEpoch());

    // A non-full-set update never evicts retired entries.
    group.retire(1, StatusCode.HARD_SPLIT);
    group.mergeActiveLocations(Collections.singletonList(loc(3, "w4")), false);
    assertEquals(3, group.activeCount());
    // The next full-set merge evicts epoch 1.
    group.mergeActiveLocations(Arrays.asList(loc(2, "w3"), loc(3, "w4")), true);
    assertEquals(2, group.activeCount());
    assertEquals(2, group.currentFor(0).getEpoch());
    assertEquals(3, group.currentFor(1).getEpoch());
  }

  @Test
  public void testConcurrentPickDuringFullSetEviction() throws Exception {
    // Regression test: pick()/latest() must not throw ArrayIndexOutOfBoundsException when a
    // concurrent full-set merge shrinks the active list via removeIf.
    int numEpochs = 16;
    List<PartitionLocation> all = new ArrayList<>();
    for (int e = 0; e < numEpochs; e++) {
      all.add(loc(e, "w" + e));
    }
    PartitionLocationGroup group = new PartitionLocationGroup(all.get(0));
    group.mergeActiveLocations(all, true);
    for (int e = 0; e < numEpochs; e += 2) {
      group.retire(e, StatusCode.SOFT_SPLIT);
    }

    AtomicBoolean stop = new AtomicBoolean(false);
    CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();
    CountDownLatch startLatch = new CountDownLatch(1);

    // Writer thread: alternate full-set merges that evict/re-add retired epochs, shrinking and
    // growing the active list concurrently with the readers.
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
                  group.mergeActiveLocations(odd, true);
                  group.mergeActiveLocations(all, true);
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
                    group.anotherUsableFor(mapIdBase + i, 0);
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
    // NULL-location regression: when every known epoch is locally retired, the outstanding-retire
    // view must cover exactly the epochs still held in the active list (LM not yet digested),
    // epoch ascending, with the upgraded cause — they are attached to the blocking revive so the
    // LM shrinks its stale active set and the gap allocation produces a fresh location.
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    assertTrue(group.outstandingRetires().isEmpty());

    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2"), loc(2, "w3")), true);
    assertTrue(group.retire(0, StatusCode.SOFT_SPLIT));
    assertFalse(group.retire(0, StatusCode.HARD_SPLIT)); // soft upgraded to hard
    assertTrue(group.retire(2, StatusCode.HARD_SPLIT));
    List<PartitionLocationGroup.OutstandingRetire> retires = group.outstandingRetires();
    assertEquals(2, retires.size());
    assertEquals(0, retires.get(0).location.getEpoch());
    assertEquals(StatusCode.HARD_SPLIT, retires.get(0).cause);
    assertEquals(2, retires.get(1).location.getEpoch());
    assertEquals(StatusCode.HARD_SPLIT, retires.get(1).cause);
    // Epoch 1 is the only writable location: every mapId routes to it.
    assertEquals(1, group.currentFor(0).getEpoch());

    // After the LM digests both retires (full set reports only epoch 1), they are evicted from
    // the active list and no longer outstanding.
    group.mergeActiveLocations(Collections.singletonList(loc(1, "w2")), true);
    assertTrue(group.outstandingRetires().isEmpty());
    assertEquals(1, group.activeCount());
  }

  @Test
  public void testEpochsSnapshots() {
    PartitionLocationGroup group = new PartitionLocationGroup(loc(0, "w1"));
    assertEquals(Collections.singletonList(0), group.activeEpochsSnapshot());
    assertTrue(group.retiredEpochsSnapshot().isEmpty());

    group.mergeActiveLocations(Arrays.asList(loc(0, "w1"), loc(1, "w2")), true);
    group.retire(0, StatusCode.HARD_SPLIT);
    group.retire(1, StatusCode.SOFT_SPLIT);
    assertEquals(Arrays.asList(0, 1), group.activeEpochsSnapshot());
    // Retired snapshot lists epoch=cause pairs regardless of active membership.
    List<String> retired = group.retiredEpochsSnapshot();
    assertEquals(2, retired.size());
    assertTrue(retired.get(0).startsWith("0="));
    assertTrue(retired.get(1).startsWith("1="));
  }
}
