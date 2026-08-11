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

package org.apache.spark.shuffle.celeborn;

import java.util.*;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class CelebornPartitionUtilSuiteJ {
  @Test
  public void testSkewPartitionSplitIgnoreEmpty1() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    PartitionLocation loc = genPartitionLocation(0, new Long[] {0L, 20L, 30L});
    List<Long> offsets = loc.getStorageInfo().getChunkOffsets();
    locations.add(loc);
    int partitionNumber = 10;
    Long sum = 0L;
    for (int i = 0; i < partitionNumber; i++) {
      Map<String, Pair<Integer, Integer>> res =
          CelebornPartitionUtil.splitSkewedPartitionLocations(locations, partitionNumber, i);
      if (!res.isEmpty()) {
        int l = res.get(loc.getUniqueId()).getLeft();
        int r = res.get(loc.getUniqueId()).getRight();
        Assert.assertTrue(r >= l);
        for (int j = l; j <= r; j++) {
          sum += offsets.get(j + 1) - offsets.get(j);
        }
      }
    }
    Assert.assertEquals(offsets.get(offsets.size() - 1), sum);
  }

  @Test
  public void testSkewPartitionSplit() {

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      locations.add(genPartitionLocation(i, new Long[] {0L, 100L, 200L, 300L, 500L, 1000L}));
    }
    locations.add(genPartitionLocation(91, new Long[] {0L, 1L}));

    int subPartitionSize = 3;

    Map<String, Pair<Integer, Integer>> result1 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 0);
    Map<String, Pair<Integer, Integer>> expectResult1 =
        genRanges(
            new Object[][] {
              {"0-0", 0, 4},
              {"0-1", 0, 4},
              {"0-10", 0, 4},
              {"0-11", 0, 4},
              {"0-12", 0, 2}
            });
    Assert.assertEquals(expectResult1, result1);

    Map<String, Pair<Integer, Integer>> result2 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 1);
    Map<String, Pair<Integer, Integer>> expectResult2 =
        genRanges(
            new Object[][] {
              {"0-12", 3, 4},
              {"0-2", 0, 4},
              {"0-3", 0, 4},
              {"0-4", 0, 4},
              {"0-5", 0, 3}
            });
    Assert.assertEquals(expectResult2, result2);

    Map<String, Pair<Integer, Integer>> result3 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, subPartitionSize, 2);
    Map<String, Pair<Integer, Integer>> expectResult3 =
        genRanges(
            new Object[][] {
              {"0-5", 4, 4},
              {"0-6", 0, 4},
              {"0-7", 0, 4},
              {"0-8", 0, 4},
              {"0-9", 0, 4},
              {"0-91", 0, 0}
            });
    Assert.assertEquals(expectResult3, result3);
  }

  @Test
  public void testBoundary() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    locations.add(genPartitionLocation(0, new Long[] {0L, 100L, 200L, 300L, 400L, 500L}));

    for (int i = 0; i < 5; i++) {
      Map<String, Pair<Integer, Integer>> result =
          CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 5, i);
      Map<String, Pair<Integer, Integer>> expectResult = genRanges(new Object[][] {{"0-0", i, i}});
      Assert.assertEquals(expectResult, result);
    }
  }

  /**
   * CELEBORN-2032 combined with skewed-partition reading (readSkewPartitionWithoutMapRange):
   * splitSkewedPartitionLocations resolves a logical sub-partition's chunk range purely from the
   * chunk offsets of whichever physical PartitionLocation instance is passed in. The primary and
   * its replica are flushed independently by two different Workers, so their chunk offsets are not
   * guaranteed to match even though they hold the same data and share the same uniqueId.
   *
   * <p>This test simulates a first attempt (reads primary) and a retry/speculative attempt (reads
   * replica, per CELEBORN-2032's odd-attemptNumber-prefers-replica policy) for the exact same
   * logical sub-partition index, and shows that the resolved chunk range differs between the two:
   * reading the primary resolves to chunk range [2, 3], while reading the replica for the very same
   * sub-partition resolves to [3, 3] and drops chunk 2 entirely. If the caller were to switch
   * between primary/replica across attempts (as happened before this fix), the bytes/CRC actually
   * read would differ across attempts and fail the AQE skew validation
   * (SkewHandlingWithoutMapRangeValidator) on retry.
   */
  @Test
  public void testSkewPartitionSplitDiffersBetweenPrimaryAndReplicaChunkOffsets() {
    // Primary and replica hold the same logical data (same total file size 900) but flush
    // independently, resulting in different physical chunk boundaries around the sub-partition
    // split point (step = 900 / 3 = 300).
    PartitionLocation primary =
        genPartitionLocation(0, new Long[] {0L, 100L, 200L, 340L, 600L, 900L});
    PartitionLocation replica =
        genPartitionLocation(0, new Long[] {0L, 100L, 200L, 260L, 600L, 900L});
    Assert.assertEquals(
        "primary and replica must represent the same logical partition",
        primary.getUniqueId(),
        replica.getUniqueId());

    int subPartitionSize = 3;
    int subPartitionIndex = 1; // e.g. the sub-partition assigned to this reduce task

    Map<String, Pair<Integer, Integer>> primaryResult =
        CelebornPartitionUtil.splitSkewedPartitionLocations(
            new ArrayList<>(Collections.singletonList(primary)),
            subPartitionSize,
            subPartitionIndex);
    Map<String, Pair<Integer, Integer>> replicaResult =
        CelebornPartitionUtil.splitSkewedPartitionLocations(
            new ArrayList<>(Collections.singletonList(replica)),
            subPartitionSize,
            subPartitionIndex);

    Map<String, Pair<Integer, Integer>> expectedPrimaryRange =
        genRanges(new Object[][] {{"0-0", 2, 3}});
    Map<String, Pair<Integer, Integer>> expectedReplicaRange =
        genRanges(new Object[][] {{"0-0", 3, 3}});
    Assert.assertEquals(expectedPrimaryRange, primaryResult);
    Assert.assertEquals(expectedReplicaRange, replicaResult);

    // The core bug: for the identical (uniqueId, subPartitionIndex), the chunk range resolved
    // depends on which replica's chunk offsets were used, so it is NOT idempotent across
    // attempts unless the same replica is consistently read every time. Here the replica-based
    // range [3, 3] is a strict subset of the primary-based range [2, 3] and drops chunk 2
    // entirely, which is exactly the kind of mismatch that fails
    // SkewHandlingWithoutMapRangeValidator when different attempts read different replicas.
    Assert.assertNotEquals(
        "chunk range must not depend on which replica is read, otherwise retries/speculative "
            + "attempts will read a different byte range for the same logical sub-partition",
        primaryResult.get(primary.getUniqueId()),
        replicaResult.get(replica.getUniqueId()));
  }

  @Test
  public void testSplitStable() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      locations.add(genPartitionLocation(i, new Long[] {0L, 100L, 200L, 300L, 500L, 1000L}));
    }
    locations.add(genPartitionLocation(91, new Long[] {0L, 1L}));

    Collections.shuffle(locations);

    Map<String, Pair<Integer, Integer>> result =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 3, 0);
    Map<String, Pair<Integer, Integer>> expectResult =
        genRanges(
            new Object[][] {
              {"0-0", 0, 4},
              {"0-1", 0, 4},
              {"0-10", 0, 4},
              {"0-11", 0, 4},
              {"0-12", 0, 2}
            });
    Assert.assertEquals(expectResult, result);
  }

  private ArrayList<PartitionLocation> genPartitionLocations(Map<Integer, Long[]> epochToOffsets) {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    epochToOffsets.forEach(
        (epoch, offsets) -> {
          PartitionLocation location =
              new PartitionLocation(
                  0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
          StorageInfo storageInfo =
              new StorageInfo(
                  StorageInfo.Type.HDD,
                  "mountPoint",
                  false,
                  "filePath",
                  StorageInfo.LOCAL_DISK_MASK,
                  1,
                  Arrays.asList(offsets));
          location.setStorageInfo(storageInfo);
          locations.add(location);
        });
    return locations;
  }

  private PartitionLocation genPartitionLocation(int epoch, Long[] offsets) {
    PartitionLocation location =
        new PartitionLocation(0, epoch, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
    StorageInfo storageInfo =
        new StorageInfo(
            StorageInfo.Type.HDD,
            "mountPoint",
            false,
            "filePath",
            StorageInfo.LOCAL_DISK_MASK,
            offsets[offsets.length - 1],
            Arrays.asList(offsets));
    location.setStorageInfo(storageInfo);
    return location;
  }

  private Map<String, Pair<Integer, Integer>> genRanges(Object[][] inputs) {
    Map<String, Pair<Integer, Integer>> ranges = new HashMap<>();
    for (Object[] idToChunkRange : inputs) {
      String uid = (String) idToChunkRange[0];
      Pair<Integer, Integer> range = Pair.of((int) idToChunkRange[1], (int) idToChunkRange[2]);
      ranges.put(uid, range);
    }
    return ranges;
  }
}
