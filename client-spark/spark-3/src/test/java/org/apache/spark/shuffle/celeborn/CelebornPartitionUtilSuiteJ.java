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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class CelebornPartitionUtilSuiteJ {
  @Test
  public void testSkewPartitionSplit() {

    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      PartitionLocation location =
          new PartitionLocation(0, i, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
      StorageInfo storageInfo =
          new StorageInfo(
              StorageInfo.Type.HDD,
              "mountPoint",
              false,
              "filePath",
              StorageInfo.LOCAL_DISK_MASK,
              1000,
              Arrays.asList(0L, 100L, 200L, 300L, 500L, 1000L));
      location.setStorageInfo(storageInfo);
      locations.add(location);
    }

    PartitionLocation location =
        new PartitionLocation(0, 91, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
    StorageInfo storageInfo =
        new StorageInfo(
            StorageInfo.Type.HDD,
            "mountPoint",
            false,
            "filePath",
            StorageInfo.LOCAL_DISK_MASK,
            1,
            Arrays.asList(0L, 1L));
    location.setStorageInfo(storageInfo);
    locations.add(location);

    int subPartitionSize = 3;

    int subPartitionIndex = 0;
    Map<String, Pair<Integer, Integer>> result1 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(
            locations, subPartitionSize, subPartitionIndex);
    Map<String, Pair<Integer, Integer>> expectResult1 =
        Map.ofEntries(
            Map.entry("0-0", Pair.of(0, 4)),
            Map.entry("0-1", Pair.of(0, 4)),
            Map.entry("0-10", Pair.of(0, 4)),
            Map.entry("0-11", Pair.of(0, 4)),
            Map.entry("0-12", Pair.of(0, 2)));
    Assert.assertEquals(expectResult1, result1);

    subPartitionIndex = 1;
    Map<String, Pair<Integer, Integer>> result2 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(
            locations, subPartitionSize, subPartitionIndex);
    Map<String, Pair<Integer, Integer>> expectResult2 =
        Map.ofEntries(
            Map.entry("0-12", Pair.of(3, 4)),
            Map.entry("0-2", Pair.of(0, 4)),
            Map.entry("0-3", Pair.of(0, 4)),
            Map.entry("0-4", Pair.of(0, 4)),
            Map.entry("0-5", Pair.of(0, 3)));
    Assert.assertEquals(expectResult2, result2);

    subPartitionIndex = 2;
    Map<String, Pair<Integer, Integer>> result3 =
        CelebornPartitionUtil.splitSkewedPartitionLocations(
            locations, subPartitionSize, subPartitionIndex);
    Map<String, Pair<Integer, Integer>> expectResult3 =
        Map.ofEntries(
            Map.entry("0-5", Pair.of(4, 4)),
            Map.entry("0-6", Pair.of(0, 4)),
            Map.entry("0-7", Pair.of(0, 4)),
            Map.entry("0-8", Pair.of(0, 4)),
            Map.entry("0-9", Pair.of(0, 4)),
            Map.entry("0-91", Pair.of(0, 0)));
    Assert.assertEquals(expectResult3, result3);
  }

  @Test
  public void testBoundary() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    PartitionLocation location =
        new PartitionLocation(0, 0, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
    StorageInfo storageInfo =
        new StorageInfo(
            StorageInfo.Type.HDD,
            "mountPoint",
            false,
            "filePath",
            StorageInfo.LOCAL_DISK_MASK,
            500,
            Arrays.asList(0L, 100L, 200L, 300L, 400L, 500L));
    location.setStorageInfo(storageInfo);
    locations.add(location);

    for (int i = 0; i < 5; i++) {
      Map<String, Pair<Integer, Integer>> result =
          CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 5, i);
      Map<String, Pair<Integer, Integer>> expectResult =
          Map.ofEntries(Map.entry("0-0", Pair.of(i, i)));
      Assert.assertEquals(expectResult, result);
    }
  }

  @Test
  public void testSplitStable() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 0; i < 13; i++) {
      PartitionLocation location =
          new PartitionLocation(0, i, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
      StorageInfo storageInfo =
          new StorageInfo(
              StorageInfo.Type.HDD,
              "mountPoint",
              false,
              "filePath",
              StorageInfo.LOCAL_DISK_MASK,
              1000,
              Arrays.asList(0L, 100L, 200L, 300L, 500L, 1000L));
      location.setStorageInfo(storageInfo);
      locations.add(location);
    }

    PartitionLocation location =
        new PartitionLocation(0, 91, "localhost", 0, 0, 0, 0, PartitionLocation.Mode.PRIMARY);
    StorageInfo storageInfo =
        new StorageInfo(
            StorageInfo.Type.HDD,
            "mountPoint",
            false,
            "filePath",
            StorageInfo.LOCAL_DISK_MASK,
            1,
            Arrays.asList(0L, 1L));
    location.setStorageInfo(storageInfo);
    locations.add(location);

    Collections.shuffle(locations);

    Map<String, Pair<Integer, Integer>> result =
        CelebornPartitionUtil.splitSkewedPartitionLocations(locations, 3, 0);
    Map<String, Pair<Integer, Integer>> expectResult =
        Map.ofEntries(
            Map.entry("0-0", Pair.of(0, 4)),
            Map.entry("0-1", Pair.of(0, 4)),
            Map.entry("0-10", Pair.of(0, 4)),
            Map.entry("0-11", Pair.of(0, 4)),
            Map.entry("0-12", Pair.of(0, 2)));
    Assert.assertEquals(expectResult, result);
  }
}
