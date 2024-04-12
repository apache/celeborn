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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.StorageInfo;

public class CelebornShuffleReaderSuiteJ {
  @Test
  public void testSkewPartitionSplit() {
    CelebornShuffleHandle handle =
        new CelebornShuffleHandle(
            "appId", "host", 0, new UserIdentifier("mock", "mock"), 0, false, 10, null);

    MockedStatic<ShuffleClient> shuffleClient = null;
    try {
      shuffleClient = Mockito.mockStatic(ShuffleClient.class);
      CelebornShuffleReader shuffleReader =
          new CelebornShuffleReader(
              handle, 0, 10, 0, 10, null, new CelebornConf(), null, new ExecutorShuffleIdTracker());

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

      Map<String, Pair<Integer, Integer>> expectResult = Maps.newHashMap();

      for (int i = 0; i < 5; i++) {
        int subPartitionSize = 3;

        int subPartitionIndex = 0;
        Map<String, Pair<Integer, Integer>> result1 =
            shuffleReader.splitSkewedPartitionLocations(
                locations, subPartitionSize, subPartitionIndex);
        expectResult.clear();
        expectResult.put("0-0", Pair.of(0, 5));
        expectResult.put("0-1", Pair.of(0, 5));
        expectResult.put("0-10", Pair.of(0, 5));
        expectResult.put("0-11", Pair.of(0, 5));
        expectResult.put("0-12", Pair.of(0, 3));
        Assert.assertEquals(expectResult, result1);

        subPartitionIndex = 1;
        Map<String, Pair<Integer, Integer>> result2 =
            shuffleReader.splitSkewedPartitionLocations(
                locations, subPartitionSize, subPartitionIndex);
        expectResult.clear();
        expectResult.put("0-12", Pair.of(4, 5));
        expectResult.put("0-2", Pair.of(0, 5));
        expectResult.put("0-3", Pair.of(0, 5));
        expectResult.put("0-4", Pair.of(0, 5));
        expectResult.put("0-5", Pair.of(0, 4));
        Assert.assertEquals(expectResult, result2);

        subPartitionIndex = 2;
        Map<String, Pair<Integer, Integer>> result3 =
            shuffleReader.splitSkewedPartitionLocations(
                locations, subPartitionSize, subPartitionIndex);
        expectResult.clear();
        expectResult.put("0-5", Pair.of(5, 5));
        expectResult.put("0-6", Pair.of(0, 5));
        expectResult.put("0-7", Pair.of(0, 5));
        expectResult.put("0-8", Pair.of(0, 5));
        expectResult.put("0-9", Pair.of(0, 5));
        expectResult.put("0-91", Pair.of(0, 1));
        Assert.assertEquals(expectResult, result3);
      }
    } finally {
      if (null != shuffleClient) {
        shuffleClient.close();
      }
    }
  }
}
