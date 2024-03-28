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

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class CelebornInputStreamSuiteJ {

  @Test
  public void returnsCorrectSubSkewPartitionLocationsForIndex() {
    ArrayList<PartitionLocation> locations = createMockLocations(10);
    ArrayList<PartitionLocation> subPartition0 =
        CelebornInputStream.getSubSkewPartitionLocations(locations, 3, 0);
    Assert.assertEquals(3, subPartition0.size());
    Assert.assertEquals("10-1", subPartition0.get(0).getUniqueId());
    Assert.assertEquals("5-1", subPartition0.get(1).getUniqueId());
    Assert.assertEquals("4-1", subPartition0.get(2).getUniqueId());

    ArrayList<PartitionLocation> subPartition1 =
        CelebornInputStream.getSubSkewPartitionLocations(locations, 3, 1);
    Assert.assertEquals(3, subPartition1.size());
    Assert.assertEquals("9-1", subPartition1.get(0).getUniqueId());
    Assert.assertEquals("6-1", subPartition1.get(1).getUniqueId());
    Assert.assertEquals("3-1", subPartition1.get(2).getUniqueId());

    ArrayList<PartitionLocation> subPartition2 =
        CelebornInputStream.getSubSkewPartitionLocations(locations, 3, 2);
    Assert.assertEquals(4, subPartition2.size());
    Assert.assertEquals("8-1", subPartition2.get(0).getUniqueId());
    Assert.assertEquals("7-1", subPartition2.get(1).getUniqueId());
    Assert.assertEquals("2-1", subPartition2.get(2).getUniqueId());
    Assert.assertEquals("1-1", subPartition2.get(3).getUniqueId());
  }

  @Test
  public void returnsEmptyListForEmptyLocations() {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    ArrayList<PartitionLocation> result =
        CelebornInputStream.getSubSkewPartitionLocations(locations, 3, 0);
    Assert.assertTrue(result.isEmpty());
  }

  private ArrayList<PartitionLocation> createMockLocations(int size) {
    ArrayList<PartitionLocation> locations = new ArrayList<>();
    for (int i = 1; i <= size; i++) {
      PartitionLocation location =
          new PartitionLocation(i, 1, "mock", -1, -1, -1, -1, PartitionLocation.Mode.PRIMARY);
      location.getStorageInfo().setFileSize(size - i);
      locations.add(location);
    }
    return locations;
  }
}
