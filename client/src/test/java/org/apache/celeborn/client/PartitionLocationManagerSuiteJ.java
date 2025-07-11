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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class PartitionLocationManagerSuiteJ {
  @Test
  public void testSplitAndUpdateLocations() {
    PartitionLocationManager info = new PartitionLocationManager();
    int numMappers = 7;
    PartitionLocation ep0 = createLoc(0, numMappers - 1, 0, 0);
    info.initLocs(ep0);
    assertNull(info.getLatestPartitionLocation(ep0));

    info.setChildren(ep0, createLocs(info.split(2, ep0, true)));
    PartitionLocation ep1 = info.getLeafLocations(ep0).get(0);
    PartitionLocation ep2 = info.getLeafLocations(ep0).get(1);
    assertEquals(Arrays.asList(ep1, ep2), info.getLeafLocations(ep0));
    info.setChildren(ep2, createLocs(info.split(2, ep2, true)));
    PartitionLocation ep3 = info.getLeafLocations(ep0).get(1);
    PartitionLocation ep4 = info.getLeafLocations(ep0).get(2);
    assertEquals(Arrays.asList(ep1, ep3, ep4), info.getLeafLocations(ep0));
    info.setChildren(ep3, createLocs(info.split(2, ep3, true)));
    PartitionLocation ep5 = info.getLeafLocations(ep0).get(1);
    PartitionLocation ep6 = info.getLeafLocations(ep0).get(2);
    assertEquals(Arrays.asList(ep1, ep5, ep6, ep4), info.getLeafLocations(ep0));
    info.setChildren(ep4, createLocs(info.split(2, ep4, true)));
    PartitionLocation ep7 = info.getLeafLocations(ep0).get(3);
    assertEquals(Arrays.asList(ep1, ep5, ep6, ep7), info.getLeafLocations(ep0));
    info.setChildren(ep1, createLocs(info.split(2, ep1, true)));
    PartitionLocation ep8 = info.getLeafLocations(ep0).get(0);
    PartitionLocation ep9 = info.getLeafLocations(ep0).get(1);
    assertEquals(Arrays.asList(ep8, ep9, ep5, ep6, ep7), info.getLeafLocations(ep0));
    info.setChildren(ep9, createLocs(info.split(2, ep9, true)));
    PartitionLocation ep10 = info.getLeafLocations(ep0).get(1);
    PartitionLocation ep11 = info.getLeafLocations(ep0).get(2);
    assertEquals(Arrays.asList(ep8, ep10, ep11, ep5, ep6, ep7), info.getLeafLocations(ep0));
    info.setChildren(ep8, createLocs(info.split(3, ep8, true)));
    PartitionLocation ep12 = info.getLeafLocations(ep0).get(0);
    PartitionLocation ep13 = info.getLeafLocations(ep0).get(1);
    assertEquals(Arrays.asList(ep12, ep13, ep10, ep11, ep5, ep6, ep7), info.getLeafLocations(ep0));

    assertEquals(info.getLatestPartitionLocation(ep4), ep7);
    assertNotEquals(info.getLatestPartitionLocation(ep4), ep4);
    assertTrue(
        Arrays.asList(ep12, ep13, ep10, ep11).contains(info.getLatestPartitionLocation(ep1)));
    assertTrue(Arrays.asList(ep8, ep9).contains(info.getRandomChild(ep1)));
    assertTrue(Arrays.asList(ep5, ep6).contains(info.getLatestPartitionLocation(ep3)));
    assertTrue(Arrays.asList(ep5, ep6, ep7).contains(info.getLatestPartitionLocation(ep2)));
    assertTrue(
        Arrays.asList(ep12, ep13, ep10, ep11, ep5, ep6, ep7)
            .contains(info.getLatestPartitionLocation(ep0)));

    String range =
        info.getLeafLocations(ep0).stream()
            .map(
                partitionLocation ->
                    partitionLocation.getSplitStart()
                        + "~"
                        + partitionLocation.getSplitEnd()
                        + ":"
                        + partitionLocation.getEpoch())
            .collect(Collectors.joining(","));
    assertEquals("0~0:12,1~1:13,2~2:10,3~3:11,4~4:5,5~5:6,6~6:7", range);
  }

  private PartitionLocation createLoc(int splitStart, int splitEnd, int epoch, int partitionId) {
    return new PartitionLocation(
        partitionId,
        epoch,
        RandomStringUtils.randomAlphabetic(10),
        9091,
        9092,
        9093,
        9094,
        PartitionLocation.Mode.PRIMARY,
        null,
        null,
        null,
        splitStart,
        splitEnd);
  }

  private List<PartitionLocation> createLocs(List<SplitInfo> splitInfos) {
    List<PartitionLocation> partitionLocations = new ArrayList<>();
    for (SplitInfo splitInfo : splitInfos) {
      partitionLocations.add(
          createLoc(
              splitInfo.getSplitStart(),
              splitInfo.getSplitEnd(),
              splitInfo.getEpoch(),
              splitInfo.getPartitionId()));
    }
    return partitionLocations;
  }
}
