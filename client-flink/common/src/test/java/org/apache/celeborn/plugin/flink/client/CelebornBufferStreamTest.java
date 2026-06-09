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

package org.apache.celeborn.plugin.flink.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.celeborn.common.protocol.PartitionLocation;

public class CelebornBufferStreamTest {

  // hasRemainingPartitions only reads locations.length, so null elements suffice here.
  private static CelebornBufferStream streamWithLocations(int numLocations) {
    return new CelebornBufferStream(
        null, null, "shuffleKey", new PartitionLocation[numLocations], 0, 0, 0L, null);
  }

  @Test
  public void hasRemainingPartitionsTracksLocationIndexBoundary() {
    // openStreamInternal advances currentLocationIndex past the partition it opened, so the last
    // partition's stream end sees index == length, where lastPartition = !hasRemainingPartitions().
    CelebornBufferStream stream = streamWithLocations(2);
    assertTrue(stream.hasRemainingPartitions()); // index 0 < 2
    stream.setCurrentLocationIndex(1);
    assertTrue(stream.hasRemainingPartitions()); // index 1 < 2, more partitions remain
    stream.setCurrentLocationIndex(2);
    assertFalse(stream.hasRemainingPartitions()); // index 2 == 2, last partition consumed
  }

  @Test
  public void hasRemainingPartitionsIsFalseWithoutLocations() {
    assertFalse(streamWithLocations(0).hasRemainingPartitions());
  }
}
