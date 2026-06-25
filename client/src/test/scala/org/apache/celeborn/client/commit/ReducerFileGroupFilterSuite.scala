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

package org.apache.celeborn.client.commit

import java.util

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.PartitionLocation.Mode
import org.apache.celeborn.common.write.LocationPushFailedBatches

class ReducerFileGroupFilterSuite extends CelebornFunSuite {

  private def location(partitionId: Int): PartitionLocation =
    new PartitionLocation(
      partitionId,
      0,
      s"host-$partitionId",
      1000 + partitionId,
      2000 + partitionId,
      3000 + partitionId,
      4000 + partitionId,
      Mode.PRIMARY)

  test("filter reducer file groups and failed batches to the requested range") {
    val allFileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    val allPushFailedBatches = new util.HashMap[String, LocationPushFailedBatches]()
    (0 until 4).foreach { partitionId =>
      val partitionLocation = location(partitionId)
      allFileGroups.put(partitionId, util.Collections.singleton(partitionLocation))
      allPushFailedBatches.put(
        partitionLocation.getUniqueId,
        new LocationPushFailedBatches())
    }

    val fileGroups = ReducerFileGroupFilter.fileGroupsForRange(
      allFileGroups,
      startPartition = 1,
      endPartition = 3,
      hasPartitionRange = true)
    val failedBatches = ReducerFileGroupFilter.pushFailedBatchesForFileGroups(
      fileGroups,
      allPushFailedBatches)
    val expectedFailedBatchIds = new util.HashSet[String]()
    expectedFailedBatchIds.add(fileGroups.get(1).iterator().next().getUniqueId)
    expectedFailedBatchIds.add(fileGroups.get(2).iterator().next().getUniqueId)

    val expectedPartitionIds = new util.HashSet[Integer]()
    expectedPartitionIds.add(1)
    expectedPartitionIds.add(2)
    assert(fileGroups.keySet() == expectedPartitionIds)
    assert(failedBatches.keySet() == expectedFailedBatchIds)
  }

  test("filter map partition success IDs and preserve the legacy full response") {
    val allPartitionIds = new util.HashSet[Integer]()
    allPartitionIds.add(0)
    allPartitionIds.add(2)
    allPartitionIds.add(4)
    val ranged = ReducerFileGroupFilter.partitionIdsForRange(
      allPartitionIds,
      startPartition = 1,
      endPartition = 4,
      hasPartitionRange = true)

    assert(ranged == util.Collections.singleton(2))

    val allFileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    allFileGroups.put(0, util.Collections.singleton(location(0)))
    val legacy = ReducerFileGroupFilter.fileGroupsForRange(
      allFileGroups,
      startPartition = 0,
      endPartition = 0,
      hasPartitionRange = false)
    assert(legacy eq allFileGroups)
  }
}
