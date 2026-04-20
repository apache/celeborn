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

package org.apache.celeborn.common.meta

import java.util

import scala.collection.JavaConverters._

import org.junit.Assert.assertEquals

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.protocol.PartitionLocation

class WorkerPartitionLocationInfoSuite extends CelebornFunSuite {

  test("CELEBORN-575: test after remove the partition location info is empty.") {
    val shuffleKey = "app_12345_12345_1-0"
    val partitionLocation00 = mockPartition(0, 0)
    val partitionLocation01 = mockPartition(0, 1)
    val partitionLocation02 = mockPartition(0, 2)
    val partitionLocation12 = mockPartition(1, 2)
    val partitionLocation11 = mockPartition(1, 1)

    val primaryLocations = new util.ArrayList[PartitionLocation]()
    primaryLocations.add(partitionLocation00)
    primaryLocations.add(partitionLocation01)
    primaryLocations.add(partitionLocation02)
    primaryLocations.add(partitionLocation11)
    primaryLocations.add(partitionLocation12)

    val replicaLocations = new util.ArrayList[PartitionLocation]()
    val partitionLocationReplica00 = mockPartition(0, 0)
    val partitionLocationReplica10 = mockPartition(1, 0)
    replicaLocations.add(partitionLocationReplica00)
    replicaLocations.add(partitionLocationReplica10)

    val workerPartitionLocationInfo = new WorkerPartitionLocationInfo
    workerPartitionLocationInfo.addPrimaryPartitions(shuffleKey, primaryLocations)
    workerPartitionLocationInfo.addReplicaPartitions(shuffleKey, replicaLocations)

    // test remove
    workerPartitionLocationInfo.removePrimaryPartitions(
      shuffleKey,
      primaryLocations.asScala.map(_.getUniqueId).asJava)
    workerPartitionLocationInfo.removeReplicaPartitions(
      shuffleKey,
      replicaLocations.asScala.map(_.getUniqueId).asJava)

    assertEquals(workerPartitionLocationInfo.isEmpty, true)
  }

  test("snapshotUncommittedUniqueIds - empty info returns empty maps") {
    val info = new WorkerPartitionLocationInfo
    val (primary, replica) = info.snapshotUncommittedUniqueIds
    assert(primary.isEmpty)
    assert(replica.isEmpty)
  }

  test("snapshotUncommittedUniqueIds - captures correct IDs across shuffles") {
    val info = new WorkerPartitionLocationInfo
    val shuffle1 = "app1-0"
    val shuffle2 = "app2-1"
    val locs1 = new util.ArrayList[PartitionLocation]()
    locs1.add(mockPartition(0, 0))
    locs1.add(mockPartition(1, 0))
    info.addPrimaryPartitions(shuffle1, locs1)
    val locs2 = new util.ArrayList[PartitionLocation]()
    locs2.add(mockPartition(2, 0))
    info.addPrimaryPartitions(shuffle2, locs2)
    val replicaLocs = new util.ArrayList[PartitionLocation]()
    replicaLocs.add(mockPartition(3, 0))
    info.addReplicaPartitions(shuffle1, replicaLocs)
    val (primary, replica) = info.snapshotUncommittedUniqueIds
    assert(primary.size == 2)
    assert(primary(shuffle1).size() == 2)
    assert(primary(shuffle1).contains("0-0"))
    assert(primary(shuffle1).contains("1-0"))
    assert(primary(shuffle2).size() == 1)
    assert(primary(shuffle2).contains("2-0"))
    assert(replica.size == 1)
    assert(replica(shuffle1).size() == 1)
    assert(replica(shuffle1).contains("3-0"))
  }

  test("snapshotUncommittedUniqueIds - filters empty shuffle keys") {
    val info = new WorkerPartitionLocationInfo
    val shuffleKey = "app1-0"
    val locs = new util.ArrayList[PartitionLocation]()
    locs.add(mockPartition(0, 0))
    locs.add(mockPartition(1, 0))
    info.addPrimaryPartitions(shuffleKey, locs)
    info.removePrimaryPartitions(shuffleKey, locs.asScala.map(_.getUniqueId).asJava)
    val (primary, _) = info.snapshotUncommittedUniqueIds
    assert(!primary.contains(shuffleKey))
  }

  test("snapshotUncommittedUniqueIds - snapshot is a point-in-time copy") {
    val info = new WorkerPartitionLocationInfo
    val shuffleKey = "app1-0"
    val locs = new util.ArrayList[PartitionLocation]()
    locs.add(mockPartition(0, 0))
    info.addPrimaryPartitions(shuffleKey, locs)
    val (primary, _) = info.snapshotUncommittedUniqueIds
    assert(primary(shuffleKey).size() == 1)
    // Add more partitions after snapshot
    val moreLocs = new util.ArrayList[PartitionLocation]()
    moreLocs.add(mockPartition(1, 0))
    moreLocs.add(mockPartition(2, 0))
    info.addPrimaryPartitions(shuffleKey, moreLocs)
    // Snapshot remains unchanged
    assert(primary(shuffleKey).size() == 1)
  }

  private def mockPartition(partitionId: Int, epoch: Int): PartitionLocation = {
    new PartitionLocation(
      partitionId,
      epoch,
      "mock",
      -1,
      -1,
      -1,
      -1,
      PartitionLocation.Mode.PRIMARY)
  }
}
