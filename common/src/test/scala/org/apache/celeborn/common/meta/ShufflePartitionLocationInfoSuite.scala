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

import org.junit.Assert.{assertEquals, assertTrue}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.protocol.PartitionLocation

class ShufflePartitionLocationInfoSuite extends CelebornFunSuite {

  test("test operate shuffle partition locations.") {
    val partitionLocation00 = mockPartition(0, 0)
    val partitionLocation01 = mockPartition(0, 1)
    val partitionLocation02 = mockPartition(0, 2)
    val partitionLocation12 = mockPartition(1, 2)
    val partitionLocation11 = mockPartition(1, 1)

    val masterLocations = new util.ArrayList[PartitionLocation]()
    masterLocations.add(partitionLocation00)
    masterLocations.add(partitionLocation01)
    masterLocations.add(partitionLocation02)
    masterLocations.add(partitionLocation11)
    masterLocations.add(partitionLocation12)

    val slaveLocations = new util.ArrayList[PartitionLocation]()
    val partitionLocationSlave00 = mockPartition(0, 0)
    val partitionLocationSlave10 = mockPartition(1, 0)
    slaveLocations.add(partitionLocationSlave00)
    slaveLocations.add(partitionLocationSlave10)

    val shufflePartitionLocationInfo = new ShufflePartitionLocationInfo
    shufflePartitionLocationInfo.addMasterPartitions(masterLocations)
    shufflePartitionLocationInfo.addSlavePartitions(slaveLocations)

    // test add
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions().size(), 5)
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions(Some(0)).size(), 3)
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions(Some(1)).size(), 2)

    assertEquals(shufflePartitionLocationInfo.getSlavePartitions().size(), 2)
    assertEquals(shufflePartitionLocationInfo.getSlavePartitions(Some(0)).size(), 1)
    assertEquals(shufflePartitionLocationInfo.getSlavePartitions(Some(1)).size(), 1)

    // test get min epoch
    val locations = shufflePartitionLocationInfo.getAllMasterLocationsWithMinEpoch()
    assertTrue(locations.contains(partitionLocation00) && locations.contains(partitionLocation11))

    // test remove
    assertEquals(shufflePartitionLocationInfo.removeMasterPartitions(0).size(), 3)
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions().size(), 2)
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions(Some(1)).size(), 2)

    assertEquals(shufflePartitionLocationInfo.removeSlavePartitions(0).size(), 1)
    assertEquals(shufflePartitionLocationInfo.getSlavePartitions().size(), 1)
    assertEquals(shufflePartitionLocationInfo.getSlavePartitions(Some(1)).size(), 1)

    // test remove all
    assertEquals(
      shufflePartitionLocationInfo.removeAndGetAllMasterPartitionIds(),
      new util.HashSet[Integer] { add(1) })
    assertEquals(shufflePartitionLocationInfo.getMasterPartitions().size(), 0)
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
      PartitionLocation.Mode.MASTER)
  }

}
