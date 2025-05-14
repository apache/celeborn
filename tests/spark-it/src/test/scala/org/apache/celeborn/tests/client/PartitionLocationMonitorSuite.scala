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

package org.apache.celeborn.tests.client

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.PartitionLocationMonitor
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode

class PartitionLocationMonitorSuite extends CelebornFunSuite {
  private def newPartitionLocation(epochId: Int): PartitionLocation = {
    new PartitionLocation(0, epochId, "localhost", 1, 1, 1, 1, PartitionLocation.Mode.PRIMARY);
  }

  test("quickly reserve slots") {
    val conf: CelebornConf = new CelebornConf()
    val partitionLocationMonitor = new PartitionLocationMonitor(0, 0, conf, 10)
    val epochGenerator = new AtomicInteger
    val epochIds = new ArrayBuffer[Int]
    // start with 1 slot
    val epochId = epochGenerator.incrementAndGet()
    epochIds += epochId
    partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // 1 slot is split, nextReserveSlotCount should be 1
    epochIds.foreach {
      epochId =>
        partitionLocationMonitor.receivePartitionSplitOrRevived(
          epochId,
          Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    var nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 1)

    // reserve 1 new slot
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // 1 new slot is split, nextReserveSlotCount should be 2
    epochIds.foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 2)

    // reserve 2 new slots
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 2)

    // 2 new slots are split, nextReserveSlotCount should be 3
    epochIds.foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 3)
  }

  test("slowly reserve slots") {
    val conf: CelebornConf = new CelebornConf()
      .set(CelebornConf.CLIENT_ACTIVE_FULL_LOCATION_TIME_WINDOW.key, "2s")
      .set(CelebornConf.CLIENT_ACTIVE_FULL_LOCATION_INTERVAL_PER_BUCKET.key, "1s")
      .set(CelebornConf.CLIENT_EXPECTED_WORKER_SPEED_MB_PER_SECOND.key, "1024")
    val partitionLocationMonitor = new PartitionLocationMonitor(0, 0, conf, 10)
    val epochGenerator = new AtomicInteger
    val epochIds = new ArrayBuffer[Int]

    // reserve 10 new slot
    for (i <- 0 until 10) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }

    epochIds.slice(0, 5).foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 5)

    // sleep timeWindowsInSecs = 2s, we expect all active full locations are expired
    Thread.sleep(2 * 1000L)
    assert(partitionLocationMonitor.activeLocationCount == 5)

    // 3 slots are split, nextReserveSlotCount should be 0
    epochIds.slice(5, 8).foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 2)
    assert(partitionLocationMonitor.nextReserveSlotCount == 0)
  }

  test("max active location limit") {
    val conf: CelebornConf = new CelebornConf()
    val partitionLocationMonitor = new PartitionLocationMonitor(0, 0, conf, 3)
    val epochGenerator = new AtomicInteger
    val epochIds = new ArrayBuffer[Int]

    // start with 1 slot
    val epochId = epochGenerator.incrementAndGet()
    epochIds += epochId
    partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // 1 slot is split, nextReserveSlotCount should be 1
    epochIds.foreach {
      epochId =>
        partitionLocationMonitor.receivePartitionSplitOrRevived(
          epochId,
          Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    var nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 1)

    // reserve 1 new slot
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // 1 new slot is split, nextReserveSlotCount should be 2
    epochIds.foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 2)

    // reserve 2 new slots
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 2)

    // 2 new slots are split, nextReserveSlotCount should be 4
    // but max active location limit is 3, so nextReserveSlotCount should be 3
    epochIds.foreach { epochId =>
      partitionLocationMonitor.receivePartitionSplitOrRevived(epochId, Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 3)
  }

  test("soft split then hard split and failed") {
    val conf: CelebornConf = new CelebornConf()
    val partitionLocationMonitor = new PartitionLocationMonitor(0, 0, conf, 10)
    val epochGenerator = new AtomicInteger
    val epochIds = new ArrayBuffer[Int]

    // start with 1 slot
    val epochId = epochGenerator.incrementAndGet()
    epochIds += epochId
    partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // 1 slot is split, nextReserveSlotCount should be 1
    epochIds.foreach {
      epochId =>
        partitionLocationMonitor.receivePartitionSplitOrRevived(
          epochId,
          Some(StatusCode.SOFT_SPLIT))
    }
    assert(partitionLocationMonitor.activeLocationCount == 0)
    var nextReserveSlotCount = partitionLocationMonitor.nextReserveSlotCount
    assert(nextReserveSlotCount == 1)

    // reserve 1 new slot
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 1)

    // epoch 1 is hard split, new activeLocationCount should be 1, current activeLocationCount is 1 so nextReserveSlotCount should be 1
    partitionLocationMonitor.receivePartitionSplitOrRevived(1, Some(StatusCode.HARD_SPLIT))
    assert(partitionLocationMonitor.activeLocationCount == 1)
    assert(partitionLocationMonitor.nextReserveSlotCount == 1)

    // reserve 1 new slot
    for (i <- 0 until nextReserveSlotCount) {
      val epochId = epochGenerator.incrementAndGet()
      partitionLocationMonitor.addActiveLocationEpoch(newPartitionLocation(epochId))
      epochIds += epochId
    }
    assert(partitionLocationMonitor.activeLocationCount == 2)
    partitionLocationMonitor.receivePartitionSplitOrRevived(
      2,
      Some(StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY))
    assert(partitionLocationMonitor.activeLocationCount == 1)
    assert(partitionLocationMonitor.nextReserveSlotCount == 1)
  }
}
