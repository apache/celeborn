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

package org.apache.celeborn.client

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.math.ceil

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.util.ByteUnit
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.ConcurrentSkipListMapWithTracker

class PartitionLocationMonitor(
    shuffleId: Int,
    partitionId: Int,
    conf: CelebornConf,
    maxActiveLocation: Int) extends Logging {

  private val timeWindowsInSecs = conf.clientActiveFullLocationTimeWindowSecs
  private val intervalPerBucketInMills = conf.clientActiveFullLocationIntervalPerBucketMs
  private val expectedWorkerSpeedMBPerSec = conf.clientExpectedWorkerSpeedMBPerSecond
  private lazy val activeFullLocationHub =
    new PartitionSplitTimeSlidingHub(timeWindowsInSecs.toInt, intervalPerBucketInMills.toInt)
  // epochId -> partitionLocation
  private val activeLocationEpochs = new ConcurrentSkipListMapWithTracker[Int, PartitionLocation]()
  private val softSplitEpochIds: java.util.Set[Int] = ConcurrentHashMap.newKeySet()
  private val exceptionEpochIds: java.util.Set[Int] = ConcurrentHashMap.newKeySet()
  private val softSplitSizeMB =
    ByteUnit.BYTE.convertTo(conf.shufflePartitionSplitThreshold, ByteUnit.MiB)
  private val hardSplitSizeMB = softSplitSizeMB * 3 // TODO: @漠云 后续考虑进行修改

  def addActiveLocationEpoch(partitionLocation: PartitionLocation): Unit = {
    activeLocationEpochs.put(partitionLocation.getEpoch, partitionLocation)
  }

  def getActiveLocations(clientMaxEpoch: Int): Seq[PartitionLocation] = {
    activeLocationEpochs.tailMap(clientMaxEpoch + 1).values().asScala.toSeq
  }

  // if active full location hub changed, return true
  def receivePartitionSplitOrRevived(epoch: Int, cause: Option[StatusCode]): Boolean = {
    var changed = false
    if (cause.isDefined) {
      if (cause.get == StatusCode.SOFT_SPLIT) { // soft_split
        if (activeLocationEpochs.remove(epoch) || exceptionEpochIds.remove(epoch)) {
          activeFullLocationHub.add(new PartitionSplitNode(softSplitSizeMB))
          softSplitEpochIds.add(epoch)
          changed = true
        }
      } else if (cause.get == StatusCode.HARD_SPLIT) {
        if (activeLocationEpochs.remove(epoch) || exceptionEpochIds.remove(epoch)) {
          activeFullLocationHub.add(new PartitionSplitNode(hardSplitSizeMB))
          changed = true
        } else if (softSplitEpochIds.remove(epoch)) {
          activeFullLocationHub.add(new PartitionSplitNode(hardSplitSizeMB - softSplitSizeMB))
          changed = true
        }
      } else {
        if (activeLocationEpochs.remove(epoch)) {
          exceptionEpochIds.add(epoch)
          changed = true
        }
      }
    }
    if (changed) {
      logInfo(
        s"Receive shuffleId: $shuffleId, partitionId: $partitionId, epochId: $epoch, cause: $cause")
    }
    changed
  }

  def activeLocationCount: Int = {
    activeLocationEpochs.size
  }

  def nextReserveSlotCount: Int = {
    val pushSpeed = activeFullLocationHub.getActiveFullLocationSizeMBPerSec()
    Math.max(
      Math.min(
        maxActiveLocation,
        ceil(pushSpeed.toDouble / expectedWorkerSpeedMBPerSec)).toInt - activeLocationCount,
      0)
  }

  def report: String = {
    activeLocationEpochs.report()
  }
}
