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

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

/** Driver-side hot state of one (shuffleId, partitionId); mutated under this instance's monitor. */
private[client] class HotState {
  val activeEpochs = new util.LinkedHashSet[Integer]()
  val hardRetiredEpochs: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  val allocTimeMs = JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()
  val splitReported: util.Set[Integer] = ConcurrentHashMap.newKeySet[Integer]()
  @volatile var desired: Int = 1
}

private[client] class PartitionHotnessTracker(
    conf: CelebornConf,
    latestEpoch: (Int, Int) => Option[Int],
    workerAvailableByLocation: PartitionLocation => Boolean) extends Logging {

  private val adaptivePartitionWriteParallelismMaxLocations =
    conf.clientShuffleAdaptivePartitionWriteParallelismMaxLocations
  private val adaptivePartitionWriteParallelismTargetSplitIntervalMs =
    conf.clientShuffleAdaptivePartitionWriteParallelismTargetSplitIntervalMs

  private val partitionHotStates =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[Integer, HotState]]()

  private val shuffleInitialAllocTimeMs =
    JavaUtils.newConcurrentHashMap[Int, java.lang.Long]()

  private val shuffleParallelismCap =
    JavaUtils.newConcurrentHashMap[Int, java.lang.Integer]()

  /**
   * Process the retire report of one epoch: update the active set (a SOFT_SPLIT location of an
   * available worker stays; every other cause removes the epoch for good), then judge hotness at
   * most once per epoch — a split filled faster than the target interval raises desired to
   * min(cap, ceil(targetInterval / fillTime)).
   */
  private[client] def onEpochRetired(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode],
      nowMs: Long): Unit = {
    val workerAvailable = workerAvailableByLocation(oldPartition)
    val hotState = getOrCreateHotState(shuffleId, partitionId)
    val boxed = Integer.valueOf(epoch)
    hotState.synchronized {
      if (cause.contains(StatusCode.SOFT_SPLIT) && workerAvailable
        && !hotState.hardRetiredEpochs.contains(boxed)) {
        hotState.activeEpochs.add(boxed)
      } else {
        hotState.activeEpochs.remove(boxed)
        hotState.hardRetiredEpochs.add(boxed)
      }
    }
    val measureEligible =
      (cause.contains(StatusCode.SOFT_SPLIT) || cause.contains(StatusCode.HARD_SPLIT)) &&
        workerAvailable
    if (!measureEligible) {
      return
    }
    hotState.synchronized {
      if (hotState.splitReported.add(boxed)) {
        val allocTime = {
          val recorded = hotState.allocTimeMs.get(epoch)
          if (recorded != null) {
            recorded
          } else if (epoch == 0) {
            shuffleInitialAllocTimeMs.get(shuffleId)
          } else {
            null
          }
        }
        if (allocTime != null
          && nowMs - allocTime < adaptivePartitionWriteParallelismTargetSplitIntervalMs) {
          // Floor at 1ms: a same-millisecond report would otherwise divide by zero.
          val fillTimeMs = math.max(1L, nowMs - allocTime)
          val target = math.ceil(
            adaptivePartitionWriteParallelismTargetSplitIntervalMs.toDouble / fillTimeMs).toInt
          // Unregistered shuffle defaults to 1 (registerShuffle precedes any revive).
          val cap = shuffleParallelismCap.getOrDefault(shuffleId, 1).intValue()
          val newDesired = math.min(cap, target)
          if (newDesired > hotState.desired) {
            hotState.desired = newDesired
            logInfo(s"Partition $shuffleId-$partitionId: fillTime ${fillTimeMs}ms, " +
              s"boost desired location count to $newDesired.")
          }
        }
      }
    }
  }

  private[client] def recordInitialAllocTime(
      shuffleId: Int,
      partitionLocations: Array[PartitionLocation],
      numMappers: Int,
      nowMs: Long): Unit = {
    if (partitionLocations != null && partitionLocations.nonEmpty) {
      shuffleInitialAllocTimeMs.putIfAbsent(shuffleId, nowMs)
      shuffleParallelismCap.computeIfAbsent(
        shuffleId,
        new util.function.Function[Int, java.lang.Integer]() {
          override def apply(s: Int): java.lang.Integer =
            if (adaptivePartitionWriteParallelismMaxLocations > 0) {
              math.max(1, math.min(adaptivePartitionWriteParallelismMaxLocations, numMappers))
            } else {
              math.max(1, numMappers)
            }
        })
    }
  }

  private[client] def registerAllocation(
      shuffleId: Int,
      partitionId: Int,
      epochs: Set[Int],
      nowMs: Long): Unit = {
    if (epochs.isEmpty) return
    val entry = getOrCreateHotState(shuffleId, partitionId)
    entry.synchronized {
      epochs.foreach { epoch =>
        entry.activeEpochs.add(Integer.valueOf(epoch))
        entry.allocTimeMs.putIfAbsent(epoch, nowMs)
      }
    }
  }

  private[client] def desiredLocationCount(shuffleId: Int, partitionId: Int): Int = {
    val entry = hotStateOrNull(shuffleId, partitionId)
    if (entry == null) 1 else entry.desired
  }

  private def getOrCreateHotState(shuffleId: Int, partitionId: Int): HotState = {
    val map = partitionHotStates.computeIfAbsent(
      shuffleId,
      new util.function.Function[Int, ConcurrentHashMap[Integer, HotState]]() {
        override def apply(s: Int): ConcurrentHashMap[Integer, HotState] =
          JavaUtils.newConcurrentHashMap()
      })
    map.computeIfAbsent(
      partitionId,
      new util.function.Function[Integer, HotState]() {
        override def apply(p: Integer): HotState = new HotState()
      })
  }

  private def hotStateOrNull(shuffleId: Int, partitionId: Int): HotState = {
    val map = partitionHotStates.get(shuffleId)
    if (map == null) null else map.get(partitionId)
  }

  private[client] def currentActiveEpochs(shuffleId: Int, partitionId: Int): Set[Int] = {
    val entry = hotStateOrNull(shuffleId, partitionId)
    if (entry != null) {
      entry.synchronized {
        entry.activeEpochs.asScala.map(_.intValue()).toSet
      }
    } else {
      latestEpoch(shuffleId, partitionId).toSet
    }
  }

  private[client] def retireUnavailableWorkerEpochs(
      shuffleId: Int,
      partitionId: Int,
      epochs: Set[Int]): Unit = {
    if (epochs.isEmpty) return
    val entry = hotStateOrNull(shuffleId, partitionId)
    if (entry == null) return
    entry.synchronized {
      epochs.foreach { epoch =>
        val boxed = Integer.valueOf(epoch)
        entry.activeEpochs.remove(boxed)
        entry.hardRetiredEpochs.add(boxed)
      }
    }
  }

  private[client] def removeShuffle(shuffleId: Int): Unit = {
    partitionHotStates.remove(shuffleId)
    shuffleInitialAllocTimeMs.remove(shuffleId)
    shuffleParallelismCap.remove(shuffleId)
  }
}
