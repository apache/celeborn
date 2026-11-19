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

package org.apache.celeborn.service.deploy.master.scale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.internal.config.ConfigEntry
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.{PbScaleType, PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.server.common.service.config.DynamicConfigServiceFactory
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAHelper

case class ScaleTarget(targetSpecWorkerCnt: Int, accountedExcludedWorkerCnt: Int)

abstract class BaseK8sScaleManager(conf: CelebornConf) extends AbstractScaleManager(conf) {

  protected lazy val configService =
    Option(DynamicConfigServiceFactory.getConfigService(conf))

  private var currentLeaderVersion = 0L
  private var accountedExcludedWorkerCnt = 0
  private var unconfirmedPlatformUpdate = Option.empty[(ScaleOperation, ScaleTarget)]

  protected def calculateSpecWorkerCnt(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCnt: Int,
      newExcludedWorkerCnt: Int): Int

  protected def calculateScaleTarget(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCnt: Int,
      unaccountedExcludedWorkerCnt: Int): ScaleTarget =
    ScaleTarget(
      calculateSpecWorkerCnt(
        registeredWorkerCnt,
        specWorkerCnt,
        workerStats,
        excludedWorkerCnt,
        unaccountedExcludedWorkerCnt),
      excludedWorkerCnt)

  protected def getCurrentClusterInfo(): ClusterInfo

  protected def getWorkerIndex(worker: WorkerInfo): Int

  protected def doScaleOperation(operation: ScaleOperation): String

  protected def checkOperation(operationId: String): OperationResult

  protected def currentTimeMillis: Long = System.currentTimeMillis()

  override protected def checkScale(): Unit = {
    val overrides = configService
      .map(_.getSystemConfigFromCache.getConfigs.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    def setting[T](entry: ConfigEntry[T]): T =
      overrides.get(entry.key).map(entry.valueConverter).getOrElse(conf.get(entry))

    val upEnabled = setting(AUTO_SCALE_UP_ENABLED)
    val downEnabled = setting(AUTO_SCALE_DOWN_ENABLED)
    if (!upEnabled && !downEnabled) {
      return
    }

    val raft = Option(HAHelper.getRatisServer(statusSystem))
    val leaderVersion = raft.map(_.getLeaderVersion).getOrElse(-1L)
    if (currentLeaderVersion != leaderVersion) {
      currentLeaderVersion = leaderVersion
      accountedExcludedWorkerCnt = 0
      unconfirmedPlatformUpdate = None
      statusSystem.invalidateWorkerStats()
      return
    }
    val now = currentTimeMillis
    if (raft.exists(!_.isLeader) || HAHelper.getWorkerTimeoutDeadline(statusSystem) > now) {
      return
    }

    // Wait for metrics from every registered worker before planning.
    val registeredWorkers = statusSystem.workersMap.synchronized {
      statusSystem.workersMap.values().asScala.toVector
    }
    val stats = registeredWorkers.map { worker =>
      Option(statusSystem.workerStats.get(worker)).flatMap(ref => Option(ref.get()))
    }
    if (stats.exists(_.isEmpty)) {
      return
    }
    val workerStats = registeredWorkers.zip(stats.flatten).toMap
    // Wait for the last recorded platform operation to reach a terminal state.
    val previous = Option(statusSystem.lastScaleOperation)
    previous.foreach { last =>
      if (last.getLeft.nonEmpty && !last.getRight.isFinished) {
        val result = checkOperation(last.getLeft)
        if (!result.isFinalStatus) {
          return
        }
        statusSystem.handleUpdateScaleOperation(
          last.getLeft,
          last.getRight.copy(isFinished = true))
      }
    }

    val clusterInfo = getCurrentClusterInfo()
    unconfirmedPlatformUpdate.foreach { case (operation, target) =>
      if (clusterInfo.matchesSpec(operation)) {
        accountedExcludedWorkerCnt = target.accountedExcludedWorkerCnt
      }
    }
    unconfirmedPlatformUpdate = None
    if (!clusterInfo.isInRunningStatus) {
      return
    }
    val (operation, target) = planScaleOperation(clusterInfo, registeredWorkers, workerStats)
    if (operation.scaleType == PbScaleType.NO_SCALE) {
      if (target.targetSpecWorkerCnt == clusterInfo.specWorkerCnt) {
        accountedExcludedWorkerCnt = target.accountedExcludedWorkerCnt
      }
      return
    }
    val window =
      if (operation.scaleType == PbScaleType.SCALE_UP) {
        setting(SCALE_UP_STABILIZATION_WINDOW_INTERVAL)
      } else {
        setting(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL)
      }
    if (previous.exists(last => currentTimeMillis - last.getRight.lastScaleTime < window)) {
      return
    }

    if (operation.scaleType == PbScaleType.SCALE_UP && upEnabled) {
      if (operation.activateWorkers.nonEmpty) {
        statusSystem.handleWorkerEvent(
          WorkerEventType.Recommission_VALUE,
          operation.activateWorkers.asJava,
          MasterClient.genRequestId())
      }
      clusterMayNeedScale(operation, clusterInfo, target)
    } else if (operation.scaleType == PbScaleType.SCALE_DOWN && downEnabled) {
      if (operation.decommissionWorkers.nonEmpty) {
        statusSystem.handleWorkerEvent(
          WorkerEventType.DecommissionThenIdle_VALUE,
          operation.decommissionWorkers.asJava,
          MasterClient.genRequestId())
      }
      clusterMayNeedScale(operation, clusterInfo, target)
    }
  }

  private def clusterMayNeedScale(
      operation: ScaleOperation,
      clusterInfo: ClusterInfo,
      target: ScaleTarget): Unit = {
    val expectedIndices = ClusterInfo(
      operation.specWorkerCnt,
      operation.reservedWorkerIndices,
      true).expectedWorkerIndices
    val acceptsTarget = operation.specWorkerCnt == target.targetSpecWorkerCnt
    if (expectedIndices != clusterInfo.expectedWorkerIndices) {
      // Recheck current registrations because workers can register after planning.
      val safe = statusSystem.workersMap.synchronized {
        statusSystem.workersMap.values().asScala.forall { worker =>
          val index = getWorkerIndex(worker)
          index < 0 || expectedIndices.contains(index) ||
          worker.getWorkerStatus.getState == PbWorkerStatus.State.Idle
        }
      }
      if (!safe) {
        return
      }
      if (acceptsTarget) {
        unconfirmedPlatformUpdate = Some((operation, target))
      }
      val operationId = doScaleOperation(operation)
      if (acceptsTarget) {
        accountedExcludedWorkerCnt = target.accountedExcludedWorkerCnt
        unconfirmedPlatformUpdate = None
      }
      statusSystem.handleUpdateScaleOperation(operationId, operation)
    } else {
      if (acceptsTarget) {
        accountedExcludedWorkerCnt = target.accountedExcludedWorkerCnt
      }
      statusSystem.handleUpdateScaleOperation("", operation.copy(isFinished = true))
    }
  }

  private def planScaleOperation(
      clusterInfo: ClusterInfo,
      registeredWorkers: Seq[WorkerInfo],
      workerStats: Map[WorkerInfo, WorkerStats]): (ScaleOperation, ScaleTarget) = {
    // Plan against registrations that belong to the current platform spec.
    val expectedIndices = clusterInfo.expectedWorkerIndices
    val workers =
      registeredWorkers.filter(worker => expectedIndices.contains(getWorkerIndex(worker)))
    val idle = workers.filter(_.getWorkerStatus.getState == PbWorkerStatus.State.Idle)
    val draining = workers.filter { worker =>
      !idle.contains(worker) && Option(statusSystem.workerEventInfos.get(worker))
        .exists(_.getEventType == WorkerEventType.DecommissionThenIdle)
    }
    val normal = workers.filterNot(worker => idle.contains(worker) || draining.contains(worker))
    val excludedWorkerCnt = normal.count { worker =>
      statusSystem.excludedWorkers.contains(worker) ||
      statusSystem.manuallyExcludedWorkers.contains(worker) ||
      worker.getWorkerStatus.getState != PbWorkerStatus.State.Normal
    }
    val unaccountedExcludedWorkerCnt = math.max(0, excludedWorkerCnt - accountedExcludedWorkerCnt)
    val target = calculateScaleTarget(
      workers.size,
      clusterInfo.specWorkerCnt,
      normal.map(workerStats),
      excludedWorkerCnt,
      unaccountedExcludedWorkerCnt)
    val targetSpecWorkerCnt = target.targetSpecWorkerCnt

    val decommissionCnt = workers.size - targetSpecWorkerCnt
    val allDraining = draining ++ idle
    // Recommission workers when fewer retirements are needed.
    val activate = allDraining.take(math.max(0, allDraining.size - decommissionCnt))
    val decommission =
      if (activate.nonEmpty || decommissionCnt <= 0) {
        Seq.empty[WorkerInfo]
      } else {
        normal.sortBy(getWorkerIndex).reverse
          .take(math.max(0, decommissionCnt - allDraining.size))
      }
    val removable =
      if (activate.nonEmpty || decommissionCnt <= 0) Seq.empty[WorkerInfo] else idle
    // Keep draining workers in the platform spec until they report Idle.
    val pendingDrainCnt = allDraining.size - activate.size + decommission.size - removable.size
    val scaleDownSpecWorkerCnt =
      math.min(clusterInfo.specWorkerCnt, targetSpecWorkerCnt + pendingDrainCnt)
    val currentReserved = clusterInfo.reservedWorkerIndices
      .filter(index => expectedIndices.lastOption.exists(index < _))
    val reserved = (currentReserved ++ removable.map(getWorkerIndex)).distinct.sorted

    val operation =
      if (targetSpecWorkerCnt > clusterInfo.specWorkerCnt || activate.nonEmpty) {
        val specWorkerCnt =
          if (targetSpecWorkerCnt < clusterInfo.specWorkerCnt) {
            math.max(targetSpecWorkerCnt, workers.size)
          } else {
            targetSpecWorkerCnt
          }
        val reservedWorkerIndices =
          if (targetSpecWorkerCnt < clusterInfo.specWorkerCnt) {
            scaleDownReservedIndices(specWorkerCnt, expectedIndices, reserved, workers)
          } else {
            reserved.dropRight(math.min(
              targetSpecWorkerCnt - clusterInfo.specWorkerCnt,
              reserved.size))
          }
        ScaleOperation(
          specWorkerCnt,
          reservedWorkerIndices,
          activate,
          decommission,
          PbScaleType.SCALE_UP,
          currentTimeMillis)
      } else if (removable.nonEmpty || decommission.nonEmpty ||
        scaleDownSpecWorkerCnt < clusterInfo.specWorkerCnt) {
        ScaleOperation(
          scaleDownSpecWorkerCnt,
          scaleDownReservedIndices(scaleDownSpecWorkerCnt, expectedIndices, reserved, workers),
          activate,
          decommission,
          PbScaleType.SCALE_DOWN,
          currentTimeMillis)
      } else {
        ScaleOperation(
          expectedIndices.size,
          reserved,
          activate,
          decommission,
          PbScaleType.NO_SCALE,
          currentTimeMillis)
      }
    (operation, target)
  }

  private def scaleDownReservedIndices(
      specWorkerCnt: Int,
      expectedIndices: Seq[Int],
      removedIndices: Seq[Int],
      workers: Seq[WorkerInfo]): Seq[Int] = {
    val registeredIndices = workers.map(getWorkerIndex).toSet
    val retained = mutable.TreeSet.empty[Int] ++ expectedIndices.filter { index =>
      registeredIndices(index) && !removedIndices.contains(index)
    }
    expectedIndices.iterator.filterNot(removedIndices.contains)
      .takeWhile(_ => retained.size < specWorkerCnt).foreach(retained.add)
    (0 until retained.lastOption.map(_ + 1).getOrElse(0)).filterNot(retained).toVector
  }
}
