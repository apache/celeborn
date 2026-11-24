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
import scala.util.Try

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.internal.config.ConfigEntry
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.{PbPendingScaleOperation, PbScaleCheckpoint, PbScaleOperationRequest, PbScaleType, PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.server.common.service.config.DynamicConfigServiceFactory
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAHelper

case class ScaleTarget(targetSpecWorkerCnt: Int, accountedExclWorkerCnt: Int)

abstract class BaseK8sScaleManager(conf: CelebornConf) extends AbstractScaleManager(conf) {

  protected lazy val configService =
    Option(DynamicConfigServiceFactory.getConfigService(conf))

  private var currentLeaderVersion = 0L
  @volatile private var stopping = false

  protected def calculateScaleTarget(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      exclWorkerCnt: Int,
      unaccountedExclWorkerCnt: Int): ScaleTarget

  protected def getCurrentClusterInfo(): ClusterInfo

  protected def doScaleOperation(operation: ScaleOperation): String

  protected def checkOperation(operationId: String): OperationResult

  protected def isRejectedPlatformUpdate(error: Exception): Boolean = false

  protected def currentTimeMillis: Long = System.currentTimeMillis()

  override def stop(): Unit = {
    stopping = true
    super.stop()
  }

  override protected def checkScale(): Unit = {
    val overrides = configService
      .flatMap(service => Option(service.getSystemConfigFromCache))
      .flatMap(config => Option(config.getConfigs))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    def setting[T](entry: ConfigEntry[T]): T =
      overrides.get(entry.key).map(entry.valueConverter).getOrElse(conf.get(entry))

    val upEnabled = setting(AUTO_SCALE_UP_ENABLED)
    val downEnabled = setting(AUTO_SCALE_DOWN_ENABLED)
    val enabled = upEnabled || downEnabled
    if (!enabled && !statusSystem.getScaleCheckpoint.hasPending) {
      return
    }

    val raft = Option(HAHelper.getRatisServer(statusSystem))
    if (stopping || raft.exists(!_.isLeader)) {
      return
    }
    val leaderVersion = raft.map(_.getLeaderVersion).getOrElse(-1L)
    if (currentLeaderVersion != leaderVersion) {
      currentLeaderVersion = leaderVersion
      statusSystem.invalidateWorkerStats()
      return
    }
    val now = currentTimeMillis
    if (HAHelper.getWorkerTimeoutDeadline(statusSystem) > now) {
      return
    }
    val leaderTerm = raft.map(_.getReadyLeaderTerm)
    def stillLeader(): Boolean =
      !stopping && raft.forall { server =>
        server.getLeaderVersion == leaderVersion && server.isLeader &&
        leaderTerm.contains(server.getReadyLeaderTerm)
      }
    if (leaderTerm.contains(-1L)) {
      return
    }

    // Wait for metrics from every registered worker before planning.
    val registeredWorkers = statusSystem.workersMap.synchronized {
      statusSystem.workersMap.values().asScala.toVector
    }
    val workerStats = registeredWorkers.flatMap { worker =>
      Option(statusSystem.workerStats.get(worker)).flatMap(ref => Option(ref.get()))
        .map(worker -> _)
    }.toMap
    val checkpoint = new Checkpoint(leaderTerm.getOrElse(-1L), () => stillLeader())
    val metricsReady = workerStats.size == registeredWorkers.size
    if (!metricsReady && !checkpoint.current.hasPending) {
      return
    }
    val pendingOperationId = checkpoint.current.getPending.getOperationId
    val result =
      if (pendingOperationId.isEmpty) None else Some(Try(checkOperation(pendingOperationId)))
    val clusterInfo = getCurrentClusterInfo()
    lazy val currentPlan = planScaleOperation(
      clusterInfo,
      registeredWorkers,
      workerStats,
      checkpoint.current.getAccountedExclWorkerCnt)
    if (!stillLeader() ||
      !recoverOperation(
        checkpoint,
        clusterInfo,
        result,
        upEnabled,
        downEnabled,
        if (metricsReady) Some(currentPlan) else None) ||
      !enabled || !clusterInfo.isInRunningStatus || !metricsReady) {
      return
    }
    val (operation, target) = currentPlan
    if (operation.scaleType == PbScaleType.NO_SCALE) {
      if (target.targetSpecWorkerCnt == clusterInfo.specWorkerCnt &&
        target.accountedExclWorkerCnt != checkpoint.current.getAccountedExclWorkerCnt) {
        checkpoint.save(checkpoint.current.toBuilder
          .setWorkloadUid(clusterInfo.workloadUid)
          .setAccountedExclWorkerCnt(target.accountedExclWorkerCnt))
      }
      return
    }
    val window =
      if (operation.scaleType == PbScaleType.SCALE_UP) {
        setting(SCALE_UP_STABILIZATION_WINDOW_INTERVAL)
      } else {
        setting(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL)
      }
    val last = checkpoint.current.getLastOperation
    val inWindow = last.hasScaleOperation &&
      currentTimeMillis - last.getScaleOperation.getLastScaleTime < window
    if (inWindow || !stillLeader()) {
      return
    }

    if (operation.scaleType == PbScaleType.SCALE_UP && upEnabled) {
      if (operation.activateWorkers.nonEmpty) {
        statusSystem.handleWorkerEvent(
          WorkerEventType.Recommission_VALUE,
          operation.activateWorkers.asJava,
          MasterClient.genRequestId())
      }
      clusterMayNeedScale(operation, clusterInfo, target, checkpoint)
    } else if (operation.scaleType == PbScaleType.SCALE_DOWN && downEnabled) {
      if (operation.decommissionWorkers.nonEmpty) {
        statusSystem.handleWorkerEvent(
          WorkerEventType.DecommissionThenIdle_VALUE,
          operation.decommissionWorkers.asJava,
          MasterClient.genRequestId())
      }
      clusterMayNeedScale(operation, clusterInfo, target, checkpoint)
    }
  }

  private class Checkpoint(val term: Long, val stillLeader: () => Boolean) {
    var current: PbScaleCheckpoint = statusSystem.getScaleCheckpoint

    def save(next: PbScaleCheckpoint.Builder): Boolean = {
      if (!stillLeader()) return false
      val revision = current.getRevision
      val updated = next.setRevision(revision + 1).build()
      val saved = statusSystem.handleCompareAndSetScaleCheckpoint(term, revision, updated)
      if (stillLeader() && saved) {
        current = updated
        true
      } else {
        false
      }
    }
  }

  // Reconcile any persisted pending operation before planning a new one; returns true when ready to plan.
  private def recoverOperation(
      checkpoint: Checkpoint,
      cluster: ClusterInfo,
      poll: Option[Try[OperationResult]],
      upEnabled: Boolean,
      downEnabled: Boolean,
      currentPlan: => Option[(ScaleOperation, ScaleTarget)]): Boolean = {
    val state = checkpoint.current
    if (state.getWorkloadUid.nonEmpty && state.getWorkloadUid != cluster.workloadUid) {
      return checkpoint.save(PbScaleCheckpoint.newBuilder().setWorkloadUid(cluster.workloadUid))
    }
    if (!state.hasPending) {
      return true
    }
    val pending = state.getPending
    val result = poll.map(_.get)
    if (result.exists(!_.isFinalStatus)) {
      return false
    }

    val operation = ScaleOperation.fromPb(pending.getOperation)
    val operationId = pending.getOperationId
    val next = state.toBuilder.clearPending()
    if (operationId.nonEmpty) {
      next.setLastOperation(operationRecord(operationId, operation.copy(isFinished = true)))
      if (result.exists(!_.success)) {
        return checkpoint.save(next)
      }
    }
    if (!cluster.isInRunningStatus) {
      return false
    }
    if (cluster.matchesSpec(operation)) {
      next.setLastOperation(operationRecord(operationId, operation.copy(isFinished = true)))
      if (pending.hasAccountedExclWorkerCnt) {
        next.setAccountedExclWorkerCnt(pending.getAccountedExclWorkerCnt)
      }
      checkpoint.save(next)
    } else if (operationId.nonEmpty || pending.getPlatformVersion != cluster.platformVersion) {
      checkpoint.save(next)
    } else {
      // Platform version is unchanged and no operationId was recorded; revalidate before retrying or clearing.
      val enabled = if (operation.scaleType == PbScaleType.SCALE_UP) upEnabled else downEnabled
      val indices = targetIndices(operation)
      if (!enabled) return checkpoint.save(next)
      currentPlan match {
        case None => !safePlatformTarget(indices) && checkpoint.save(next)
        case Some((plan, target)) =>
          val sameTarget = plan.scaleType == operation.scaleType &&
            plan.specWorkerCnt == operation.specWorkerCnt &&
            plan.reservedWorkerIndices.toSet == operation.reservedWorkerIndices.toSet &&
            pending.hasAccountedExclWorkerCnt == (plan.specWorkerCnt == target.targetSpecWorkerCnt) &&
            (!pending.hasAccountedExclWorkerCnt ||
              pending.getAccountedExclWorkerCnt == target.accountedExclWorkerCnt)
          if (sameTarget && safePlatformTarget(indices) && checkpoint.stillLeader()) {
            submitPlatformUpdate(checkpoint, pending, operation, firstAttempt = false)
            false
          } else {
            checkpoint.save(next)
          }
      }
    }
  }

  private def operationRecord(id: String, operation: ScaleOperation): PbScaleOperationRequest =
    PbScaleOperationRequest.newBuilder().setOperationId(id).setScaleOperation(
      operation.toPb).build()

  private def targetIndices(operation: ScaleOperation): Set[Int] =
    ClusterInfo.expectedWorkerIndices(
      operation.specWorkerCnt,
      operation.reservedWorkerIndices.toSet).toSet

  // Every registered managed worker outside the target ordinals must already be Idle before platform removal.
  private def safePlatformTarget(expectedIndices: Set[Int]): Boolean = {
    statusSystem.workersMap.synchronized {
      statusSystem.workersMap.values().asScala.forall { worker =>
        val index = worker.workerIndex
        index < 0 || expectedIndices.contains(index) ||
        worker.getWorkerStatus.getState == PbWorkerStatus.State.Idle
      }
    }
  }

  private def submitPlatformUpdate(
      checkpoint: Checkpoint,
      pending: PbPendingScaleOperation,
      operation: ScaleOperation,
      firstAttempt: Boolean): Unit = {
    val id =
      try {
        doScaleOperation(operation)
      } catch {
        case error: Exception if firstAttempt && isRejectedPlatformUpdate(error) =>
          checkpoint.save(checkpoint.current.toBuilder.clearPending())
          throw error
      }
    checkpoint.save(checkpoint.current.toBuilder
      .setPending(pending.toBuilder.setOperationId(id))
      .setLastOperation(operationRecord(id, operation)))
  }

  // Persist pending intent before calling the platform so a failover leader can confirm or abandon the update.
  private def clusterMayNeedScale(
      operation: ScaleOperation,
      clusterInfo: ClusterInfo,
      target: ScaleTarget,
      checkpoint: Checkpoint): Unit = {
    val expectedIndices = targetIndices(operation)
    val acceptsTarget = operation.specWorkerCnt == target.targetSpecWorkerCnt
    val next = checkpoint.current.toBuilder.setWorkloadUid(clusterInfo.workloadUid)
    val currentIndices = clusterInfo.expectedWorkerIndices.toSet
    if (expectedIndices != currentIndices) {
      if (!safePlatformTarget(expectedIndices)) {
        return
      }
      val pending = PbPendingScaleOperation.newBuilder()
        .setOperation(operation.toPb)
        .setPlatformVersion(clusterInfo.platformVersion)
        .setRemovesWorkerIndices(!currentIndices.subsetOf(expectedIndices))
      if (acceptsTarget) {
        pending.setAccountedExclWorkerCnt(target.accountedExclWorkerCnt)
      }
      if (checkpoint.save(next.setPending(pending))) {
        if (safePlatformTarget(expectedIndices) && checkpoint.stillLeader()) {
          submitPlatformUpdate(checkpoint, pending.build(), operation, firstAttempt = true)
        } else {
          checkpoint.save(checkpoint.current.toBuilder.clearPending())
        }
      }
    } else {
      if (acceptsTarget) {
        next.setAccountedExclWorkerCnt(target.accountedExclWorkerCnt)
      }
      checkpoint.save(next.setLastOperation(operationRecord("", operation.copy(isFinished = true))))
    }
  }

  private def planScaleOperation(
      clusterInfo: ClusterInfo,
      registeredWorkers: Seq[WorkerInfo],
      workerStats: Map[WorkerInfo, WorkerStats],
      accountedExclWorkerCnt: Int): (ScaleOperation, ScaleTarget) = {
    // Plan against registrations that belong to the current platform spec.
    val expectedIndices = clusterInfo.expectedWorkerIndices
    val workers =
      registeredWorkers.filter(worker => expectedIndices.contains(worker.workerIndex))
    val idle = workers.filter(_.getWorkerStatus.getState == PbWorkerStatus.State.Idle)
    val draining = workers.filter { worker =>
      !idle.contains(worker) && Option(statusSystem.workerEventInfos.get(worker))
        .exists(_.getEventType == WorkerEventType.DecommissionThenIdle)
    }
    val normal = workers.filterNot(worker => idle.contains(worker) || draining.contains(worker))
    val exclWorkerCnt = normal.count { worker =>
      val state = worker.getWorkerStatus.getState
      val recommissioning = state == PbWorkerStatus.State.InDecommissionThenIdle &&
        Option(statusSystem.workerEventInfos.get(worker)).exists(
          _.getEventType == WorkerEventType.Recommission)
      statusSystem.excludedWorkers.contains(worker) ||
      statusSystem.manuallyExcludedWorkers.contains(worker) ||
      (state != PbWorkerStatus.State.Normal && !recommissioning)
    }
    val unaccountedExclWorkerCnt = math.max(0, exclWorkerCnt - accountedExclWorkerCnt)
    val oldSpecWorkerCnt = clusterInfo.specWorkerCnt
    val target = calculateScaleTarget(
      workers.size,
      oldSpecWorkerCnt,
      normal.map(workerStats),
      exclWorkerCnt,
      unaccountedExclWorkerCnt)
    val targetSpecWorkerCnt = target.targetSpecWorkerCnt

    val decommissionCnt = workers.size - targetSpecWorkerCnt
    val allDraining = draining ++ idle
    // Recommission workers when fewer retirements are needed.
    val activate = allDraining.take(math.max(0, allDraining.size - decommissionCnt))
    val decommission =
      if (activate.nonEmpty || decommissionCnt <= 0) {
        Seq.empty[WorkerInfo]
      } else {
        normal.sortBy(_.workerIndex).reverse
          .take(math.max(0, decommissionCnt - allDraining.size))
      }
    val removable =
      if (activate.nonEmpty || decommissionCnt <= 0) Seq.empty[WorkerInfo] else idle
    // Keep draining workers in the platform spec until they report Idle.
    val pendingDrainCnt = allDraining.size - activate.size + decommission.size - removable.size
    val scaleDownSpecWorkerCnt =
      math.min(oldSpecWorkerCnt, targetSpecWorkerCnt + pendingDrainCnt)
    val currentReserved = clusterInfo.reservedWorkerIndices
      .filter(index => expectedIndices.lastOption.exists(index < _))
    val reserved = (currentReserved ++ removable.map(_.workerIndex)).distinct.sorted

    val kind =
      if (targetSpecWorkerCnt > oldSpecWorkerCnt) {
        ScaleOpKind.ExpandSpec
      } else if (activate.nonEmpty) {
        ScaleOpKind.RecommissionWorkers
      } else if (removable.nonEmpty || decommission.nonEmpty ||
        scaleDownSpecWorkerCnt < oldSpecWorkerCnt) {
        ScaleOpKind.ScaleDown
      } else {
        ScaleOpKind.KeepSpec
      }

    val (specWorkerCnt, reservedWorkerIndices) = kind match {
      case ScaleOpKind.ExpandSpec =>
        val addedReplicaCnt = targetSpecWorkerCnt - oldSpecWorkerCnt
        val newReserved = reserved.dropRight(math.min(addedReplicaCnt, reserved.size))
        (targetSpecWorkerCnt, newReserved)
      case ScaleOpKind.RecommissionWorkers if targetSpecWorkerCnt < oldSpecWorkerCnt =>
        val newSpecWorkerCnt =
          math.min(
            oldSpecWorkerCnt,
            math.max(targetSpecWorkerCnt, workers.map(_.workerIndex).distinct.size))
        val newReserved =
          scaleDownReservedIndices(newSpecWorkerCnt, expectedIndices, reserved, workers)
        (newSpecWorkerCnt, newReserved)
      case ScaleOpKind.RecommissionWorkers =>
        (oldSpecWorkerCnt, reserved)
      case ScaleOpKind.ScaleDown =>
        val newReserved =
          scaleDownReservedIndices(scaleDownSpecWorkerCnt, expectedIndices, reserved, workers)
        (scaleDownSpecWorkerCnt, newReserved)
      case ScaleOpKind.KeepSpec =>
        (expectedIndices.size, reserved)
    }
    val scaleType = kind match {
      case ScaleOpKind.ExpandSpec | ScaleOpKind.RecommissionWorkers => PbScaleType.SCALE_UP
      case ScaleOpKind.ScaleDown => PbScaleType.SCALE_DOWN
      case ScaleOpKind.KeepSpec => PbScaleType.NO_SCALE
    }

    val operation = ScaleOperation(
      specWorkerCnt,
      reservedWorkerIndices,
      activate,
      decommission,
      scaleType,
      currentTimeMillis)
    (operation, target)
  }

  // Retain registered ordinals not selected for removal and encode lower-ordinal gaps as reserved indices.
  private def scaleDownReservedIndices(
      specWorkerCnt: Int,
      expectedIndices: Seq[Int],
      removedIndices: Seq[Int],
      workers: Seq[WorkerInfo]): Seq[Int] = {
    val registeredIndices = workers.map(_.workerIndex).toSet
    val retained = mutable.TreeSet.empty[Int] ++ expectedIndices.filter { index =>
      registeredIndices(index) && !removedIndices.contains(index)
    }
    expectedIndices.iterator.filterNot(removedIndices.contains)
      .takeWhile(_ => retained.size < specWorkerCnt).foreach(retained.add)
    (0 until retained.lastOption.map(_ + 1).getOrElse(0)).filterNot(retained).toVector
  }
}

sealed private trait ScaleOpKind

private object ScaleOpKind {
  case object ExpandSpec extends ScaleOpKind
  case object RecommissionWorkers extends ScaleOpKind
  case object ScaleDown extends ScaleOpKind
  case object KeepSpec extends ScaleOpKind
}
