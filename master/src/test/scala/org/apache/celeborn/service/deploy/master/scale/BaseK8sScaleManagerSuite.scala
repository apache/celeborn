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

import java.io.IOException
import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.network.CelebornRackResolver
import org.apache.celeborn.common.protocol.{PbScaleType, PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.service.deploy.master.ApplicationMetricsSource
import org.apache.celeborn.service.deploy.master.clustermeta.SingleMasterMetaManager
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAMasterMetaManager, HARaftServer}

class BaseK8sScaleManagerSuite extends AnyFunSuite {
  import PbWorkerStatus.State

  test("submits a scale-up operation with its creation time and waits for a final result") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2)
      f.manager.tick()
      val created = f.meta.lastScaleOperation.getRight.lastScaleTime
      assert(!f.meta.lastScaleOperation.getRight.isFinished)
      assert(f.meta.lastScaleOperation.getRight.scaleType == PbScaleType.SCALE_UP)
      f.manager.result = OperationResult(false, false)
      f.manager.now += 10000
      f.manager.tick()
      assert(f.manager.operations.size == 1)
      f.manager.result = OperationResult(true, true)
      f.manager.tick()
      assert(f.meta.lastScaleOperation.getRight.isFinished)
      assert(f.meta.lastScaleOperation.getRight.lastScaleTime == created)
    }
  }

  test("a failed final platform result finishes the old operation and permits the next plan") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2)
      f.manager.tick()
      f.manager.result = OperationResult(false, true)
      f.manager.target = 6
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(5, 6))
    }
  }

  test("drain-only plans are recorded as finished and Idle removal waits for the down window") {
    withManager(5, 3) { f =>
      f.register(0, 1, 2, 3, 4)
      f.conf.set(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key, "60s")
      f.manager.tick()
      assert(f.event(3).contains(WorkerEventType.DecommissionThenIdle))
      assert(f.event(4).contains(WorkerEventType.DecommissionThenIdle))
      assert(f.manager.operations.isEmpty)
      assert(f.meta.lastScaleOperation.getLeft.isEmpty)
      assert(f.meta.lastScaleOperation.getRight.isFinished)
      assert(f.meta.lastScaleOperation.getRight.lastScaleTime == f.manager.now)
      f.heartbeat(4, State.Idle)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      f.manager.now += 60000
      f.manager.tick()
      assert(f.manager.operations.last.specWorkerCnt == 4)
      f.heartbeat(3, State.Idle)
      f.manager.now += 60000
      f.manager.tick()
      assert(f.manager.operations.last.specWorkerCnt == 3)
    }
  }

  test("does not increase platform replicas while draining overlapping Worker registrations") {
    withManager(5, 5) { f =>
      f.register(0, 1, 2, 3, 4)
      f.meta.updateRegisterWorkerMeta(
        "workers-0",
        1,
        5,
        6,
        7,
        -1,
        null,
        Collections.emptyMap[String, DiskInfo]())
      f.publishMetrics()
      assert(f.meta.workersMap.size() == 6)
      f.manager.tick()
      assert(f.event(4).contains(WorkerEventType.DecommissionThenIdle))
      assert(f.manager.operations.isEmpty)
      assert(f.meta.lastScaleOperation.getRight.specWorkerCnt == 5)
    }
  }

  test("the up window starts at plan creation rather than platform completion") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2)
      f.conf.set(SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key, "60s")
      f.manager.tick()
      f.manager.now += 30000
      f.manager.tick()
      f.manager.target = 6
      f.manager.now += 30000
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(5, 6))
    }
  }

  test("the next plan direction selects the stabilization window") {
    withManager(3, 2) { f =>
      f.register(0, 1, 2)
      f.conf.set(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key, "60s")
      f.meta.handleUpdateScaleOperation(
        "",
        ScaleOperation(
          3,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          PbScaleType.SCALE_UP,
          f.manager.now - 10000,
          true))
      f.manager.tick()
      assert(f.meta.workerEventInfos.isEmpty)
      f.manager.now += 60000
      f.manager.tick()
      assert(f.event(2).contains(WorkerEventType.DecommissionThenIdle))
    }
  }

  test("retains registered high ordinals while cancelling pending capacity") {
    withManager(10, 4) { f =>
      f.register(0, 1, 8, 9)
      f.manager.tick()
      val operation = f.manager.operations.last
      assert(operation.specWorkerCnt == 4)
      assert(operation.reservedWorkerIndices == (2 until 8))
      assert(f.meta.workerEventInfos.isEmpty)
    }
  }

  test("reserves completed low ordinals without deleting retained registered workers") {
    withManager(5, 3) { f =>
      f.register(0, 1, 2, 3, 4)
      f.heartbeat(1, State.Idle)
      f.heartbeat(3, State.Idle)
      f.manager.tick()
      assert(f.manager.operations.last.specWorkerCnt == 3)
      assert(f.manager.operations.last.reservedWorkerIndices == Seq(1, 3))
    }
  }

  test("recommissions draining and Idle workers when demand returns") {
    withManager(5, 3) { f =>
      f.register(0, 1, 2, 3, 4)
      f.manager.tick()
      f.meta.handleReportWorkerDecommission(
        Seq(f.worker(3), f.worker(4)).asJava,
        MasterClient.genRequestId())
      f.heartbeat(3, State.InDecommissionThenIdle)
      f.heartbeat(4, State.Idle)
      f.manager.target = 5
      f.manager.tick()
      assert(f.event(3).contains(WorkerEventType.Recommission))
      assert(f.event(4).contains(WorkerEventType.Recommission))
      assert(f.manager.operations.isEmpty)
      f.heartbeat(3, State.Normal)
      f.heartbeat(4, State.Normal)
      assert(!f.meta.decommissionWorkers.contains(f.worker(3)))
      assert(!f.meta.decommissionWorkers.contains(f.worker(4)))
    }
  }

  test("Idle workers do not require an autoscaler ownership checkpoint to be recommissioned") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      f.heartbeat(1, State.Idle)
      f.manager.tick()
      assert(f.event(1).contains(WorkerEventType.Recommission))
      assert(f.meta.lastScaleOperation.getRight.scaleType == PbScaleType.SCALE_UP)
    }
  }

  test("recommission can accompany a reduction in unregistered platform capacity") {
    withManager(10, 3) { f =>
      f.register(0, 1, 8, 9)
      f.heartbeat(8, State.Idle)
      f.heartbeat(9, State.Idle)
      f.conf.set(AUTO_SCALE_DOWN_ENABLED, false)
      f.manager.tick()
      assert(f.manager.operations.last.specWorkerCnt == 4)
      assert(f.manager.operations.last.scaleType == PbScaleType.SCALE_UP)
      assert(f.event(8).contains(WorkerEventType.Recommission) ||
        f.event(9).contains(WorkerEventType.Recommission))
      assert(ClusterInfo(4, f.manager.operations.last.reservedWorkerIndices, true)
        .expectedWorkerIndices == Seq(0, 1, 8, 9))
    }
  }

  test("compensates changes in exclusion count rather than changes in worker identity") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(1))
      f.manager.tick()
      f.meta.manuallyExcludedWorkers.remove(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(2))
      f.manager.tick()
      f.meta.manuallyExcludedWorkers.remove(f.worker(1))
      f.manager.tick()
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.manager.tick()
      assert(f.manager.exclusionDeltas.toSeq == Seq(2, 0, 0, 1))
      assert(f.meta.lastScaleOperation == null)
    }
  }

  test("retries an excluded replacement that did not fit in an accepted target") {
    withManager(10, 11) { f =>
      f.register(0 until 10: _*)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(1))
      f.manager.accountedExclusions = Some(1)

      f.conf.set(AUTO_SCALE_UP_ENABLED, false)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      f.conf.set(AUTO_SCALE_UP_ENABLED, true)
      f.manager.beforePlatformUpdate = () => throw new IOException("update rejected")
      intercept[IOException](f.manager.tick())
      assert(f.manager.cluster.specWorkerCnt == 10)

      f.manager.beforePlatformUpdate = () => ()
      f.manager.tick()
      assert(f.manager.cluster.specWorkerCnt == 11)
      f.manager.tick()
      assert(f.manager.operations.size == 1)

      f.manager.target = 12
      f.manager.accountedExclusions = Some(2)
      f.manager.tick()
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(11, 12))
      assert(f.manager.exclusionDeltas.toSeq == Seq(2, 2, 2, 1, 1, 0))
    }
  }

  test("confirms an excluded replacement from the platform after losing the update response") {
    withManager(10, 11) { f =>
      f.register(0 until 10: _*)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(1))
      f.manager.accountedExclusions = Some(1)

      f.manager.afterPlatformUpdate = () => throw new IOException("update response lost")
      intercept[IOException](f.manager.tick())
      assert(f.manager.cluster.specWorkerCnt == 11)

      f.manager.onRead = () => throw new IOException("read unavailable")
      intercept[IOException](f.manager.tick())
      f.manager.onRead = () => ()
      f.manager.afterPlatformUpdate = () => ()
      f.manager.target = 12
      f.manager.accountedExclusions = Some(2)
      f.manager.tick()
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(11, 12))
      assert(f.manager.exclusionDeltas.toSeq == Seq(2, 1, 0))
    }
  }

  test("accounts for replacements already covered by the observed target") {
    withManager(10, 10) { f =>
      f.register(0 until 5: _*)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(1))

      f.manager.tick()
      assert(f.manager.operations.isEmpty)

      f.publishMetrics()
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      assert(f.manager.exclusionDeltas.toSeq == Seq(2, 0))
    }
  }

  test("non-Normal workers count as excluded even when absent from both exclusion sets") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      f.heartbeat(1, State.InGraceFul)
      f.meta.excludedWorkers.clear()
      f.meta.manuallyExcludedWorkers.clear()
      f.manager.tick()
      assert(f.manager.newExclusions == 1)
    }
  }

  test("the stabilization window uses the time after reading the platform") {
    withManager(3, 2) { f =>
      f.register(0, 1, 2)
      f.conf.set(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key, "60s")
      f.meta.handleUpdateScaleOperation(
        "",
        ScaleOperation(
          3,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          PbScaleType.SCALE_UP,
          f.manager.now - 59000,
          true))
      f.manager.onRead = () => f.manager.now += 2000
      f.manager.tick()
      assert(f.event(2).contains(WorkerEventType.DecommissionThenIdle))
    }
  }

  test("leader changes clear observations and reset the local exclusion count") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      f.conf.set("celeborn.master.ha.heartbeat.batch.enabled", "false")
      val meta = new HAMasterMetaManager(mock[RpcEnv], f.conf, f.resolver, f.metrics)
      meta.workersMap.putAll(f.meta.workersMap)
      meta.workerStats.putAll(f.meta.workerStats)
      meta.manuallyExcludedWorkers.add(f.worker(1))
      val raft = mock[HARaftServer]
      meta.setRatisServer(raft)
      when(raft.isLeader).thenReturn(true)
      when(raft.getLeaderVersion).thenReturn(1L)
      f.manager.init(meta)
      f.manager.tick()
      assert(meta.workerStats.values().asScala.forall(_.get() == null))
      f.publishMetrics(meta)
      f.manager.tick()
      f.manager.tick()
      when(raft.getLeaderVersion).thenReturn(2L)
      f.manager.tick()
      assert(meta.workerStats.values().asScala.forall(_.get() == null))
      f.publishMetrics(meta)
      f.manager.tick()
      assert(f.manager.exclusionDeltas.toSeq == Seq(1, 0, 1))
    }
  }

  test("waits for all registered metrics, followers and registration warmup") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2, 9)
      f.meta.workerStats.get(f.worker(9)).set(null)
      f.manager.tick()
      assert(f.manager.reads == 0)
      val follower = mock[HAMasterMetaManager]
      val raft = mock[HARaftServer]
      when(follower.getRatisServer).thenReturn(raft)
      when(raft.isLeader).thenReturn(false)
      f.manager.init(follower)
      f.manager.tick()
      f.manager.tick()
      assert(f.manager.reads == 0)
      when(raft.isLeader).thenReturn(true)
      when(raft.getWorkerTimeoutDeadline).thenReturn(f.manager.now + 10000)
      f.manager.tick()
      assert(f.manager.reads == 0)
    }
  }

  test("checks fresh registrations before submitting a platform change") {
    withManager(3, 1) { f =>
      f.register(0, 1)
      f.manager.onCalculate = () => f.register(2)
      f.manager.tick()
      assert(f.event(1).contains(WorkerEventType.DecommissionThenIdle))
      assert(f.manager.operations.isEmpty)
    }
  }

  test("blocks deletion of a non-Idle registration outside the current platform view") {
    withManager(3, 2) { f =>
      f.register(0, 1, 2, 3)
      f.manager.tick()
      f.heartbeat(2, State.Idle)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
    }
  }

  test("ignores reserved indices beyond the effective platform range") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      f.manager.cluster = ClusterInfo(3, Seq(100), true)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      assert(f.meta.lastScaleOperation == null)
    }
  }

  test("a replacement manager resumes the last operation after fresh worker observations") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2)
      f.manager.tick()
      val replacement = new TestScaleManager(f.conf, f.manager.cluster, 5)
      replacement.init(f.meta)
      try {
        replacement.tick()
        f.publishMetrics()
        replacement.result = OperationResult(false, false)
        replacement.tick()
        assert(replacement.polls == 1)
        assert(replacement.operations.isEmpty)
        replacement.result = OperationResult(true, true)
        replacement.tick()
        assert(f.meta.lastScaleOperation.getRight.isFinished)
      } finally {
        replacement.stop()
      }
    }
  }

  test("keeps one thousand replicas until the selected quarter finishes draining") {
    withManager(1000, 750) { f =>
      f.register((0 until 1000): _*)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      assert((0 until 750).forall(index => f.event(index).isEmpty))
      assert((750 until 1000).forall(index =>
        f.event(index).contains(WorkerEventType.DecommissionThenIdle)))
      (750 until 1000).foreach(f.heartbeat(_, State.Idle))
      f.manager.tick()
      assert(f.manager.cluster.expectedWorkerIndices == (0 until 750))
      assert(f.manager.operations.last.specWorkerCnt == 750)
      assert((0 until 750).forall(index =>
        f.worker(index).getWorkerStatus.getState == State.Normal))
    }
  }

  test("reconciles repeated sparse-ordinal changes across one thousand logical workers") {
    withManager(1000, 1000) { f =>
      f.manager.cluster = ClusterInfo(1000, Seq(1, 7, 63, 511), true)
      f.register(f.manager.cluster.expectedWorkerIndices: _*)
      val random = new scala.util.Random(13L)
      val targets = Seq(400, 800, 1, 1000) ++ Seq.fill(40)(1 + random.nextInt(1000))
      targets.foreach { target =>
        val oldIndices = f.manager.cluster.expectedWorkerIndices.toSet
        f.manager.target = target
        f.manager.tick()
        if (target < oldIndices.size) {
          assert(f.manager.cluster.expectedWorkerIndices.toSet == oldIndices)
          val draining = oldIndices.filter(index =>
            f.event(index).contains(WorkerEventType.DecommissionThenIdle))
          assert(draining.size == oldIndices.size - target)
          draining.foreach(f.heartbeat(_, State.Idle))
          f.manager.tick()
          val removed = oldIndices -- f.manager.cluster.expectedWorkerIndices.toSet
          assert(removed == draining)
        } else {
          assert(oldIndices.subsetOf(f.manager.cluster.expectedWorkerIndices.toSet))
        }
        val newIndices = f.manager.cluster.expectedWorkerIndices.toSet
        assert(newIndices.size == target)
        (oldIndices -- newIndices).foreach(index =>
          f.meta.updateWorkerLostMeta(s"workers-$index", 1, 2, 3, 4))
        f.register((newIndices -- oldIndices).toSeq: _*)
        f.publishMetrics()
        f.manager.tick()
        assert(f.meta.workersMap.size() == target)
        assert(newIndices.forall(index =>
          f.worker(index).getWorkerStatus.getState == State.Normal))
      }
    }
  }

  private def withManager(spec: Int, target: Int)(testBody: Fixture => Unit): Unit = {
    val fixture = new Fixture(spec, target)
    try {
      testBody(fixture)
    } finally {
      fixture.manager.stop()
      fixture.metrics.destroy()
      assert(fixture.metrics.metricsCleaner.awaitTermination(5, TimeUnit.SECONDS))
      fixture.resolver.stop()
    }
  }

  private class Fixture(spec: Int, target: Int) {
    val conf = new CelebornConf(false)
      .set(AUTO_SCALE_UP_ENABLED.key, "true")
      .set(AUTO_SCALE_DOWN_ENABLED.key, "true")
      .set(SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key, "0s")
      .set(SCALE_DOWN_STABILIZATION_WINDOW_INTERVAL.key, "0s")
    val resolver = new CelebornRackResolver(conf)
    val metrics = new ApplicationMetricsSource(conf)
    val meta = new SingleMasterMetaManager(mock[RpcEnv], conf, resolver, metrics)
    val manager = new TestScaleManager(conf, ClusterInfo(spec, Seq.empty, true), target)
    manager.init(meta)
    manager.tick()

    def register(indices: Int*): Unit = indices.foreach { index =>
      meta.updateRegisterWorkerMeta(
        s"workers-$index",
        1,
        2,
        3,
        4,
        -1,
        null,
        Collections.emptyMap[String, DiskInfo]())
      meta.prepareWorkerStatsUpdate(worker(index)).set(WorkerStats(Map(
        WorkerStats.MemoryPressureRatio -> 0.4,
        WorkerStats.LastMinuteSystemLoadRatio -> 0.4,
        WorkerStats.DiskUsedRatio -> 0.4)))
    }

    def publishMetrics(
        metaManager: org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager =
          meta): Unit = {
      metaManager.workerStats.values().asScala.foreach(_.set(WorkerStats(Map(
        WorkerStats.MemoryPressureRatio -> 0.4,
        WorkerStats.LastMinuteSystemLoadRatio -> 0.4,
        WorkerStats.DiskUsedRatio -> 0.4))))
    }

    def worker(index: Int): WorkerInfo =
      meta.workersMap.get(new WorkerInfo(s"workers-$index", 1, 2, 3, 4).toUniqueId)

    def event(index: Int): Option[WorkerEventType] =
      Option(meta.workerEventInfos.get(worker(index))).map(_.getEventType)

    def heartbeat(index: Int, state: State): Unit = {
      val w = worker(index)
      meta.updateWorkerHeartbeatMeta(
        w.host,
        w.rpcPort,
        w.pushPort,
        w.fetchPort,
        w.replicatePort,
        Collections.emptyMap[String, DiskInfo](),
        manager.now,
        new WorkerStatus(state.getNumber, manager.now),
        false)
    }
  }

  private class TestScaleManager(
      conf: CelebornConf,
      var cluster: ClusterInfo,
      var target: Int) extends BaseK8sScaleManager(conf) {
    val operations = new ArrayBuffer[ScaleOperation]
    var now = 1000000L
    var reads = 0
    var polls = 0
    var newExclusions = 0
    val exclusionDeltas = new ArrayBuffer[Int]
    var onCalculate: () => Unit = () => ()
    var onRead: () => Unit = () => ()
    var beforePlatformUpdate: () => Unit = () => ()
    var afterPlatformUpdate: () => Unit = () => ()
    var accountedExclusions = Option.empty[Int]
    var result = OperationResult(true, true)

    def tick(): Unit = checkScale()
    override protected def currentTimeMillis: Long = now
    override protected def getCurrentClusterInfo(): ClusterInfo = {
      reads += 1
      onRead()
      cluster
    }
    override protected def getWorkerIndex(worker: WorkerInfo): Int =
      worker.host.stripPrefix("workers-").toInt
    override protected def doScaleOperation(operation: ScaleOperation): String = {
      beforePlatformUpdate()
      operations += operation
      cluster = ClusterInfo(operation.specWorkerCnt, operation.reservedWorkerIndices, true)
      afterPlatformUpdate()
      operations.size.toString
    }
    override protected def checkOperation(operationId: String): OperationResult = {
      polls += 1
      result
    }
    override protected def calculateSpecWorkerCnt(
        registeredWorkerCnt: Int,
        specWorkerCnt: Int,
        workerStats: Seq[WorkerStats],
        excludedWorkerCnt: Int,
        newExcludedWorkerCnt: Int): Int = {
      newExclusions = newExcludedWorkerCnt
      exclusionDeltas += newExcludedWorkerCnt
      onCalculate()
      target
    }

    override protected def calculateScaleTarget(
        registeredWorkerCnt: Int,
        specWorkerCnt: Int,
        workerStats: Seq[WorkerStats],
        excludedWorkerCnt: Int,
        unaccountedExcludedWorkerCnt: Int): ScaleTarget = {
      val result = super.calculateScaleTarget(
        registeredWorkerCnt,
        specWorkerCnt,
        workerStats,
        excludedWorkerCnt,
        unaccountedExcludedWorkerCnt)
      result.copy(accountedExcludedWorkerCnt = accountedExclusions.getOrElse(excludedWorkerCnt))
    }
  }
}
