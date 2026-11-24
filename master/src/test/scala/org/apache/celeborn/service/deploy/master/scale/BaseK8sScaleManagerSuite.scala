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
import java.nio.file.AccessDeniedException
import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.mockito.ArgumentMatchers
import org.mockito.MockitoSugar._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.{DiskInfo, WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.network.CelebornRackResolver
import org.apache.celeborn.common.protocol.{PbMetaRequest, PbScaleOperationRequest, PbScaleType, PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.common.rpc.RpcEnv
import org.apache.celeborn.service.deploy.master.ApplicationMetricsSource
import org.apache.celeborn.service.deploy.master.clustermeta.{AbstractMetaManager, SingleMasterMetaManager}
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.{ResourceRequest, ResourceResponse}
import org.apache.celeborn.service.deploy.master.clustermeta.ha.{HAMasterMetaManager, HARaftServer, MetaHandler}

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
      f.registerIn(f.meta, new WorkerInfo("workers-0", 1, 5, 6, 7), 0)
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
      f.recordOperation(
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
      assert(f.manager.cluster.expectedWorkerIndices == Seq(0, 1, 8, 9))
    }
  }

  test("compensates changes in exclusion count rather than changes in worker identity") {
    withManager(3, 3) { f =>
      f.register(0, 1, 2)
      val previous = ScaleOperation(
        3,
        Seq.empty,
        Seq.empty,
        Seq.empty,
        PbScaleType.SCALE_UP,
        f.manager.now - 10000,
        isFinished = true)
      f.recordOperation("previous", previous)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(1))
      f.manager.tick()
      assert(f.meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 2)
      f.meta.manuallyExcludedWorkers.remove(f.worker(0))
      f.meta.manuallyExcludedWorkers.add(f.worker(2))
      f.manager.tick()
      f.meta.manuallyExcludedWorkers.remove(f.worker(1))
      f.manager.tick()
      assert(f.meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.manager.tick()
      assert(f.meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 2)
      assert(f.manager.exclusionDeltas.toSeq == Seq(2, 0, 0, 1))
      assert(f.meta.lastScaleOperation.getRight == previous)
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
      f.manager.beforePlatformUpdate = () => throw new AccessDeniedException("update rejected")
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
      f.recordOperation(
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

  Seq(true, false).foreach { previouslyCompensated =>
    test(s"recovers exclusion compensation on a new leader, previously compensated=$previouslyCompensated") {
      withManager(3, 4) { f =>
        f.useCalculator(f.manager)
        f.conf.set(SCALE_WORKER_NUMBER_MIN, if (previouslyCompensated) 1 else 4)
        f.register(0, 1, 2)
        val excluded = if (previouslyCompensated) Seq(1) else Seq.empty
        val (meta, raft) = f.startHa(excluded: _*)
        f.manager.tick()
        assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(4))

        f.register(3)
        meta.workersMap.putAll(f.meta.workersMap)
        meta.workerStats.putAll(f.meta.workerStats)
        f.publishMetrics(meta)
        f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
        f.manager.tick()
        assert(f.manager.cluster.specWorkerCnt == 4)
        assert(meta.lastScaleOperation.getRight.isFinished)
        if (!previouslyCompensated) meta.manuallyExcludedWorkers.add(f.worker(1))

        f.withNewLeader(meta, raft) { replacement =>
          replacement.tick()
          assert(replacement.cluster.specWorkerCnt == (if (previouslyCompensated) 4 else 5))
          if (previouslyCompensated) {
            assert(replacement.operations.isEmpty)
            meta.manuallyExcludedWorkers.add(f.worker(2))
            replacement.tick()
          }
          assert(replacement.operations.map(_.specWorkerCnt).toSeq == Seq(5))
        }
      }
    }
  }

  test("recovers the uncompensated part of a capped target after failover") {
    withManager(4, 5) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1).set(SCALE_WORKER_NUMBER_MAX, 5)
      f.register(0, 1, 2, 3)
      val (meta, raft) = f.startHa(0, 1)
      f.manager.tick()
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(5))
      assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)

      f.withNewLeader(meta, raft) { next =>
        next.tick()
        assert(next.operations.isEmpty)
        f.conf.set(SCALE_WORKER_NUMBER_MAX, 8)
        next.tick()
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(6))
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 2)
      }
    }
  }

  Seq(true, false).foreach { applied =>
    test(s"reconciles a pending replacement after losing leadership, write applied=$applied") {
      withManager(3, 4) { f =>
        f.useCalculator(f.manager)
        f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
        f.register(0, 1, 2)
        val (meta, raft) = f.startHa(1)
        val loseResponse = () => {
          when(raft.isLeader).thenReturn(false)
          throw new IOException("leadership lost while updating replicas")
        }
        if (applied) f.manager.afterPlatformUpdate = loseResponse
        else f.manager.beforePlatformUpdate = loseResponse
        intercept[IOException](f.manager.tick())
        assert(meta.getScaleCheckpoint.hasPending)
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 0)

        f.withNewLeader(meta, raft) { next =>
          next.tick()
          next.tick()
          assert((f.manager.operations ++ next.operations).map(_.specWorkerCnt).toSeq == Seq(4))
          assert(!meta.getScaleCheckpoint.hasPending)
          assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
        }
      }
    }
  }

  test("observes the platform after the operation poll applies a pending replacement") {
    withManager(3, 4) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa(1)
      var applyAccepted = Option.empty[() => Unit]
      f.manager.applyPlatformUpdate = (expected, operation) =>
        applyAccepted = Some(() => f.manager.platform.update(expected, operation))
      f.manager.tick()
      assert(f.manager.cluster.specWorkerCnt == 3)
      assert(meta.getScaleCheckpoint.getPending.getOperationId.nonEmpty)

      f.withNewLeader(meta, raft) { next =>
        next.onPoll = applyAccepted.get
        next.beforePlatformUpdate =
          () => fail("attempted a second update after the first completed")
        next.tick()
        assert(next.polls == 1)
        assert(next.cluster.specWorkerCnt == 4)
        assert(!meta.getScaleCheckpoint.hasPending)
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
      }
    }
  }

  test("a recreated workload starts without its predecessor's confirmed compensation") {
    withManager(3, 4) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa(1)
      f.manager.tick()
      f.manager.tick()
      assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
      assert(f.manager.cluster.specWorkerCnt == 4)

      f.manager.cluster =
        f.manager.cluster.copy(workloadUid = "replacement", platformVersion = "20")
      f.withNewLeader(meta, raft) { next =>
        next.tick()
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(5))
        assert(meta.getScaleCheckpoint.getWorkloadUid == "replacement")
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
      }
    }
  }

  test("matching replicas with different reserved ordinals do not confirm a pending target") {
    withManager(3, 4) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa(1)
      f.manager.afterPlatformUpdate = () => throw new IOException("response lost")
      intercept[IOException](f.manager.tick())
      assert(meta.getScaleCheckpoint.hasPending)
      assert(f.manager.cluster.specWorkerCnt == 4)

      f.manager.cluster =
        f.manager.cluster.copy(reservedWorkerIndices = Seq(3), platformVersion = "20")
      f.withNewLeader(meta, raft) { next =>
        next.tick()
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(5))
        assert(!meta.getScaleCheckpoint.hasPending)
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 1)
      }
    }
  }

  test("a late platform write cannot overwrite the replacement leader's confirmed target") {
    withManager(3, 4) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa(1)
      f.manager.beforePlatformUpdate = () =>
        f.withNewLeader(meta, raft) { next =>
          next.tick()
          f.register(3)
          meta.workersMap.putAll(f.meta.workersMap)
          meta.workerStats.putAll(f.meta.workerStats)
          f.publishMetrics(meta)
          meta.manuallyExcludedWorkers.add(f.worker(2))
          next.tick()
          next.tick()
          assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(4, 5))
        }
      intercept[AccessDeniedException](f.manager.tick())
      assert(f.manager.platform.conflicts == 1)
      assert(f.manager.operations.isEmpty)
      assert(f.manager.cluster.specWorkerCnt == 5)
      assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 2)
      assert(!meta.getScaleCheckpoint.hasPending)
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

  test("does not submit a plan after losing leadership during the platform read") {
    withManager(3, 4) { f =>
      f.register(0, 1, 2)
      f.conf.set("celeborn.master.ha.heartbeat.batch.enabled", "false")
      val meta = new HAMasterMetaManager(mock[RpcEnv], f.conf, f.resolver, f.metrics)
      val raft = mock[HARaftServer]
      meta.setRatisServer(raft)
      meta.workersMap.putAll(f.meta.workersMap)
      meta.workerStats.putAll(f.meta.workerStats)
      when(raft.isLeader).thenReturn(true)
      when(raft.getLeaderVersion).thenReturn(1L)
      f.manager.init(meta)
      f.manager.tick()
      f.publishMetrics(meta)
      f.manager.onRead = () => when(raft.isLeader).thenReturn(false)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)

      when(raft.isLeader).thenReturn(true)
      f.manager.onRead = () => when(raft.getReadyLeaderTerm).thenReturn(2L)
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
    }
  }

  test("retries a replacement if the submitted operation fails without changing the spec") {
    withManager(3, 4) { f =>
      f.register(0, 1, 2)
      f.meta.manuallyExcludedWorkers.add(f.worker(0))
      f.manager.onCalculate = () => {
        f.manager.target = f.manager.cluster.specWorkerCnt + f.manager.newExclusions
      }
      f.manager.tick()
      f.manager.result = OperationResult(false, true)
      f.manager.cluster = f.manager.cluster.copy(specWorkerCnt = 3)
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(4, 4))
      assert(f.manager.exclusionDeltas.toSeq == Seq(1, 1))
    }
  }

  test("uses the original stabilization time when the platform response is lost") {
    withManager(3, 4) { f =>
      f.conf.set(SCALE_UP_STABILIZATION_WINDOW_INTERVAL.key, "60s")
      f.register(0, 1, 2)
      val created = f.manager.now
      f.manager.afterPlatformUpdate = () => throw new IOException("response lost")
      intercept[IOException](f.manager.tick())
      f.manager.afterPlatformUpdate = () => ()
      f.register(3)
      f.manager.target = 5
      f.manager.now += 1000
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(4))
      assert(f.meta.lastScaleOperation.getRight.lastScaleTime == created)
      f.manager.now = created + 60000
      f.manager.tick()
      assert(f.manager.operations.map(_.specWorkerCnt).toSeq == Seq(4, 5))
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
      f.manager.cluster = f.manager.cluster.copy(reservedWorkerIndices = Seq(100))
      f.manager.tick()
      assert(f.manager.operations.isEmpty)
      assert(f.meta.lastScaleOperation == null)
    }
  }

  test("a replacement manager resumes the last operation after fresh worker observations") {
    withManager(3, 5) { f =>
      f.register(0, 1, 2)
      f.manager.tick()
      val replacement = new TestScaleManager(f.conf, f.manager.platform, 5)
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
      f.manager.cluster = f.manager.cluster.copy(reservedWorkerIndices = Seq(1, 7, 63, 511))
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

  test("a disabled pending expansion permits a new leader to scale down in the same tick") {
    withManager(3, 4) { f =>
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa()
      f.interruptSubmission(meta)
      f.conf.set(AUTO_SCALE_UP_ENABLED, false)
      f.withNewLeader(meta, raft) { next =>
        next.calculator = None
        next.target = 2
        next.tick()
        assert(!meta.getScaleCheckpoint.hasPending)
        assert(Option(meta.workerEventInfos.get(f.workerId(2)))
          .exists(_.getEventType == WorkerEventType.DecommissionThenIdle))
        f.heartbeat(2, State.Idle, meta)
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(2))
      }
    }
  }

  test("a new leader honors the current worker count cap before retrying an expansion") {
    withManager(3, 4) { f =>
      f.useCalculator(f.manager)
      f.conf.set(SCALE_WORKER_NUMBER_MIN, 1).set(SCALE_WORKER_NUMBER_MAX, 4)
      f.register(0, 1, 2)
      val (meta, raft) = f.startHa(0)
      f.interruptSubmission(meta)
      f.conf.set(SCALE_WORKER_NUMBER_MAX, 3)
      f.withNewLeader(meta, raft) { next =>
        next.tick()
        assert(next.operations.isEmpty)
        assert(!meta.getScaleCheckpoint.hasPending)
        assert(meta.getScaleCheckpoint.getAccountedExclWorkerCnt == 0)
        f.conf.set(SCALE_WORKER_NUMBER_MAX, 4)
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(4))
      }
    }
  }

  test("a new leader drains a worker that registered before a pending removal") {
    withManager(3, 2) { f =>
      f.register(0, 1)
      val (meta, raft) = f.startHa()
      f.interruptSubmission(meta)
      f.registerIn(meta, 2)
      f.withNewLeader(meta, raft) { next =>
        next.calculator = None
        next.tick()
        assert(next.operations.isEmpty)
        assert(!meta.getScaleCheckpoint.hasPending)
        assert(Option(meta.workerEventInfos.get(f.workerId(2)))
          .exists(_.getEventType == WorkerEventType.DecommissionThenIdle))
        f.heartbeat(2, State.Idle, meta)
        next.tick()
        assert(next.operations.map(_.specWorkerCnt).toSeq == Seq(2))
      }
    }
  }

  test("expanding a workload admits registrations during the platform update") {
    withManager(2, 3) { f =>
      f.register(0, 1)
      f.manager.beforePlatformUpdate = () => {
        f.register(2, 3, -1)
        assert(Seq(2, 3, -1).forall(i => f.meta.availableWorkers.contains(f.workerId(i))))
      }
      f.manager.tick()
      assert(f.manager.cluster.specWorkerCnt == 3)
      f.manager.result = OperationResult(false, false)
      f.manager.tick()
      assert(Seq(2, 3, -1).forall(i => f.meta.availableWorkers.contains(f.workerId(i))))
    }
  }

  test(
    "a removal blocks a new IP registered with the departing ordinal and admits static workers") {
    withManager(3, 2) { f =>
      f.register(0, 1, -1)
      val departing = new WorkerInfo("192.0.2.32", 1, 2, 3, 4)
      val unmanaged = new WorkerInfo("workers-2", 1, 2, 3, 4)
      f.manager.beforePlatformUpdate = () => {
        assert(f.meta.getScaleCheckpoint.getPending.getRemovesWorkerIndices)
        f.registerIn(f.meta, departing, 2)
        f.registerIn(f.meta, unmanaged, -1)
        assert(f.meta.workersMap.containsKey(departing.toUniqueId))
        assert(!f.meta.availableWorkers.contains(departing))
        assert(f.meta.availableWorkers.contains(unmanaged))
        assert(Seq(0, 1, -1).forall(i => f.meta.availableWorkers.contains(f.workerId(i))))
      }
      f.manager.tick()
      assert(f.manager.cluster.specWorkerCnt == 2)
    }
  }

  Seq(true, false).foreach { changesTermDuringWrite =>
    test(
      s"a stale removal restricts admission until its clearing CAS commits, term changes=$changesTermDuringWrite") {
      withManager(3, 2) { f =>
        f.register(0, 1)
        val (meta, raft) = f.startHa()
        f.interruptSubmission(meta)
        f.withNewLeader(meta, raft) { next =>
          f.registerIn(meta, 2)
          assert(!meta.availableWorkers.contains(f.workerId(2)))
          next.calculator = None
          next.target = 3
          var observedClear = false
          f.beforeCheckpointApply = request => {
            assert(!request.getCompareAndSetScaleCheckpointRequest.getCheckpoint.hasPending)
            observedClear = true
            assert(!meta.availableWorkers.contains(f.workerId(2)))
            if (changesTermDuringWrite) when(raft.getReadyLeaderTerm).thenReturn(3L)
          }
          next.tick()
          assert(observedClear)
          assert(meta.getScaleCheckpoint.hasPending == changesTermDuringWrite)
          assert(meta.availableWorkers.contains(f.workerId(2)) == !changesTermDuringWrite)
          assert(next.operations.isEmpty)
        }
      }
    }
  }

  test("losing leadership during a platform read preserves pending removal admission") {
    withManager(3, 2) { f =>
      f.register(0, 1)
      val (meta, raft) = f.startHa()
      f.manager.beforePlatformUpdate = () => f.registerIn(meta, 2)
      f.manager.tick()
      assert(!meta.availableWorkers.contains(f.workerId(2)))
      f.manager.result = OperationResult(false, false)
      f.manager.onRead = () => when(raft.isLeader).thenReturn(false)
      f.manager.tick()
      assert(!meta.availableWorkers.contains(f.workerId(2)))
      assert(meta.getScaleCheckpoint.hasPending)
    }
  }

  Seq(PbScaleType.SCALE_DOWN, PbScaleType.SCALE_UP).foreach { direction =>
    test(s"a submitted $direction removal protects its ordinal throughout leader warmup") {
      withManager(3, 2) { f =>
        f.register(0, 1)
        val (meta, raft) = f.startHa()
        if (direction == PbScaleType.SCALE_UP) f.heartbeat(1, State.Idle, meta)
        f.manager.tick()
        assert(f.manager.operations.last.scaleType == direction)
        assert(f.manager.cluster.specWorkerCnt == 2)
        assert(meta.getScaleCheckpoint.getPending.getOperationId.nonEmpty)
        f.registerIn(meta, 2)
        assert(!meta.availableWorkers.contains(f.workerId(2)))
        when(raft.getWorkerTimeoutDeadline).thenReturn(f.manager.now + 10000)
        f.withNewLeader(meta, raft) { next =>
          assert(!meta.availableWorkers.contains(f.workerId(2)))
          f.registerIn(meta, -1)
          next.tick()
          assert(next.reads == 0 && next.polls == 0)
          assert(!meta.availableWorkers.contains(f.workerId(2)))
          assert(meta.availableWorkers.contains(f.workerId(-1)))
          assert(meta.getScaleCheckpoint.hasPending)
          when(raft.getWorkerTimeoutDeadline).thenReturn(0L)
          next.calculator = None
          next.result = OperationResult(false, false)
          next.tick()
          assert(!meta.availableWorkers.contains(f.workerId(2)))
          next.result = OperationResult(true, true)
          next.tick()
          assert(!meta.getScaleCheckpoint.hasPending)
          assert(meta.availableWorkers.contains(f.workerId(2)))
        }
      }
    }
  }

  Seq("completed", "rejected", "disabled", "cancelled").foreach { outcome =>
    test(s"late workers are ineligible during a platform removal and released when $outcome") {
      withManager(3, 2) { f =>
        f.register(0, 1)
        val meta = f.meta
        var lateWorkerWasAvailable = true
        f.manager.beforePlatformUpdate = () => {
          f.registerIn(meta, 2)
          lateWorkerWasAvailable = meta.availableWorkers.contains(f.workerId(2))
          if (outcome == "rejected") throw new AccessDeniedException("rejected")
          if (outcome != "completed") throw new IOException("unknown patch outcome")
        }
        if (outcome == "completed") f.manager.tick() else intercept[IOException](f.manager.tick())
        assert(!lateWorkerWasAvailable)
        f.manager.beforePlatformUpdate = () => ()
        outcome match {
          case "completed" =>
            assert(!meta.availableWorkers.contains(f.workerId(2)))
            f.manager.result = OperationResult(false, false)
            f.manager.tick()
            assert(!meta.availableWorkers.contains(f.workerId(2)))
            f.manager.result = OperationResult(true, true)
            f.manager.tick()
          case "rejected" =>
          case "disabled" =>
            f.conf.set(AUTO_SCALE_DOWN_ENABLED, false).set(AUTO_SCALE_UP_ENABLED, false)
            f.manager.tick()
          case "cancelled" =>
            f.manager.target = 3
            f.manager.tick()
        }
        assert(meta.availableWorkers.contains(f.workerId(2)))
      }
    }
  }

  test("disabled scaling reconciles an already submitted removal before releasing its ordinal") {
    withManager(3, 2) { f =>
      f.register(0, 1)
      f.manager.tick()
      assert(f.meta.getScaleCheckpoint.getPending.getOperationId.nonEmpty)
      f.conf.set(AUTO_SCALE_DOWN_ENABLED, false).set(AUTO_SCALE_UP_ENABLED, false)
      f.register(2)
      f.manager.result = OperationResult(false, false)
      f.manager.tick()
      assert(f.meta.getScaleCheckpoint.hasPending)
      assert(!f.meta.availableWorkers.contains(f.workerId(2)))
      f.manager.result = OperationResult(true, true)
      f.manager.tick()
      assert(!f.meta.getScaleCheckpoint.hasPending)
      assert(f.meta.availableWorkers.contains(f.workerId(2)))
      assert(f.manager.operations.size == 1)
    }
  }

  test("recommissioning overlapping registrations cannot expand a target below the old spec") {
    withManager(3, 2) { f =>
      f.register(0, 1, 2)
      f.registerIn(f.meta, new WorkerInfo("workers-0", 1, 5, 6, 7), 0)
      f.publishMetrics()
      Seq(0, 1, 2).foreach(f.heartbeat(_, State.Idle))
      assert(f.meta.workersMap.size() == 4)
      f.manager.tick()
      assert(f.manager.cluster.specWorkerCnt == 3)
      assert(f.manager.operations.isEmpty)
      assert((0 until 3).count(i => f.event(i).contains(WorkerEventType.Recommission)) == 1)
    }
  }

  Seq("none", "manual", "automatic").foreach { exclusion =>
    test(s"an in-flight recommission has exclusion source $exclusion") {
      withManager(4, 4) { f =>
        f.useCalculator(f.manager)
        f.conf.set(SCALE_WORKER_NUMBER_MIN, 1)
        f.register(0, 1, 2, 3)
        f.meta.handleWorkerEvent(
          WorkerEventType.DecommissionThenIdle_VALUE,
          Seq(f.worker(3)).asJava,
          MasterClient.genRequestId())
        f.heartbeat(3, State.InDecommissionThenIdle)
        f.meta.excludedWorkers.remove(f.worker(3))
        f.manager.tick()
        assert(f.event(3).contains(WorkerEventType.Recommission))
        assert(f.manager.operations.isEmpty)
        exclusion match {
          case "none" =>
          case "manual" => f.meta.manuallyExcludedWorkers.add(f.worker(3))
          case "automatic" => f.meta.excludedWorkers.add(f.worker(3))
        }
        f.manager.tick()
        val expected = if (exclusion == "none") Seq.empty else Seq(5)
        assert(f.manager.operations.map(_.specWorkerCnt).toSeq == expected)
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
    val manager = new TestScaleManager(
      conf,
      new TestPlatform(ClusterInfo(spec, Seq.empty, true, "workers", "0")),
      target)
    manager.init(meta)
    manager.tick()

    def useCalculator(manager: TestScaleManager): Unit = {
      conf.set(SCALE_WORKER_NUMBER_MIN_ALIGN_ENABLED, true)
        .set(SCALE_UP_MEMORY_PRESSURE_RATIO, 1.0)
        .set(SCALE_UP_SYSTEM_LOAD_RATIO, 1.0)
        .set(SCALE_UP_DISK_RATIO, 1.0)
      manager.calculator = Some(new SpecWorkerCntCalculator(conf))
    }

    var beforeCheckpointApply: PbMetaRequest => Unit = (_: PbMetaRequest) => ()

    def recordOperation(id: String, operation: ScaleOperation): Unit = {
      val previous = meta.getScaleCheckpoint
      val next = previous.toBuilder.setLastOperation(
        PbScaleOperationRequest.newBuilder().setOperationId(id).setScaleOperation(operation.toPb))
      assert(meta.handleCompareAndSetScaleCheckpoint(-1L, previous.getRevision, next.build()))
    }

    def startHa(excluded: Int*): (HAMasterMetaManager, HARaftServer) = {
      conf.set("celeborn.master.ha.heartbeat.batch.enabled", "false")
      val replica = new HAMasterMetaManager(mock[RpcEnv], conf, resolver, metrics)
      val raft = mock[HARaftServer]
      val handler = new MetaHandler(replica)
      org.mockito.Mockito.when(raft.submitMetaRequest(ArgumentMatchers.any(classOf[PbMetaRequest])))
        .thenAnswer((call: InvocationOnMock) => {
          val request = call.getArgument[PbMetaRequest](0)
          beforeCheckpointApply(request)
          ResourceResponse.parseFrom(handler.handleWriteRequest(request, raft.getReadyLeaderTerm)
            .toByteArray)
        })
      org.mockito.Mockito.when(raft.submitRequest(ArgumentMatchers.any(classOf[ResourceRequest])))
        .thenAnswer((call: InvocationOnMock) => {
          val request = PbMetaRequest.parseFrom(call.getArgument[ResourceRequest](0).toByteArray)
          ResourceResponse.parseFrom(handler.handleWriteRequest(request, raft.getReadyLeaderTerm)
            .toByteArray)
        })
      replica.setRatisServer(raft)
      when(raft.isLeader).thenReturn(true)
      when(raft.getLeaderVersion).thenReturn(1L)
      when(raft.getReadyLeaderTerm).thenReturn(1L)
      replica.workersMap.putAll(meta.workersMap)
      replica.workerStats.putAll(meta.workerStats)
      excluded.foreach(index => replica.manuallyExcludedWorkers.add(worker(index)))
      manager.init(replica)
      manager.tick()
      publishMetrics(replica)
      (replica, raft)
    }

    def interruptSubmission(meta: HAMasterMetaManager): Unit = {
      val before = manager.cluster
      manager.beforePlatformUpdate = () => throw new IOException("unknown patch outcome")
      intercept[IOException](manager.tick())
      assert(manager.operations.isEmpty)
      assert(manager.cluster == before)
      assert(meta.getScaleCheckpoint.hasPending)
      assert(meta.getScaleCheckpoint.getPending.getOperationId.isEmpty)
    }

    def withNewLeader(meta: HAMasterMetaManager, raft: HARaftServer)(
        body: TestScaleManager => Unit): Unit = {
      when(raft.isLeader).thenReturn(true)
      when(raft.getLeaderVersion).thenReturn(2L)
      when(raft.getReadyLeaderTerm).thenReturn(2L)
      val replacement = new TestScaleManager(conf, manager.platform, manager.target)
      try {
        useCalculator(replacement)
        replacement.init(meta)
        replacement.tick()
        assert(meta.workerStats.values().asScala.forall(_.get() == null))
        publishMetrics(meta)
        body(replacement)
      } finally {
        replacement.stop()
      }
    }

    def workerId(index: Int): WorkerInfo = new WorkerInfo(s"workers-$index", 1, 2, 3, 4)

    def register(indices: Int*): Unit = indices.foreach(registerIn(meta, _))

    def registerIn(manager: AbstractMetaManager, index: Int): Unit =
      registerIn(manager, workerId(index), math.max(-1, index))

    def registerIn(manager: AbstractMetaManager, worker: WorkerInfo, index: Int): Unit = {
      manager.updateRegisterWorkerMeta(
        worker.host,
        worker.rpcPort,
        worker.pushPort,
        worker.fetchPort,
        worker.replicatePort,
        -1,
        null,
        Collections.emptyMap[String, DiskInfo](),
        index)
      manager.prepareWorkerStatsUpdate(worker).set(WorkerStats(Map(
        WorkerStats.MemoryPressureRatio -> 0.4,
        WorkerStats.LastMinuteSystemLoadRatio -> 0.4,
        WorkerStats.DiskUsedRatio -> 0.4)))
    }

    def publishMetrics(
        metaManager: AbstractMetaManager = meta): Unit = {
      metaManager.workerStats.values().asScala.foreach(_.set(WorkerStats(Map(
        WorkerStats.MemoryPressureRatio -> 0.4,
        WorkerStats.LastMinuteSystemLoadRatio -> 0.4,
        WorkerStats.DiskUsedRatio -> 0.4))))
    }

    def worker(index: Int): WorkerInfo =
      meta.workersMap.get(workerId(index).toUniqueId)

    def event(index: Int): Option[WorkerEventType] =
      Option(meta.workerEventInfos.get(worker(index))).map(_.getEventType)

    def heartbeat(index: Int, state: State, metaManager: AbstractMetaManager = meta): Unit = {
      val w = workerId(index)
      metaManager.updateWorkerHeartbeatMeta(
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

  private class TestPlatform(var info: ClusterInfo) {
    private var version = 0
    var conflicts = 0

    def update(expected: ClusterInfo, operation: ScaleOperation): Unit = {
      if (info.workloadUid != expected.workloadUid ||
        info.platformVersion != expected.platformVersion) {
        conflicts += 1
        throw new AccessDeniedException("platform version changed")
      }
      version += 1
      info = info.copy(
        specWorkerCnt = operation.specWorkerCnt,
        reservedWorkerIndices = operation.reservedWorkerIndices,
        platformVersion = version.toString)
    }
  }

  private class TestScaleManager(
      conf: CelebornConf,
      val platform: TestPlatform,
      var target: Int) extends BaseK8sScaleManager(conf) {
    val operations = new ArrayBuffer[ScaleOperation]
    var now = 1000000L
    var reads = 0
    var polls = 0
    var newExclusions = 0
    val exclusionDeltas = new ArrayBuffer[Int]
    var onCalculate: () => Unit = () => ()
    var onRead: () => Unit = () => ()
    var onPoll: () => Unit = () => ()
    var applyPlatformUpdate: (ClusterInfo, ScaleOperation) => Unit = platform.update
    var beforePlatformUpdate: () => Unit = () => ()
    var afterPlatformUpdate: () => Unit = () => ()
    var accountedExclusions = Option.empty[Int]
    var calculator = Option.empty[SpecWorkerCntCalculator]
    var result = OperationResult(true, true)

    private var observed = platform.info

    def cluster: ClusterInfo = platform.info
    def cluster_=(value: ClusterInfo): Unit = platform.info = value
    def tick(): Unit = checkScale()
    override protected def isRejectedPlatformUpdate(error: Exception): Boolean =
      error.isInstanceOf[AccessDeniedException]
    override protected def currentTimeMillis: Long = now
    override protected def getCurrentClusterInfo(): ClusterInfo = {
      reads += 1
      onRead()
      observed = cluster
      observed
    }
    override protected def doScaleOperation(operation: ScaleOperation): String = {
      val expected = observed
      beforePlatformUpdate()
      applyPlatformUpdate(expected, operation)
      operations += operation
      afterPlatformUpdate()
      cluster.platformVersion
    }
    override protected def checkOperation(operationId: String): OperationResult = {
      polls += 1
      onPoll()
      result
    }
    override protected def calculateScaleTarget(
        registeredWorkerCnt: Int,
        specWorkerCnt: Int,
        workerStats: Seq[WorkerStats],
        exclWorkerCnt: Int,
        unaccountedExclWorkerCnt: Int): ScaleTarget = {
      calculator match {
        case Some(policy) =>
          val result = policy.cal(SpecWorkerCntCalculator.Input(
            registeredWorkerCnt,
            specWorkerCnt,
            workerStats,
            exclWorkerCnt,
            unaccountedExclWorkerCnt))
          ScaleTarget(result.targetSpecWorkerCnt, result.accountedExclWorkerCnt)
        case None =>
          newExclusions = unaccountedExclWorkerCnt
          exclusionDeltas += unaccountedExclWorkerCnt
          onCalculate()
          ScaleTarget(target, accountedExclusions.getOrElse(exclWorkerCnt))
      }
    }
  }
}
