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

import scala.collection.JavaConverters._

import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt, anyLong, anyString}
import org.mockito.Mockito.{doAnswer, mock, never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

class ChangePartitionManagerSuite extends CelebornFunSuite {
  test("collectCandidateWorkers combines snapshot and endpoint-ready workers") {
    val conf = dynamicResourceConf()
    val lifecycleManager = mock(classOf[LifecycleManager])
    val statusTracker = new WorkerStatusTracker(conf, lifecycleManager)
    when(lifecycleManager.workerStatusTracker).thenReturn(statusTracker)

    val snapshotWorker = worker("snapshot")
    val duplicateSnapshotWorker = worker("snapshot")
    val dynamicWorker = worker("dynamic")
    when(lifecycleManager.workerSnapshots(1)).thenReturn(snapshot(snapshotWorker))
    statusTracker.addEndpointReadyWorkers(Set(duplicateSnapshotWorker, dynamicWorker))

    val manager = new ChangePartitionManager(conf, lifecycleManager)
    val candidates = manager.collectCandidateWorkers(1)

    verify(lifecycleManager).refreshEndpointReadyWorkersFromMaster(1)
    assert(candidates.asScala.toSet === Set(snapshotWorker, dynamicWorker))
    assert(candidates.asScala.find(_ == snapshotWorker).get eq snapshotWorker)
  }

  test("collectCandidateWorkers filters workers excluded during refresh") {
    val conf = dynamicResourceConf()
    val lifecycleManager = mock(classOf[LifecycleManager])
    val statusTracker = new WorkerStatusTracker(conf, lifecycleManager)
    when(lifecycleManager.workerStatusTracker).thenReturn(statusTracker)

    val failedWorker = worker("failed")
    when(lifecycleManager.workerSnapshots(1)).thenReturn(snapshot(failedWorker))
    statusTracker.addEndpointReadyWorkers(Set(failedWorker))
    doAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock): AnyRef = {
        statusTracker.excludedWorkers.put(
          failedWorker,
          (StatusCode.WORKER_UNRESPONSIVE, System.currentTimeMillis()))
        null
      }
    }).when(lifecycleManager).refreshEndpointReadyWorkersFromMaster(1)

    val manager = new ChangePartitionManager(conf, lifecycleManager)

    assert(manager.collectCandidateWorkers(1).isEmpty)
  }

  test("revive count ignores duplicate requests served by an existing newer location") {
    val (lifecycleManager, manager) = reviveFixture(reserveResult = true)
    val latest = new util.concurrent.ConcurrentHashMap[Int, PartitionLocation]()
    latest.put(0, new PartitionLocation(0, 1, "host", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY))
    lifecycleManager.latestPartitionLocation.put(1, latest)

    (0 until 40).foreach(_ => requestRevive(manager, epoch = 0))

    verify(lifecycleManager, never()).reserveSlotsWithRetry(
      anyInt(),
      any[util.HashSet[WorkerInfo]](),
      any[WorkerResource](),
      anyBoolean(),
      anyBoolean())
    verify(lifecycleManager, never()).incClientCounter(anyString(), anyLong())
  }

  test("revive count is not incremented when slot reservation fails") {
    val (lifecycleManager, manager) = reviveFixture(reserveResult = false)

    requestRevive(manager, epoch = 0)

    verify(lifecycleManager, never()).incClientCounter(anyString(), anyLong())
  }

  test("revive count is incremented after successful slot reservation") {
    val (lifecycleManager, manager) = reviveFixture(reserveResult = true)

    requestRevive(manager, epoch = 0)
    verify(lifecycleManager).incClientCounter(CelebornClientSource.REVIVE_COUNT, 1L)
  }

  test("revive count includes successful soft splits with no cause") {
    val (lifecycleManager, manager) = reviveFixture(reserveResult = true)

    requestRevive(manager, epoch = 0, cause = None)

    verify(lifecycleManager).incClientCounter(CelebornClientSource.REVIVE_COUNT, 1L)
  }

  test("revive count excludes initial allocations") {
    val (lifecycleManager, manager) = reviveFixture(reserveResult = true)

    manager.handleRequestPartitionLocation(
      mock(classOf[RequestLocationCallContext]),
      1,
      0,
      -1,
      null,
      None,
      isSegmentGranularityVisible = false)

    verify(lifecycleManager).reserveSlotsWithRetry(
      anyInt(),
      any[util.HashSet[WorkerInfo]](),
      any[WorkerResource](),
      anyBoolean(),
      anyBoolean())
    verify(lifecycleManager, never()).incClientCounter(anyString(), anyLong())
  }

  private def reviveFixture(reserveResult: Boolean): (LifecycleManager, ChangePartitionManager) = {
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_BATCH_HANDLE_CHANGE_PARTITION_ENABLED, false)
    val lifecycleManager = mock(classOf[LifecycleManager])
    val statusTracker = new WorkerStatusTracker(conf, lifecycleManager)
    when(lifecycleManager.workerStatusTracker).thenReturn(statusTracker)
    when(lifecycleManager.latestPartitionLocation).thenReturn(
      JavaUtils.newConcurrentHashMap[
        Int,
        util.concurrent.ConcurrentHashMap[Int, PartitionLocation]]())
    when(lifecycleManager.commitManager).thenReturn(mock(classOf[CommitManager]))
    when(lifecycleManager.workerSnapshots(1)).thenReturn(snapshot(worker("w1")))
    when(lifecycleManager.reserveSlotsWithRetry(
      anyInt(),
      any[util.HashSet[WorkerInfo]](),
      any[WorkerResource](),
      anyBoolean(),
      anyBoolean())).thenReturn(reserveResult)
    (lifecycleManager, new ChangePartitionManager(conf, lifecycleManager))
  }

  private def requestRevive(
      manager: ChangePartitionManager,
      epoch: Int,
      cause: Option[StatusCode] = Some(StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY)): Unit = {
    manager.handleRequestPartitionLocation(
      mock(classOf[RequestLocationCallContext]),
      1,
      0,
      epoch,
      new PartitionLocation(0, epoch, "host", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY),
      cause,
      isSegmentGranularityVisible = false)
  }

  private def dynamicResourceConf(): CelebornConf = {
    new CelebornConf().set(CelebornConf.CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED, true)
  }

  private def worker(host: String): WorkerInfo = {
    new WorkerInfo(host, 1001, 1002, 1003, 1004)
  }

  private def snapshot(worker: WorkerInfo): util.Map[String, ShufflePartitionLocationInfo] = {
    val workers = new util.HashMap[String, ShufflePartitionLocationInfo]()
    workers.put(worker.toUniqueId, new ShufflePartitionLocationInfo(worker))
    workers
  }
}
