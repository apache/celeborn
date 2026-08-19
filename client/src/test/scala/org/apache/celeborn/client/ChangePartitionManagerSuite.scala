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

import org.mockito.Mockito.{doAnswer, mock, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.message.StatusCode

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
