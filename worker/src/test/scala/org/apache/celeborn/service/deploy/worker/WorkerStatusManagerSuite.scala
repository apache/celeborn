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

package org.apache.celeborn.service.deploy.worker

import java.util.concurrent.atomic.AtomicBoolean

import com.google.common.collect.{Maps, Sets}
import org.junit.Assert
import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PbWorkerStatus, WorkerEventType}
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class WorkerStatusManagerSuite extends AnyFunSuite {
  private var worker: Worker = _
  private val conf = new CelebornConf()

  test("Test Worker status transition to none exit status") {
    worker = mock[Worker]
    val storageManager = mock[StorageManager]
    val shuffleKeys = Sets.newHashSet("test")
    val workerInfo = new WorkerInfo(
      "host",
      0,
      0,
      0,
      0,
      0,
      Maps.newHashMap(),
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption])
    when(storageManager.shuffleKeySet()).thenReturn(shuffleKeys)
    when(worker.storageManager).thenReturn(storageManager)
    when(worker.shutdown).thenReturn(new AtomicBoolean())
    when(worker.workerInfo).thenReturn(workerInfo)
    val statusManager = new WorkerStatusManager(conf)
    statusManager.init(worker)

    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    Assert.assertEquals(statusManager.getWorkerState(), PbWorkerStatus.State.InDecommissionThenIdle)
    Assert.assertEquals(
      worker.workerInfo.getWorkerStatus().getStateValue,
      PbWorkerStatus.State.InDecommissionThenIdle.getNumber)

    // Rerun state Transition
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    Assert.assertEquals(statusManager.getWorkerState(), PbWorkerStatus.State.InDecommissionThenIdle)

    // Reset shuffleKeys
    shuffleKeys.clear()
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    Assert.assertEquals(statusManager.getWorkerState(), PbWorkerStatus.State.Idle)

    statusManager.doTransition(WorkerEventType.Recommission)
    Assert.assertEquals(statusManager.getWorkerState(), PbWorkerStatus.State.Normal)

    statusManager.doTransition(WorkerEventType.Recommission)
    Assert.assertEquals(statusManager.getWorkerState(), PbWorkerStatus.State.Normal)
  }
}
