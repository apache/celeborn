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
    Assert.assertEquals(PbWorkerStatus.State.InDecommissionThenIdle, statusManager.getWorkerState())
    Assert.assertEquals(
      PbWorkerStatus.State.InDecommissionThenIdle.getNumber,
      worker.workerInfo.getWorkerStatus().getStateValue)

    // Rerun state Transition
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    Assert.assertEquals(PbWorkerStatus.State.InDecommissionThenIdle, statusManager.getWorkerState())

    // Reset shuffleKeys
    shuffleKeys.clear()
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    Assert.assertEquals(PbWorkerStatus.State.Idle, statusManager.getWorkerState())

    statusManager.doTransition(WorkerEventType.Recommission)
    Assert.assertEquals(PbWorkerStatus.State.Normal, statusManager.getWorkerState())

    statusManager.doTransition(WorkerEventType.Recommission)
    Assert.assertEquals(PbWorkerStatus.State.Normal, statusManager.getWorkerState())
  }

  test("Test exitEventType initialization based on config") {
    // Default: neither graceful nor decommission → Immediately
    val conf1 = new CelebornConf()
    val mgr1 = new WorkerStatusManager(conf1)
    Assert.assertEquals(WorkerEventType.Immediately, mgr1.exitEventType)

    // Graceful shutdown only → Graceful
    val conf2 = new CelebornConf()
    conf2.set("celeborn.worker.graceful.shutdown.enabled", "true")
    val mgr2 = new WorkerStatusManager(conf2)
    Assert.assertEquals(WorkerEventType.Graceful, mgr2.exitEventType)

    // Decommission shutdown only → Decommission
    val conf3 = new CelebornConf()
    conf3.set("celeborn.worker.decommission.shutdown.enabled", "true")
    val mgr3 = new WorkerStatusManager(conf3)
    Assert.assertEquals(WorkerEventType.Decommission, mgr3.exitEventType)

    // Both enabled → Decommission overrides graceful
    val conf4 = new CelebornConf()
    conf4.set("celeborn.worker.graceful.shutdown.enabled", "true")
    conf4.set("celeborn.worker.decommission.shutdown.enabled", "true")
    val mgr4 = new WorkerStatusManager(conf4)
    Assert.assertEquals(WorkerEventType.Decommission, mgr4.exitEventType)
    Assert.assertFalse(conf4.workerGracefulShutdownEnabled)
    Assert.assertTrue(conf4.workerDecommissionShutdownEnabled)
  }
}
