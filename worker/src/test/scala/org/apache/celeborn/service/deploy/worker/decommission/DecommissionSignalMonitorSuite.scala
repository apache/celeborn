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

package org.apache.celeborn.service.deploy.worker.decommission

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.concurrent.duration._

import com.google.common.collect.{Maps, Sets}
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.PbWorkerStatus.State
import org.apache.celeborn.common.protocol.WorkerEventType
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.util.JavaUtils
import org.apache.celeborn.service.deploy.worker.{Worker, WorkerStatusManager}
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class DecommissionSignalMonitorSuite extends AnyFunSuite with BeforeAndAfterEach {

  private class NoExitWorkerStatusManager(conf: CelebornConf) extends WorkerStatusManager(conf) {
    override protected[worker] def scheduleSystemExit(): Unit = {}
  }

  private val conf = new CelebornConf()
  private var statusManager: WorkerStatusManager = _
  private var triggerCount: AtomicInteger = _
  private var monitor: DecommissionSignalMonitor = _

  override def beforeEach(): Unit = {
    val worker = mock[Worker]
    val storageManager = mock[StorageManager]
    val workerInfo = new WorkerInfo(
      "host",
      0,
      0,
      0,
      0,
      0,
      Maps.newHashMap(),
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption])
    when(storageManager.shuffleKeySet()).thenReturn(Sets.newHashSet("shuffle"))
    when(worker.storageManager).thenReturn(storageManager)
    when(worker.shutdown).thenReturn(new AtomicBoolean())
    when(worker.workerInfo).thenReturn(workerInfo)
    statusManager = new NoExitWorkerStatusManager(conf)
    statusManager.init(worker)
    triggerCount = new AtomicInteger(0)
  }

  override def afterEach(): Unit = {
    if (monitor != null) {
      monitor.stop()
      monitor = null
    }
  }

  private def newMonitor(
      checker: DecommissionSignalChecker,
      intervalMs: Long = 100): DecommissionSignalMonitor = {
    monitor = new DecommissionSignalMonitor(
      checker,
      intervalMs,
      () => statusManager.inExitStatus(),
      () => {
        triggerCount.incrementAndGet()
        statusManager.doTransition(WorkerEventType.Decommission)
      })
    monitor
  }

  test("polling thread decommissions the worker once the checker signals") {
    newMonitor(new AlwaysDecommissionSignalChecker).start()
    eventually(timeout(5.seconds), interval(50.milliseconds)) {
      assert(statusManager.getWorkerState() == State.InDecommission)
    }
    assert(monitor.isTriggered)
    assert(triggerCount.get == 1)
  }

  test("decommission is triggered at most once") {
    val m = newMonitor(new AlwaysDecommissionSignalChecker)
    (1 to 5).foreach(_ => m.checkOnce())
    assert(triggerCount.get == 1)
    assert(statusManager.getWorkerState() == State.InDecommission)
  }

  test("checker exceptions are treated as no signal") {
    val m = newMonitor(new ThrowingThenTrueDecommissionSignalChecker(3))
    (1 to 3).foreach { _ =>
      m.checkOnce()
      assert(statusManager.getWorkerState() == State.Normal)
    }
    assert(triggerCount.get == 0)
    m.checkOnce()
    assert(triggerCount.get == 1)
    assert(statusManager.getWorkerState() == State.InDecommission)
  }

  test("skip while the worker is exiting, escalate from DecommissionThenIdle") {
    val m = newMonitor(new AlwaysDecommissionSignalChecker)
    statusManager.doTransition(WorkerEventType.Graceful)
    assert(statusManager.getWorkerState() == State.InGraceFul)
    m.checkOnce()
    assert(triggerCount.get == 0)
    assert(!m.isTriggered)

    beforeEach()
    val escalating = newMonitor(new AlwaysDecommissionSignalChecker)
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    assert(statusManager.getWorkerState() == State.InDecommissionThenIdle)
    escalating.checkOnce()
    assert(triggerCount.get == 1)
    assert(statusManager.getWorkerState() == State.InDecommission)
  }

  test("stop closes the checker once and disables further checks") {
    AlwaysDecommissionSignalChecker.closeCount.set(0)
    val m = newMonitor(new AlwaysDecommissionSignalChecker, intervalMs = 60000)
    m.start()
    m.stop()
    m.stop()
    assert(AlwaysDecommissionSignalChecker.closeCount.get == 1)
    m.checkOnce()
    assert(triggerCount.get == 0)
    assert(statusManager.getWorkerState() == State.Normal)
  }

  test("createChecker instantiates the configured class") {
    val c = conf.clone
    c.set(
      WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS.key,
      classOf[AlwaysDecommissionSignalChecker].getName)
    assert(DecommissionSignalMonitor.createChecker(c).isInstanceOf[AlwaysDecommissionSignalChecker])
    assert(DecommissionSignalMonitor.createChecker(new CelebornConf())
      .isInstanceOf[NoOpDecommissionSignalChecker])
  }

  test("createChecker fails fast on a bad class") {
    Seq(
      "does.not.Exist",
      classOf[NotADecommissionSignalChecker].getName,
      classOf[InitFailingDecommissionSignalChecker].getName).foreach { className =>
      val c = conf.clone
      c.set(WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS.key, className)
      val e = intercept[CelebornException](DecommissionSignalMonitor.createChecker(c))
      assert(e.getMessage.contains(WORKER_DECOMMISSION_SIGNAL_CHECKER_CLASS.key))
      assert(e.getMessage.contains(className))
    }
  }
}
