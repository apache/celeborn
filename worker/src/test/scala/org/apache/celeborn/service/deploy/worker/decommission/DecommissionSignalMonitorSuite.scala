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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.concurrent.duration._

import com.google.common.collect.{Maps, Sets}
import com.google.common.util.concurrent.Uninterruptibles
import org.mockito.MockitoSugar.{mock, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.WORKER_DECOMMISSION_SIGNAL_CHECKER
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
    statusManager = new WorkerStatusManager(conf)
    statusManager.systemExitAction = () => ()
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
      intervalMs: Long = 100,
      stopTimeoutMs: Long = 5000): DecommissionSignalMonitor = {
    monitor = new DecommissionSignalMonitor(
      checker,
      intervalMs,
      () => statusManager.inExitStatus(),
      () => {
        triggerCount.incrementAndGet()
        statusManager.doTransition(WorkerEventType.Decommission)
      },
      stopTimeoutMs)
    monitor
  }

  test("polling thread decommissions the worker once the checker signals") {
    newMonitor(new AlwaysDecommissionSignalChecker(conf)).start()
    eventually(timeout(5.seconds), interval(50.milliseconds)) {
      assert(statusManager.getWorkerState() == State.InDecommission)
    }
    assert(monitor.isTriggered)
    assert(triggerCount.get == 1)
  }

  test("decommission is triggered at most once") {
    val m = newMonitor(new AlwaysDecommissionSignalChecker(conf))
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

  test("LinkageError from the checker is treated as no signal") {
    val m = newMonitor(
      new ThrowingThenTrueDecommissionSignalChecker(1, () => new NoClassDefFoundError("missing")))
    m.checkOnce()
    assert(triggerCount.get == 0)
    m.checkOnce()
    assert(triggerCount.get == 1)
  }

  test("a failing getReason does not lose the trigger") {
    val m = newMonitor(new DecommissionSignalChecker {
      override def shouldDecommission(): Boolean = true
      override def getReason: String = throw new NoClassDefFoundError("missing")
    })
    m.checkOnce()
    assert(m.isTriggered)
    assert(triggerCount.get == 1)
    assert(statusManager.getWorkerState() == State.InDecommission)
  }

  test("a blocked getReason does not hold back the trigger") {
    val inReason = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val m = newMonitor(new DecommissionSignalChecker {
      override def shouldDecommission(): Boolean = true
      override def getReason: String = {
        inReason.countDown()
        Uninterruptibles.awaitUninterruptibly(release)
        "late reason"
      }
    })
    val poller = new Thread(new Runnable {
      override def run(): Unit = m.checkOnce()
    })
    poller.setDaemon(true)
    poller.start()
    try {
      assert(inReason.await(5, TimeUnit.SECONDS))
      assert(triggerCount.get == 1)
      assert(statusManager.getWorkerState() == State.InDecommission)
    } finally {
      release.countDown()
      poller.join(5000)
    }
  }

  test("stop does not wait forever on a check that ignores interrupts") {
    val inCheck = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val checkRunning = new AtomicBoolean(false)
    val closedDuringCheck = new AtomicBoolean(false)
    val m = newMonitor(
      new DecommissionSignalChecker {
        override def shouldDecommission(): Boolean = {
          checkRunning.set(true)
          inCheck.countDown()
          Uninterruptibles.awaitUninterruptibly(release)
          checkRunning.set(false)
          false
        }
        override def close(): Unit = closedDuringCheck.set(checkRunning.get)
      },
      intervalMs = 10,
      stopTimeoutMs = 200)
    m.start()
    try {
      assert(inCheck.await(5, TimeUnit.SECONDS))
      val start = System.nanoTime()
      m.stop()
      assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 3000)
      assert(closedDuringCheck.get)
    } finally {
      release.countDown()
    }
  }

  test("stop does not wait forever on a blocked close") {
    val inClose = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val m = newMonitor(
      new DecommissionSignalChecker {
        override def shouldDecommission(): Boolean = false
        override def close(): Unit = {
          inClose.countDown()
          Uninterruptibles.awaitUninterruptibly(release)
        }
      },
      intervalMs = 60000,
      stopTimeoutMs = 200)
    m.start()
    val stopper = new Thread(new Runnable {
      override def run(): Unit = m.stop()
    })
    stopper.setDaemon(true)
    stopper.start()
    try {
      stopper.join(3000)
      assert(inClose.getCount == 0)
      assert(!stopper.isAlive)
    } finally {
      release.countDown()
    }
  }

  test("stop waits for an in-flight check before closing the checker") {
    val inCheck = new CountDownLatch(1)
    val checkDone = new AtomicBoolean(false)
    val checkDoneAtClose = new AtomicReference[java.lang.Boolean]()
    val m = newMonitor(
      new DecommissionSignalChecker {
        override def shouldDecommission(): Boolean = {
          inCheck.countDown()
          try {
            Thread.sleep(60000)
          } catch {
            case _: InterruptedException => Thread.sleep(500)
          }
          checkDone.set(true)
          false
        }
        override def close(): Unit = checkDoneAtClose.set(checkDone.get)
      },
      intervalMs = 10)
    m.start()
    assert(inCheck.await(5, TimeUnit.SECONDS))
    m.stop()
    assert(checkDoneAtClose.get == java.lang.Boolean.TRUE)
  }

  test("no trigger when stopped while a poll is in flight") {
    lazy val m: DecommissionSignalMonitor = newMonitor(new DecommissionSignalChecker {
      override def shouldDecommission(): Boolean = {
        m.stop()
        true
      }
    })
    m.checkOnce()
    assert(!m.isTriggered)
    assert(triggerCount.get == 0)
  }

  test("a failing trigger is not retried") {
    val calls = new AtomicInteger(0)
    val m = new DecommissionSignalMonitor(
      new AlwaysDecommissionSignalChecker(conf),
      100,
      () => false,
      () => {
        calls.incrementAndGet()
        throw new IllegalStateException("trigger failed")
      })
    try {
      m.checkOnce()
      m.checkOnce()
      assert(m.isTriggered)
      assert(calls.get == 1)
    } finally {
      m.stop()
    }
  }

  test("skip while the worker is exiting, escalate from DecommissionThenIdle") {
    val m = newMonitor(new AlwaysDecommissionSignalChecker(conf))
    statusManager.doTransition(WorkerEventType.Graceful)
    assert(statusManager.getWorkerState() == State.InGraceFul)
    m.checkOnce()
    assert(triggerCount.get == 0)
    assert(!m.isTriggered)

    beforeEach()
    val escalating = newMonitor(new AlwaysDecommissionSignalChecker(conf))
    statusManager.doTransition(WorkerEventType.DecommissionThenIdle)
    assert(statusManager.getWorkerState() == State.InDecommissionThenIdle)
    escalating.checkOnce()
    assert(triggerCount.get == 1)
    assert(statusManager.getWorkerState() == State.InDecommission)
  }

  test("stop closes the checker once and disables further checks") {
    AlwaysDecommissionSignalChecker.closeCount.set(0)
    val m = newMonitor(new AlwaysDecommissionSignalChecker(conf), intervalMs = 60000)
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
      WORKER_DECOMMISSION_SIGNAL_CHECKER.key,
      classOf[AlwaysDecommissionSignalChecker].getName)
    assert(DecommissionSignalMonitor.createChecker(c).isInstanceOf[AlwaysDecommissionSignalChecker])
    assert(DecommissionSignalMonitor.createChecker(new CelebornConf())
      .isInstanceOf[NoOpDecommissionSignalChecker])
  }

  test("createChecker fails fast on a bad class") {
    Seq(
      "does.not.Exist",
      classOf[NotADecommissionSignalChecker].getName,
      classOf[NoConfConstructorDecommissionSignalChecker].getName,
      classOf[ConstructorFailingDecommissionSignalChecker].getName).foreach { className =>
      val c = conf.clone
      c.set(WORKER_DECOMMISSION_SIGNAL_CHECKER.key, className)
      val e = intercept[CelebornException](DecommissionSignalMonitor.createChecker(c))
      assert(e.getMessage.contains(WORKER_DECOMMISSION_SIGNAL_CHECKER.key))
      assert(e.getMessage.contains(className))
    }
  }
}
