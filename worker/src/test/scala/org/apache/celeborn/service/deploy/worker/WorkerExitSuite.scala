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

import java.util.concurrent.{Callable, ConcurrentLinkedQueue, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.PbWorkerStatus.State
import org.apache.celeborn.common.protocol.WorkerEventType
import org.apache.celeborn.common.util.ShutdownHookManager
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class WorkerExitSuite extends AnyFunSuite {
  private val configuredTimeoutMs = TimeUnit.HOURS.toMillis(6)
  private val requestedTimeoutMs = TimeUnit.MINUTES.toMillis(30)

  private class ExitFixture {
    val conf = new CelebornConf()
      .set(CelebornConf.WORKER_DECOMMISSION_FORCE_EXIT_TIMEOUT.key, "6h")
    val worker = mock[Worker]
    val shutdownHooks = mock[ShutdownHookManager]
    val exitTimeouts = new ConcurrentLinkedQueue[Long]()
    val statusManager = new WorkerStatusManager(conf) {
      override protected def startExitThread(): Unit = {
        // Keep the real transition but do not start a thread that terminates the test JVM.
        exitTimeouts.add(ExitFixture.this.worker.decommissionTimeoutMs)
      }
    }

    when(worker.conf).thenReturn(conf)
    when(worker.workerStatusManager).thenReturn(statusManager)
    when(worker.workerInfo).thenReturn(mock[WorkerInfo])
    when(worker.shutdown).thenReturn(new AtomicBoolean())
    when(worker.storageManager).thenReturn(mock[StorageManager])
    Mockito.doCallRealMethod().when(worker).exit(any[String](), any[String]())
    Mockito.doCallRealMethod().when(worker).configureDecommissionTimeout(any[Option[Long]]())
    Mockito.doCallRealMethod().when(worker).decommissionTimeoutMs
    statusManager.init(worker)

    def withShutdownHooks(body: => Unit): Unit = {
      // Static mocks are local to the calling thread, including concurrent HTTP requests below.
      val mocked = Mockito.mockStatic(classOf[ShutdownHookManager])
      try {
        mocked.when[ShutdownHookManager](() => ShutdownHookManager.get()).thenReturn(shutdownHooks)
        body
      } finally {
        mocked.close()
      }
    }

    def exit(exitType: String, timeout: String): Unit = withShutdownHooks {
      worker.exit(exitType, timeout)
    }

    def assertDecommissionTimeout(expected: Long): Unit = {
      assert(statusManager.getWorkerState() == State.InDecommission)
      assert(worker.decommissionTimeoutMs == expected)
      // The timeout is installed before the exit thread can start.
      assert(exitTimeouts.size() == 1)
      assert(exitTimeouts.peek() == expected)
      verify(shutdownHooks).updateTimeout(expected, TimeUnit.MILLISECONDS)
      verifyNoMoreInteractions(shutdownHooks)
    }
  }

  test("decommission installs the requested timeout before starting shutdown") {
    val fixture = new ExitFixture
    fixture.exit("DECOMMISSION", "30m")
    fixture.assertDecommissionTimeout(requestedTimeoutMs)
  }

  for (timeout <- Seq(null, "", "invalid", "0s", "-1s")) {
    test(s"decommission uses the configured timeout for '$timeout'") {
      val fixture = new ExitFixture
      fixture.exit("DECOMMISSION", timeout)
      fixture.assertDecommissionTimeout(configuredTimeoutMs)
    }
  }

  test("ignored decommission requests cannot overwrite the accepted timeout") {
    val fixture = new ExitFixture
    fixture.exit("DECOMMISSION", "30m")
    val acceptedStatus = fixture.statusManager.currentWorkerStatus

    Seq(null, "5s", "invalid").foreach { timeout =>
      fixture.exit("DECOMMISSION", timeout)
      assert(fixture.statusManager.currentWorkerStatus eq acceptedStatus)
      fixture.assertDecommissionTimeout(requestedTimeoutMs)
    }
  }

  test("concurrent decommission requests configure only the accepted timeout") {
    val fixture = new ExitFixture
    val barrier = new CyclicBarrier(2)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val requests = Seq("30m", "5s").map { timeout =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            barrier.await(10, TimeUnit.SECONDS)
            fixture.exit("DECOMMISSION", timeout)
          }
        })
      }
      requests.foreach(_.get(10, TimeUnit.SECONDS))
      val acceptedTimeout = fixture.exitTimeouts.peek()
      assert(Set(requestedTimeoutMs, 5000L).contains(acceptedTimeout))
      fixture.assertDecommissionTimeout(acceptedTimeout)
    } finally {
      executor.shutdownNow()
    }
  }

  for ((exitType, state) <- Seq("GRACEFUL" -> State.InGraceFul, "IMMEDIATELY" -> State.InExit)) {
    test(s"decommission cannot alter the timeout after $exitType is accepted") {
      val fixture = new ExitFixture
      fixture.exit(exitType, null)
      fixture.exit("DECOMMISSION", "5s")

      assert(fixture.statusManager.getWorkerState() == state)
      assert(fixture.exitTimeouts.size() == 1)
      verify(fixture.worker, never).configureDecommissionTimeout(any[Option[Long]]())
      Mockito.verifyNoInteractions(fixture.shutdownHooks)
    }
  }

  test("heartbeat decommission uses the configured timeout and ignores later API overrides") {
    val fixture = new ExitFixture
    fixture.withShutdownHooks {
      fixture.statusManager.doTransition(WorkerEventType.Decommission)
    }
    fixture.exit("DECOMMISSION", "5s")
    fixture.assertDecommissionTimeout(configuredTimeoutMs)
  }

  test("heartbeat events cannot change a decommission timeout accepted through the API") {
    val fixture = new ExitFixture
    fixture.exit("DECOMMISSION", "30m")
    fixture.withShutdownHooks {
      fixture.statusManager.doTransition(WorkerEventType.Decommission)
      fixture.statusManager.doTransition(WorkerEventType.Graceful)
    }
    fixture.assertDecommissionTimeout(requestedTimeoutMs)
  }
}
