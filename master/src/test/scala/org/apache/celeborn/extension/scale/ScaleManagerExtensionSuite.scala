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

package org.apache.celeborn.extension.scale

import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.{CountDownLatch, ExecutorService, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.RandomPortSupport
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.exception.CelebornException
import org.apache.celeborn.common.protocol.PbCheckWorkersAvailableResponse
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckWorkersAvailable
import org.apache.celeborn.common.util.CelebornExitKind
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.{AbstractScaleManager, ScaleManager}

class ScaleManagerExtensionSuite extends AnyFunSuite with RandomPortSupport {
  test("Master loads an external manager and invokes its lifecycle in order") {
    ExternalScaleManager.instances.clear()
    val conf = masterConf().set(SCALE_MANAGER_CLASS_NAME.key, classOf[ExternalScaleManager].getName)
    var manager: ExternalScaleManager = null
    withMaster(conf) { master =>
      manager = ExternalScaleManager.instances.poll(5, TimeUnit.SECONDS)
      assert(manager != null)
      assert(manager.started.await(5, TimeUnit.SECONDS))
      assert(manager.conf eq conf)
      assert(manager.metaManager eq master.statusSystem)
      assert(manager.events.asScala.toSeq == Seq("construct", "init", "start"))
    }
    assert(manager.stopped.await(5, TimeUnit.SECONDS))
    assert(manager.events.asScala.toSeq == Seq("construct", "init", "start", "stop"))
  }

  test("Master starts without a scale manager by default") {
    ExternalScaleManager.instances.clear()
    val conf = masterConf()
    assert(conf.get(SCALE_MANAGER_CLASS_NAME).isEmpty)
    withMaster(conf) { master =>
      val response = master.self.askSync[PbCheckWorkersAvailableResponse](CheckWorkersAvailable())
      assert(!response.getAvailable)
      assert(ExternalScaleManager.instances.isEmpty)
    }
  }

  test("Master cleans up its executor when the scale manager fails to stop") {
    ExternalScaleManager.instances.clear()
    val conf =
      masterConf().set(SCALE_MANAGER_CLASS_NAME.key, classOf[FailingStopScaleManager].getName)
    var executor: ExecutorService = null
    try {
      withMaster(conf) { master =>
        val manager = ExternalScaleManager.instances.poll(5, TimeUnit.SECONDS)
        assert(manager != null)
        assert(manager.started.await(5, TimeUnit.SECONDS))
        val field = classOf[Master].getDeclaredField("forwardMessageThread")
        field.setAccessible(true)
        executor = field.get(master).asInstanceOf[ExecutorService]
        assert(!executor.isShutdown)
      }
      assert(executor.isShutdown)
      assert(executor.awaitTermination(5, TimeUnit.SECONDS))
    } finally {
      Option(executor).foreach(_.shutdownNow())
    }
  }

  test("the base starts checks on start and terminates its scheduler on stop") {
    val conf = new CelebornConf(false).set(SCALE_CHECK_INTERVAL.key, "10ms")
    val cycle = new CountDownLatch(1)
    var schedulerThread: Thread = null
    val manager = new AbstractScaleManager(conf) {
      override protected def checkScale(): Unit = {
        schedulerThread = Thread.currentThread()
        cycle.countDown()
      }
    }
    try {
      assert(!cycle.await(100, TimeUnit.MILLISECONDS))
      manager.start()
      assert(cycle.await(5, TimeUnit.SECONDS))
    } finally {
      manager.stop()
    }
    schedulerThread.join(5000)
    assert(!schedulerThread.isAlive)
  }

  test("the base handles lifecycle edges and continues after failed checks") {
    val conf = new CelebornConf(false).set(SCALE_CHECK_INTERVAL.key, "10ms")
    val idleManager = new AbstractScaleManager(conf) {
      override protected def checkScale(): Unit = {}
    }
    idleManager.stop()
    idleManager.stop()
    intercept[IllegalStateException](idleManager.start())

    val checks = new AtomicInteger(0)
    val recovered = new CountDownLatch(2)
    val manager = new AbstractScaleManager(conf) {
      override protected def checkScale(): Unit = {
        val count = checks.incrementAndGet()
        recovered.countDown()
        if (count == 1) {
          throw new IllegalStateException("transient check failure")
        }
      }
    }
    try {
      manager.start()
      manager.start()
      assert(recovered.await(5, TimeUnit.SECONDS))
    } finally {
      manager.stop()
      manager.stop()
    }
  }

  test("the base rejects non-positive check intervals on construction") {
    for (interval <- Seq("0ms", "-1ms", "500us")) {
      val conf = new CelebornConf(false).set(SCALE_CHECK_INTERVAL.key, interval)
      val error = intercept[IllegalArgumentException] {
        new AbstractScaleManager(conf) {
          override protected def checkScale(): Unit = {}
        }
      }
      assert(error.getMessage.contains(SCALE_CHECK_INTERVAL.key))
    }
  }

  test("Master fails fast on invalid scale manager configuration before binding ports") {
    val occupiedSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
    try {
      def occupiedConf(): CelebornConf = {
        masterConf().set(MASTER_PORT, occupiedSocket.getLocalPort)
      }

      val blankConf = occupiedConf().set(SCALE_MANAGER_CLASS_NAME.key, "   ")
      val blankError = intercept[IllegalArgumentException] {
        new Master(blankConf, new MasterArguments(Array.empty, blankConf))
      }
      assert(blankError.getMessage.contains(SCALE_MANAGER_CLASS_NAME.key))

      val wrongTypeConf =
        occupiedConf().set(SCALE_MANAGER_CLASS_NAME.key, classOf[NonScaleManager].getName)
      val wrongTypeError = intercept[IllegalArgumentException] {
        new Master(wrongTypeConf, new MasterArguments(Array.empty, wrongTypeConf))
      }
      assert(wrongTypeError.getMessage.contains(SCALE_MANAGER_CLASS_NAME.key))
      assert(wrongTypeError.getMessage.contains(classOf[ScaleManager].getName))

      val invalidIntervalConf = occupiedConf()
        .set(SCALE_MANAGER_CLASS_NAME.key, classOf[IntervalCheckingScaleManager].getName)
        .set(SCALE_CHECK_INTERVAL.key, "500us")
      val intervalError = intercept[CelebornException] {
        new Master(invalidIntervalConf, new MasterArguments(Array.empty, invalidIntervalConf))
      }
      val cause = rootCause(intervalError)
      assert(cause.isInstanceOf[IllegalArgumentException])
      assert(cause.getMessage.contains(SCALE_CHECK_INTERVAL.key))
    } finally {
      occupiedSocket.close()
    }
  }

  private def rootCause(t: Throwable): Throwable = {
    var current = t
    while (current.getCause != null && current.getCause != current) {
      current = current.getCause
    }
    current
  }

  private def masterConf(): CelebornConf = {
    new CelebornConf(false)
      .set(HA_ENABLED, false)
      .set(METRICS_ENABLED, false)
      .set(MASTER_HOST, "localhost")
      .set(MASTER_PORT, selectRandomPort())
      .set(PORT_MAX_RETRY, 0)
      .set(RPC_DISPATCHER_THREADS, 1)
      .set(ENDPOINT_VERIFIER_SEPARATE_ENABLED, false)
  }

  private def withMaster(conf: CelebornConf)(testBody: Master => Unit): Unit = {
    val master = new Master(conf, new MasterArguments(Array.empty, conf))
    try {
      testBody(master)
    } finally {
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.rpcEnv.shutdown()
      master.rpcEnv.awaitTermination()
    }
  }
}

class ExternalScaleManager(val conf: CelebornConf) extends ScaleManager {
  val events = new LinkedBlockingQueue[String]()
  val started = new CountDownLatch(1)
  val stopped = new CountDownLatch(1)
  @volatile var metaManager: AbstractMetaManager = _

  events.add("construct")
  ExternalScaleManager.instances.add(this)

  override def init(metaManager: AbstractMetaManager): Unit = {
    this.metaManager = metaManager
    events.add("init")
  }

  override def start(): Unit = {
    events.add("start")
    started.countDown()
  }

  override def stop(): Unit = {
    events.add("stop")
    stopped.countDown()
  }
}

object ExternalScaleManager {
  val instances = new LinkedBlockingQueue[ExternalScaleManager]()
}

class FailingStopScaleManager(conf: CelebornConf) extends ExternalScaleManager(conf) {
  override def stop(): Unit = {
    super.stop()
    throw new IllegalStateException("ScaleManager stop failed")
  }
}

class IntervalCheckingScaleManager(conf: CelebornConf) extends AbstractScaleManager(conf) {
  override protected def checkScale(): Unit = {}
}

class NonScaleManager(val conf: CelebornConf)
