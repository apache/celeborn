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

import java.util.concurrent.{CountDownLatch, ExecutorService, LinkedBlockingQueue, TimeUnit}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.RandomPortSupport
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.PbCheckWorkersAvailableResponse
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckWorkersAvailable
import org.apache.celeborn.common.util.CelebornExitKind
import org.apache.celeborn.service.deploy.master.{Master, MasterArguments}
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.{AbstractScaleManager, BaseK8sScaleManager, ClusterInfo, OperationResult, ScaleManager, ScaleOperation}

class ScaleManagerExtensionSuite extends AnyFunSuite with RandomPortSupport {
  test("Master loads an external manager and invokes its lifecycle in order") {
    ExternalScaleManager.instances.clear()
    val conf = masterConf().set(SCALE_MANAGER_CLASS_NAME.key, classOf[ExternalScaleManager].getName)
    var manager: ExternalScaleManager = null
    withMaster(conf) { master =>
      manager = ExternalScaleManager.instances.poll(5, TimeUnit.SECONDS)
      assert(manager != null)
      assert(manager.running.await(5, TimeUnit.SECONDS))
      assert(manager.conf eq conf)
      assert(manager.metaManager eq master.statusSystem)
      assert(manager.events.asScala.toSeq == Seq("construct", "init", "run"))
    }
    assert(manager.stopped.await(5, TimeUnit.SECONDS))
    assert(manager.events.asScala.toSeq == Seq("construct", "init", "run", "stop"))
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
        assert(manager.running.await(5, TimeUnit.SECONDS))
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

  test("the base starts checks on run and terminates its scheduler on stop") {
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
      manager.run()
      assert(cycle.await(5, TimeUnit.SECONDS))
    } finally {
      manager.stop()
    }
    schedulerThread.join(5000)
    assert(!schedulerThread.isAlive)
  }

  test("an external K8s subclass supplies its own count policy") {
    ExternalK8sScaleManager.instances.clear()
    val conf = masterConf()
      .set(SCALE_MANAGER_CLASS_NAME.key, classOf[ExternalK8sScaleManager].getName)
      .set(AUTO_SCALE_UP_ENABLED, true)
    withMaster(conf) { master =>
      master.self.askSync[PbCheckWorkersAvailableResponse](CheckWorkersAvailable())
      val manager = ExternalK8sScaleManager.instances.poll(5, TimeUnit.SECONDS)
      assert(manager != null)
      manager.tick()
      manager.tick()
      assert(manager.operations.asScala.map(_.specWorkerCnt).toSeq == Seq(1))
      assert(master.statusSystem.lastScaleOperation.getLeft == "external-operation")
    }
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
  val running = new CountDownLatch(1)
  val stopped = new CountDownLatch(1)
  @volatile var metaManager: AbstractMetaManager = _

  events.add("construct")
  ExternalScaleManager.instances.add(this)

  override def init(metaManager: AbstractMetaManager): Unit = {
    this.metaManager = metaManager
    events.add("init")
  }

  override def run(): Unit = {
    events.add("run")
    running.countDown()
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

class ExternalK8sScaleManager(conf: CelebornConf) extends BaseK8sScaleManager(conf) {
  val operations = new LinkedBlockingQueue[ScaleOperation]()
  ExternalK8sScaleManager.instances.add(this)

  def tick(): Unit = checkScale()

  override protected def getCurrentClusterInfo(): ClusterInfo = ClusterInfo(0, Seq.empty, true)
  override protected def getWorkerIndex(worker: WorkerInfo): Int = 0
  override protected def doScaleOperation(operation: ScaleOperation): String = {
    operations.add(operation)
    "external-operation"
  }
  override protected def checkOperation(operationId: String): OperationResult =
    OperationResult(true, true)
  override protected def calculateSpecWorkerCnt(
      registeredWorkerCnt: Int,
      specWorkerCnt: Int,
      workerStats: Seq[WorkerStats],
      excludedWorkerCnt: Int,
      newExcludedWorkerCnt: Int): Int = specWorkerCnt + 1
}

object ExternalK8sScaleManager {
  val instances = new LinkedBlockingQueue[ExternalK8sScaleManager]()
}
