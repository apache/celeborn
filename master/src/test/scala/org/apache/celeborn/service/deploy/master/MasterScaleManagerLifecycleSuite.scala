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

package org.apache.celeborn.service.deploy.master

import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.RandomPortSupport
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf._
import org.apache.celeborn.common.protocol.PbCheckWorkersAvailableResponse
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckWorkersAvailable
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.{CelebornExitKind, ThreadUtils}
import org.apache.celeborn.service.deploy.master.clustermeta.AbstractMetaManager
import org.apache.celeborn.service.deploy.master.scale.ScaleManager

class MasterScaleManagerLifecycleSuite extends AnyFunSuite with RandomPortSupport {
  import MasterScaleManagerLifecycleSuite._

  for (phase <- Seq("init", "start", "init-and-stop")) {
    test(s"Master keeps dispatching concurrently when ScaleManager $phase fails") {
      instances.clear()
      val conf = masterConf(phase)
      val rpcEntered = new CountDownLatch(1)
      val releaseRpc = new CountDownLatch(1)
      val master = new Master(conf, new MasterArguments(Array.empty, conf)) {
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case BlockRpc =>
            rpcEntered.countDown()
            require(releaseRpc.await(15, TimeUnit.SECONDS))
            context.reply(true)
          case message => super.receiveAndReply(context)(message)
        }
      }
      val manager = instances.poll(5, TimeUnit.SECONDS)
      val expectedEvents =
        if (phase == "start") Seq("init", "start", "stop") else Seq("init", "stop")
      try {
        assert(manager != null)
        val blockedRpc = master.self.ask[Boolean](BlockRpc)
        assert(rpcEntered.await(5, TimeUnit.SECONDS))
        val reply = master.self.ask[PbCheckWorkersAvailableResponse](CheckWorkersAvailable())
        assert(!ThreadUtils.awaitResult(reply, 5.seconds).getAvailable)
        assert(manager.events.asScala.toList == expectedEvents)
        releaseRpc.countDown()
        assert(ThreadUtils.awaitResult(blockedRpc, 5.seconds))
      } finally {
        releaseRpc.countDown()
        stopMaster(master)
      }
      assert(manager.events.asScala.toList == expectedEvents)
    }
  }

  test("Stopping a dual-port Master during ScaleManager init skips start and waits for stop") {
    instances.clear()
    val conf = masterConf("block-init")
      .set(INTERNAL_PORT_ENABLED, true)
      .set(MASTER_INTERNAL_PORT, selectRandomPort())
    val master = new Master(conf, new MasterArguments(Array.empty, conf))
    val manager = instances.poll(5, TimeUnit.SECONDS)
    try {
      assert(manager != null)
      assert(manager.initEntered.await(5, TimeUnit.SECONDS))
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      assert(!manager.stopped.await(1, TimeUnit.SECONDS), "stop ran while init was blocked")
    } finally {
      Option(manager).foreach(_.releaseInit.countDown())
      stopMaster(master)
    }
    assert(manager.stopped.await(5, TimeUnit.SECONDS))
    assert(manager.events.asScala.toList == Seq("init", "init-completed", "stop"))
  }

  test("Stopping a dual-port Master during ScaleManager start waits before calling stop") {
    instances.clear()
    val conf = masterConf("block-start")
      .set(INTERNAL_PORT_ENABLED, true)
      .set(MASTER_INTERNAL_PORT, selectRandomPort())
    val master = new Master(conf, new MasterArguments(Array.empty, conf))
    val manager = instances.poll(5, TimeUnit.SECONDS)
    try {
      assert(manager != null)
      assert(manager.startEntered.await(5, TimeUnit.SECONDS))
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      assert(!manager.stopped.await(1, TimeUnit.SECONDS), "stop ran while start was blocked")
    } finally {
      Option(manager).foreach(_.releaseStart.countDown())
      stopMaster(master)
    }
    assert(manager.stopped.await(5, TimeUnit.SECONDS))
    assert(manager.events.asScala.toList == Seq("init", "start", "start-completed", "stop"))
  }

  test("Scale check interval must be positive after conversion to milliseconds") {
    val conf = new CelebornConf(false)
    assert(conf.get(SCALE_CHECK_INTERVAL) == 30000L)
    conf.set(SCALE_CHECK_INTERVAL.key, "1ms")
    assert(conf.get(SCALE_CHECK_INTERVAL) == 1L)
    for (interval <- Seq("0ms", "-1ms", "1us", "500us")) {
      conf.set(SCALE_CHECK_INTERVAL.key, interval)
      val error = intercept[IllegalArgumentException](conf.get(SCALE_CHECK_INTERVAL))
      assert(error.getMessage.contains(SCALE_CHECK_INTERVAL.key))
    }
  }

  private def masterConf(mode: String): CelebornConf = {
    new CelebornConf(false)
      .set(HA_ENABLED, false)
      .set(METRICS_ENABLED, false)
      .set(MASTER_HOST, "localhost")
      .set(MASTER_PORT, selectRandomPort())
      .set(PORT_MAX_RETRY, 0)
      .set(RPC_DISPATCHER_THREADS, 4)
      .set(ENDPOINT_VERIFIER_SEPARATE_ENABLED, false)
      .set(SCALE_MANAGER_CLASS_NAME, classOf[LifecycleTestScaleManager].getName)
      .set(ModeKey, mode)
  }

  private def stopMaster(master: Master): Unit = {
    master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
    val rpcEnvs = Seq(master.rpcEnv, master.internalRpcEnvInUse).distinct
    rpcEnvs.foreach(_.shutdown())
    rpcEnvs.foreach(_.awaitTermination())
  }
}

private object MasterScaleManagerLifecycleSuite {
  val ModeKey = "celeborn.test.scale.manager.mode"
  val instances = new LinkedBlockingQueue[LifecycleTestScaleManager]()
  case object BlockRpc
}

class LifecycleTestScaleManager(conf: CelebornConf) extends ScaleManager {
  import MasterScaleManagerLifecycleSuite._

  private val mode = conf.get(ModeKey)
  val events = new LinkedBlockingQueue[String]()
  val initEntered = new CountDownLatch(1)
  val releaseInit = new CountDownLatch(1)
  val startEntered = new CountDownLatch(1)
  val releaseStart = new CountDownLatch(1)
  val stopped = new CountDownLatch(1)
  instances.add(this)

  override def init(metaManager: AbstractMetaManager): Unit = {
    events.add("init")
    initEntered.countDown()
    if (mode == "init" || mode == "init-and-stop") {
      throw new IllegalStateException("ScaleManager init failed")
    }
    if (mode == "block-init") {
      require(releaseInit.await(15, TimeUnit.SECONDS))
      events.add("init-completed")
    }
  }

  override def start(): Unit = {
    events.add("start")
    startEntered.countDown()
    if (mode == "start") {
      throw new IllegalStateException("ScaleManager start failed")
    }
    if (mode == "block-start") {
      require(releaseStart.await(15, TimeUnit.SECONDS))
      events.add("start-completed")
    }
  }

  override def stop(): Unit = {
    events.add("stop")
    stopped.countDown()
    if (mode == "init-and-stop") {
      throw new IllegalStateException("ScaleManager stop failed")
    }
  }
}
