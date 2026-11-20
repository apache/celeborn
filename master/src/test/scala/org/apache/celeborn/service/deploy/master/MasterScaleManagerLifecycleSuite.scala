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
  instances.add(this)

  override def init(metaManager: AbstractMetaManager): Unit = {
    events.add("init")
    if (mode == "init" || mode == "init-and-stop") {
      throw new IllegalStateException("ScaleManager init failed")
    }
  }

  override def start(): Unit = {
    events.add("start")
    if (mode == "start") {
      throw new IllegalStateException("ScaleManager start failed")
    }
  }

  override def stop(): Unit = {
    events.add("stop")
    if (mode == "init-and-stop") {
      throw new IllegalStateException("ScaleManager stop failed")
    }
  }
}
