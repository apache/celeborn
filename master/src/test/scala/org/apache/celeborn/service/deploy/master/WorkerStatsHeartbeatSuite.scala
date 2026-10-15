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

import java.util.Collections

import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.{WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.{PbRegisterWorker, PbWorkerLost}
import org.apache.celeborn.common.protocol.message.ControlMessages.{HeartbeatFromWorker, HeartbeatFromWorkerResponse}
import org.apache.celeborn.common.rpc.RpcCallContext

class WorkerStatsHeartbeatSuite extends AnyFunSuite
  with BeforeAndAfterAll with MasterClusterFeature {

  private var master: Master = _
  private val worker = new WorkerInfo("localhost", 19101, 19102, 19103, 19104)
  private val stats = WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> 0.5))

  override def beforeAll(): Unit = {
    master = setupMasterWithRandomPort()
  }

  override def afterAll(): Unit = {
    if (master != null) shutdownMaster()
  }

  private def register(): Unit = {
    master.receiveAndReply(mock[RpcCallContext])(
      PbRegisterWorker.newBuilder().setHost(worker.host).setRpcPort(worker.rpcPort)
        .setPushPort(worker.pushPort).setFetchPort(worker.fetchPort)
        .setReplicatePort(worker.replicatePort).setRequestId(MasterClient.genRequestId()).build())
  }

  private def heartbeat(value: Option[WorkerStats]): HeartbeatFromWorkerResponse = {
    val context = mock[RpcCallContext]
    master.receiveAndReply(context)(HeartbeatFromWorker(
      worker.host,
      worker.rpcPort,
      worker.pushPort,
      worker.fetchPort,
      worker.replicatePort,
      Seq.empty,
      Collections.emptyMap(),
      Collections.emptySet(),
      false,
      WorkerStatus.normalWorkerStatus(),
      value))
    val response = ArgumentCaptor.forClass(classOf[Any])
    verify(context).reply(response.capture())
    response.getValue.asInstanceOf[HeartbeatFromWorkerResponse]
  }

  test("worker registration, missing metrics, restart and removal govern metric retention") {
    assert(!heartbeat(Some(stats)).registered)
    assert(master.workerStatsStore.get(worker).isEmpty)
    register()
    assert(heartbeat(Some(stats)).registered)
    assert(master.workerStatsStore.get(worker).contains(stats))
    val extended = WorkerStats(stats.metrics ++ Map(
      "" -> 1.0,
      "NaN" -> Double.NaN,
      "PositiveInfinity" -> Double.PositiveInfinity,
      "NegativeInfinity" -> Double.NegativeInfinity))
    assert(heartbeat(Some(extended)).registered)
    val retained = master.workerStatsStore.get(worker).get.metrics
    assert(retained.keySet == extended.metrics.keySet)
    assert(retained(WorkerStats.NettyMemoryUsedRatio) == 0.5)
    assert(retained("") == 1.0)
    assert(retained("NaN").isNaN)
    assert(retained("PositiveInfinity") == Double.PositiveInfinity)
    assert(retained("NegativeInfinity") == Double.NegativeInfinity)
    assert(heartbeat(None).registered)
    assert(master.workerStatsStore.get(worker).isEmpty)
    heartbeat(Some(stats))
    register()
    assert(master.workerStatsStore.get(worker).isEmpty)
    val restarted = WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> 0.25))
    assert(heartbeat(Some(restarted)).registered)
    assert(master.workerStatsStore.get(worker).contains(restarted))
    master.receiveAndReply(mock[RpcCallContext])(
      PbWorkerLost.newBuilder().setHost(worker.host).setRpcPort(worker.rpcPort)
        .setPushPort(worker.pushPort).setFetchPort(worker.fetchPort)
        .setReplicatePort(worker.replicatePort).setRequestId(MasterClient.genRequestId()).build())
    assert(master.workerStatsStore.get(worker).isEmpty)
    assert(!heartbeat(Some(stats)).registered)
  }
}
