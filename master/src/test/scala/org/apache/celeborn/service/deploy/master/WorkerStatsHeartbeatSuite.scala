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

import java.nio.file.Files
import java.util.Collections
import java.util.concurrent.{FutureTask, TimeUnit}

import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.{WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.{PbRegisterWorker, PbWorkerLost}
import org.apache.celeborn.common.protocol.message.ControlMessages.{HeartbeatFromWorker, HeartbeatFromWorkerResponse}
import org.apache.celeborn.common.rpc.RpcCallContext

class WorkerStatsHeartbeatSuite extends AnyFunSuite
  with BeforeAndAfterAll with BeforeAndAfterEach with Eventually with MasterClusterFeature {

  private var master: Master = _
  private val worker = new WorkerInfo("localhost", 19101, 19102, 19103, 19104)
  private val stats = WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> 0.5))

  private def currentStats: Option[WorkerStats] =
    Option(master.statusSystem.workerStats.get(worker)).flatMap(ref => Option(ref.get()))

  override def beforeAll(): Unit = {
    master = setupMasterWithRandomPort()
  }

  override def afterAll(): Unit = {
    if (master != null) shutdownMaster()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    lose()
  }

  private def lose(): Unit = {
    master.receiveAndReply(mock[RpcCallContext])(
      PbWorkerLost.newBuilder().setHost(worker.host).setRpcPort(worker.rpcPort)
        .setPushPort(worker.pushPort).setFetchPort(worker.fetchPort)
        .setReplicatePort(worker.replicatePort).setRequestId(MasterClient.genRequestId()).build())
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
    assert(currentStats.isEmpty)
    register()
    assert(heartbeat(Some(stats)).registered)
    assert(currentStats.contains(stats))
    assert(heartbeat(None).registered)
    assert(currentStats.isEmpty)
    heartbeat(Some(stats))
    register()
    assert(currentStats.isEmpty)
    val restarted = WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> 0.25))
    assert(heartbeat(Some(restarted)).registered)
    assert(currentStats.contains(restarted))
    lose()
    assert(currentStats.isEmpty)
    assert(!heartbeat(Some(stats)).registered)
  }

  private def whileHeartbeatBlocked(value: Option[WorkerStats])(action: => Unit): Unit = {
    val task = new FutureTask[HeartbeatFromWorkerResponse](() => heartbeat(value))
    val thread = new Thread(task, "worker-stats-heartbeat-test")
    thread.setDaemon(true)
    try {
      master.statusSystem.workersMap.synchronized {
        thread.start()
        // Pause after the registration check, at the existing metadata membership lock.
        eventually(timeout(Span(10, Seconds)), interval(Span(10, Millis))) {
          assert(thread.getState == Thread.State.BLOCKED)
          assert(thread.getStackTrace.exists(_.getMethodName == "updateWorkerHeartbeatMeta"))
        }
        action
      }
    } finally {
      task.get(10, TimeUnit.SECONDS)
    }
  }

  test("a heartbeat overlapping worker loss cannot recreate its stats") {
    register()
    heartbeat(Some(stats))
    whileHeartbeatBlocked(Some(stats)) {
      lose()
      assert(currentStats.isEmpty)
    }
    assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))
    assert(currentStats.isEmpty)
  }

  for (value <- Seq(Some(stats), None)) {
    test(s"a heartbeat with $value from before re-registration cannot change the new stats") {
      register()
      val restarted = WorkerStats(Map(WorkerStats.NettyMemoryUsedRatio -> 0.25))
      whileHeartbeatBlocked(value) {
        register()
        assert(currentStats.isEmpty)
        heartbeat(Some(restarted))
      }
      assert(currentStats.contains(restarted))
    }
  }

  test("snapshot restoration clears observations and accepts fresh worker heartbeats") {
    register()
    heartbeat(Some(stats))
    val oldStatsRef = master.statusSystem.workerStats.get(worker)
    val snapshot = Files.createTempFile("worker-stats", ".snapshot").toFile
    try {
      master.statusSystem.writeMetaInfoToFile(snapshot)
      master.statusSystem.restoreMetaFromFile(snapshot)
      assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      oldStatsRef.set(stats)
      assert(currentStats.isEmpty)
      assert(heartbeat(Some(stats)).registered)
      assert(currentStats.contains(stats))
    } finally {
      Files.deleteIfExists(snapshot.toPath)
    }
  }

  test("snapshot restoration removes stats for workers absent from the snapshot") {
    val snapshot = Files.createTempFile("worker-stats-absent", ".snapshot").toFile
    try {
      master.statusSystem.writeMetaInfoToFile(snapshot)
      register()
      heartbeat(Some(stats))
      val oldStatsRef = master.statusSystem.workerStats.get(worker)
      master.statusSystem.restoreMetaFromFile(snapshot)
      oldStatsRef.set(stats)
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(!master.statusSystem.workerStats.containsKey(worker))
      assert(currentStats.isEmpty)
    } finally {
      Files.deleteIfExists(snapshot.toPath)
    }
  }
}
