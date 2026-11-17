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
import java.util.concurrent.{CountDownLatch, FutureTask, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.RandomPortSupport
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.{WorkerInfo, WorkerStatus}
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.{PbRegisterWorker, PbWorkerLost}
import org.apache.celeborn.common.protocol.message.ControlMessages.{HeartbeatFromWorker, HeartbeatFromWorkerResponse}
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.CelebornExitKind

class WorkerStatsHeartbeatSuite extends AnyFunSuite
  with BeforeAndAfterAll with BeforeAndAfterEach with RandomPortSupport {

  private class AdmissionGate {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
  }

  private class GatedMaster(conf: CelebornConf)
    extends Master(conf, new MasterArguments(Array.empty, conf)) {
    val nextAdmission = new AtomicReference[AdmissionGate]()

    override def executeWithLeaderChecker[T](context: RpcCallContext, f: => T): Unit =
      super.executeWithLeaderChecker(
        context, {
          val gate = if (context == null) null else nextAdmission.getAndSet(null)
          if (gate != null) {
            gate.entered.countDown()
            assert(gate.release.await(10, TimeUnit.SECONDS))
          }
          f
        })
  }

  private var master: GatedMaster = _
  private val worker = new WorkerInfo("localhost", 19101, 19102, 19103, 19104)
  private val stats = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.5))

  private def currentStats: Option[WorkerStats] =
    Option(master.statusSystem.workerStats.get(worker)).flatMap(ref => Option(ref.get()))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new CelebornConf()
      .set(CelebornConf.METRICS_ENABLED, false)
      .set(CelebornConf.MASTER_HOST, "localhost")
      .set(CelebornConf.MASTER_PORT, selectRandomPort())
      .set(CelebornConf.PORT_MAX_RETRY, 0)
    master = new GatedMaster(conf)
  }

  override def afterAll(): Unit = {
    try {
      if (master != null) {
        master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
        master.rpcEnv.shutdown()
      }
    } finally {
      super.afterAll()
    }
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
    val restarted = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.25))
    assert(heartbeat(Some(restarted)).registered)
    assert(currentStats.contains(restarted))
    lose()
    assert(currentStats.isEmpty)
    assert(!heartbeat(Some(stats)).registered)
  }

  private def whileHeartbeatBlocked(value: Option[WorkerStats])(action: => Unit): Unit = {
    val gate = new AdmissionGate
    assert(master.nextAdmission.compareAndSet(null, gate))
    val task = new FutureTask[HeartbeatFromWorkerResponse](() => heartbeat(value))
    val thread = new Thread(task, "worker-stats-heartbeat-test")
    thread.setDaemon(true)
    try {
      thread.start()
      assert(gate.entered.await(10, TimeUnit.SECONDS))
      action
    } finally {
      gate.release.countDown()
      task.get(10, TimeUnit.SECONDS)
      thread.join(10000)
      assert(!thread.isAlive)
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
      val restarted = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.25))
      whileHeartbeatBlocked(value) {
        register()
        assert(currentStats.isEmpty)
        heartbeat(Some(restarted))
      }
      assert(currentStats.contains(restarted))
    }
  }

  for (value <- Seq(Some(stats), None)) {
    test(s"an earlier heartbeat with $value cannot replace a later observation") {
      register()
      heartbeat(Some(stats))
      val fresh = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.75))
      whileHeartbeatBlocked(value) {
        assert(heartbeat(Some(fresh)).registered)
      }
      assert(currentStats.contains(fresh))
    }
  }

  for (value <- Seq(Some(stats), None)) {
    test(s"a heartbeat with $value from before invalidation cannot change fresh observations") {
      register()
      heartbeat(Some(stats))
      val fresh = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.75))
      whileHeartbeatBlocked(value) {
        master.statusSystem.invalidateWorkerStats()
        assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
        assert(currentStats.isEmpty)
        assert(heartbeat(Some(fresh)).registered)
      }
      assert(currentStats.contains(fresh))
    }
  }

  test("snapshot restoration clears observations and accepts fresh worker heartbeats") {
    register()
    heartbeat(Some(stats))
    val fresh = WorkerStats(Map(WorkerStats.MemoryPressureRatio -> 0.75))
    val snapshot = Files.createTempFile("worker-stats", ".snapshot").toFile
    try {
      master.statusSystem.writeMetaInfoToFile(snapshot)
      whileHeartbeatBlocked(Some(stats)) {
        master.statusSystem.restoreMetaFromFile(snapshot)
        assert(master.statusSystem.workersMap.containsKey(worker.toUniqueId))
        assert(currentStats.isEmpty)
        assert(heartbeat(Some(fresh)).registered)
      }
      assert(currentStats.contains(fresh))
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
      whileHeartbeatBlocked(Some(stats)) {
        master.statusSystem.restoreMetaFromFile(snapshot)
        assert(currentStats.isEmpty)
      }
      assert(!master.statusSystem.workersMap.containsKey(worker.toUniqueId))
      assert(!master.statusSystem.workerStats.containsKey(worker))
      assert(currentStats.isEmpty)
    } finally {
      Files.deleteIfExists(snapshot.toPath)
    }
  }
}
