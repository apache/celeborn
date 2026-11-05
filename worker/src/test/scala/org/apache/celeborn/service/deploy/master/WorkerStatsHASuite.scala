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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.example.metrics.ControlledWorkerStatsCollector
import org.apache.ratis.protocol.TransferLeadershipRequest
import org.apache.ratis.rpc.CallId
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}

import org.apache.celeborn.RandomPortSupport
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.client.MasterClient
import org.apache.celeborn.common.meta.WorkerStatus
import org.apache.celeborn.common.metrics.WorkerStats
import org.apache.celeborn.common.protocol.RpcNameConstants
import org.apache.celeborn.common.protocol.message.ControlMessages.{HeartbeatFromWorker, HeartbeatFromWorkerResponse}
import org.apache.celeborn.common.rpc.{RpcAddress, RpcCallContext, RpcEndpointRef}
import org.apache.celeborn.common.util.{CelebornExitKind, JavaUtils, Utils}
import org.apache.celeborn.service.deploy.master.clustermeta.ha.HAMasterMetaManager
import org.apache.celeborn.service.deploy.worker.WorkerStatsTestWorker
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class WorkerStatsHASuite extends AnyFunSuite with Eventually with RandomPortSupport {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(25, Millis))

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
            assert(gate.release.await(30, TimeUnit.SECONDS), "Heartbeat admission was not released")
          }
          f
        })
  }

  private class Cluster(internalPorts: Boolean) extends AutoCloseable {
    private val root = Files.createTempDirectory("worker-stats-ha")
    val masters = mutable.ArrayBuffer.empty[GatedMaster]
    private val stoppedMasters = mutable.Set.empty[GatedMaster]
    private val gates = mutable.ArrayBuffer.empty[AdmissionGate]
    var worker: WorkerStatsTestWorker = _
    var collector: ControlledWorkerStatsCollector = _
    private var workerThread: Thread = _
    private var workerTask: FutureTask[Unit] = _

    def raft(master: Master) =
      master.statusSystem.asInstanceOf[HAMasterMetaManager].getRatisServer

    def leader: GatedMaster = {
      var found: Seq[GatedMaster] = Seq.empty
      eventually {
        found = masters.filterNot(stoppedMasters).filter { master =>
          val info = raft(master).getServer.getDivision(raft(master).getGroupId).getInfo
          info.isLeader && info.isLeaderReady
        }.toSeq
        assert(found.size == 1, "Expected exactly one ready Raft leader")
      }
      found.head
    }

    def start(): Unit = {
      ControlledWorkerStatsCollector.instances.clear()
      val nodes = (1 to 3).map { id =>
        (id.toString, selectRandomPort(), selectRandomPort(), selectRandomPort())
      }
      nodes.foreach { case (id, _, _, _) =>
        val conf = new CelebornConf()
          .set(CelebornConf.HA_ENABLED, true)
          .set(CelebornConf.HA_MASTER_NODE_ID.key, id)
          .set(CelebornConf.INTERNAL_PORT_ENABLED, internalPorts)
          .set(CelebornConf.METRICS_ENABLED, false)
          .set(CelebornConf.RPC_IO_THREADS.key, "2")
          .set(CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key, "2m")
          .set(CelebornConf.HA_MASTER_RATIS_STORAGE_DIR.key, root.resolve(id).toString)
          .set(CelebornConf.HA_MASTER_RATIS_UNEXPECTED_CLOSE_ACTION.key, "none")
          .set(CelebornConf.PORT_MAX_RETRY, 0)
        nodes.foreach { case (peer, port, internalPort, raftPort) =>
          conf.set(s"celeborn.master.ha.node.$peer.host", "localhost")
          conf.set(s"celeborn.master.ha.node.$peer.port", port.toString)
          conf.set(s"celeborn.master.ha.node.$peer.internal.port", internalPort.toString)
          conf.set(s"celeborn.master.ha.node.$peer.ratis.port", raftPort.toString)
        }
        masters += new GatedMaster(conf)
      }
      val firstLeader = leader
      val bootstrap = masters.find(_ ne firstLeader).get
      val bootstrapInternalPort =
        if (internalPorts) bootstrap.masterArgs.internalPort else bootstrap.masterArgs.port
      val conf = new CelebornConf()
        .set(CelebornConf.METRICS_ENABLED, false)
        .set(CelebornConf.INTERNAL_PORT_ENABLED, internalPorts)
        .set(CelebornConf.MASTER_ENDPOINTS, Seq(s"localhost:${bootstrap.masterArgs.port}"))
        .set(
          CelebornConf.MASTER_INTERNAL_ENDPOINTS,
          Seq(s"localhost:$bootstrapInternalPort"))
        .set(CelebornConf.WORKER_STORAGE_DIRS, Seq(root.resolve("worker").toString))
        .set(CelebornConf.WORKER_DISK_MONITOR_ENABLED, false)
        .set(CelebornConf.WORKER_HTTP_PORT, selectRandomPort())
        .set(CelebornConf.WORKER_HEARTBEAT_TIMEOUT.key, "4s")
        .set(CelebornConf.RPC_IO_THREADS.key, "2")
        .set(CelebornConf.MASTER_CLIENT_RPC_ASK_TIMEOUT.key, "2s")
        .set(CelebornConf.SCALE_METRICS_ENABLED, true)
        .set(
          CelebornConf.SCALE_METRIC_COLLECTOR_CLASS_NAME,
          classOf[ControlledWorkerStatsCollector].getName)
      worker = new WorkerStatsTestWorker(conf)
      workerTask = new FutureTask[Unit](() => worker.initialize())
      workerThread = new Thread(workerTask, "ha-worker-stats-worker")
      workerThread.setDaemon(true)
      workerThread.start()
      eventually {
        if (workerTask.isDone) workerTask.get(1, TimeUnit.SECONDS)
        assert(worker.initialized.get(), "Worker did not initialize through a follower bootstrap")
        assertRegistered()
      }
      collector = ControlledWorkerStatsCollector.instances.poll(10, TimeUnit.SECONDS)
      assert(collector != null, "Worker did not initialize its collector")
    }

    def assertRegistered(): Unit =
      masters.filterNot(stoppedMasters).foreach { master =>
        assert(master.statusSystem.workersMap.containsKey(worker.workerInfo.toUniqueId))
      }

    def stats(master: Master): Option[WorkerStats] =
      Option(master.statusSystem.workerStats.get(worker.workerInfo)).flatMap(ref =>
        Option(ref.get()))

    def report(master: Master, value: Option[WorkerStats]): Unit = {
      collector.report(value)
      eventually {
        assert(stats(master) == value, "Worker heartbeat did not publish the expected observation")
      }
    }

    def endpoint(master: Master): RpcEndpointRef = {
      val port = if (internalPorts) master.masterArgs.internalPort else master.masterArgs.port
      val name =
        if (internalPorts) RpcNameConstants.MASTER_INTERNAL_EP else RpcNameConstants.MASTER_EP
      worker.rpcEnv.setupEndpointRef(RpcAddress("localhost", port), name)
    }

    def heartbeat(value: Option[WorkerStats]): HeartbeatFromWorker = {
      val info = worker.workerInfo
      HeartbeatFromWorker(
        info.host,
        info.rpcPort,
        info.pushPort,
        info.fetchPort,
        info.replicatePort,
        worker.storageManager.allDisksSnapshot(),
        Collections.emptyMap(),
        Collections.emptySet(),
        false,
        WorkerStatus.normalWorkerStatus(),
        value,
        MasterClient.genRequestId())
    }

    def transfer(from: Master, to: Master): Unit = {
      val old = raft(from)
      val next = raft(to)
      val reply = old.getServer.transferLeadership(new TransferLeadershipRequest(
        old.getClientId,
        old.getServer.getId,
        old.getGroupId,
        CallId.getAndIncrement(),
        next.getServer.getId,
        10000L))
      assert(reply.isSuccess, "Raft leadership transfer failed")
      eventually {
        assert(leader eq to)
        assert(!old.getServer.getDivision(old.getGroupId).getInfo.isLeader)
      }
      assertRegistered()
    }

    def pauseNextAdmission(master: GatedMaster): AdmissionGate = {
      val gate = new AdmissionGate
      gates += gate
      assert(master.nextAdmission.compareAndSet(null, gate))
      gate
    }

    def stopMaster(master: GatedMaster): Unit = {
      master.stop(CelebornExitKind.EXIT_IMMEDIATELY)
      master.internalRpcEnvInUse.shutdown()
      if (master.internalRpcEnvInUse ne master.rpcEnv) master.rpcEnv.shutdown()
      stoppedMasters += master
    }

    override def close(): Unit = {
      gates.foreach(_.release.countDown())
      if (worker != null) {
        Utils.tryLogNonFatalError(worker.stop(CelebornExitKind.EXIT_IMMEDIATELY))
        worker.shutdownRpcEnvs()
        if (workerThread != null) {
          workerThread.interrupt()
          workerThread.join(10000)
          assert(!workerThread.isAlive, "Worker initializer did not terminate")
        }
        MemoryManager.reset()
      }
      masters.filterNot(stoppedMasters).reverse.foreach(stopMaster)
      JavaUtils.deleteRecursively(root.toFile)
      usedPorts.clear()
    }
  }

  private def withCluster(internalPorts: Boolean)(test: Cluster => Unit): Unit = {
    val cluster = new Cluster(internalPorts)
    try {
      cluster.start()
      test(cluster)
    } finally {
      cluster.close()
    }
  }

  private def pressure(value: Double): WorkerStats =
    WorkerStats(Map(WorkerStats.MemoryPressureRatio -> value))

  private def withDelayedHeartbeat(
      cluster: Cluster,
      master: GatedMaster,
      value: Option[WorkerStats])(whileDelayed: => Unit): HeartbeatFromWorkerResponse = {
    val gate = cluster.pauseNextAdmission(master)
    val pending = new FutureTask[HeartbeatFromWorkerResponse](() =>
      cluster.endpoint(master).askSync[HeartbeatFromWorkerResponse](cluster.heartbeat(value)))
    val thread = new Thread(pending, "delayed-worker-heartbeat")
    thread.setDaemon(true)
    thread.start()
    try {
      assert(gate.entered.await(10, TimeUnit.SECONDS), "Master never admitted the heartbeat")
      whileDelayed
      gate.release.countDown()
      pending.get(10, TimeUnit.SECONDS)
    } finally {
      gate.release.countDown()
      thread.join(10000)
      assert(!thread.isAlive, "Delayed heartbeat did not terminate")
    }
  }

  for (internalPorts <- Seq(false, true)) {
    test(s"heartbeats admitted before a leadership round trip cannot replace fresh stats, internal=$internalPorts") {
      withCluster(internalPorts) { cluster =>
        val cases = Seq(
          (Some(pressure(0.1)), None, true),
          (Some(pressure(0.1)), Some(pressure(0.7)), false),
          (None, Some(pressure(0.7)), false))
        for ((oldValue, freshValue, alreadyKnowsB) <- cases) {
          val a = cluster.leader
          val b = cluster.masters.find(_ ne a).get
          cluster.report(a, Some(pressure(0.2)))
          val response = withDelayedHeartbeat(cluster, a, oldValue) {
            cluster.transfer(a, b)
            assert(cluster.stats(a).isEmpty)
            if (alreadyKnowsB) {
              // No redirect heartbeat may reach A and accidentally detach its old reference.
              val response = cluster.endpoint(b).askSync[HeartbeatFromWorkerResponse](
                cluster.heartbeat(Some(pressure(0.9))))
              assert(response.registered)
              assert(cluster.stats(b).contains(pressure(0.9)))
            } else {
              cluster.report(b, Some(pressure(0.9)))
            }
            eventually {
              val id = cluster.worker.workerInfo.toUniqueId
              assert(a.statusSystem.workersMap.get(id).lastHeartbeat ==
                b.statusSystem.workersMap.get(id).lastHeartbeat)
            }
            assert(cluster.stats(a).isEmpty, "Follower must not inherit B's local observations")
            cluster.transfer(b, a)
            assert(cluster.stats(a).isEmpty)
            freshValue.foreach(value => cluster.report(a, Some(value)))
          }
          assert(response.registered)
          assert(
            cluster.stats(a) == freshValue,
            "A heartbeat from the previous epoch repopulated or replaced current observations")
        }
      }
    }
  }

  test("an older heartbeat finishing late cannot replace a newer observation in the same term") {
    withCluster(internalPorts = true) { cluster =>
      val master = cluster.leader
      for (oldValue <- Seq(Some(pressure(0.1)), None)) {
        cluster.report(master, Some(pressure(0.2)))
        val response = withDelayedHeartbeat(cluster, master, oldValue) {
          cluster.report(master, Some(pressure(0.7)))
        }
        assert(response.registered)
        assert(
          cluster.stats(master).contains(pressure(0.7)),
          "Completion order must not replace the latest admitted observation")
      }
    }
  }

}
