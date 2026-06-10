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

package org.apache.celeborn.client

import java.util
import java.util.concurrent.ThreadPoolExecutor

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, mock, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.client.listener.WorkerStatusListener
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{
  CLIENT_BATCH_HANDLE_COMMIT_PARTITION_ENABLED,
  CLIENT_PUSH_REPLICATE_ENABLED,
  CLIENT_SHUFFLE_DATA_LOST_ON_UNKNOWN_WORKER_ENABLED
}
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.protocol.PartitionType
import org.apache.celeborn.common.protocol.message.ControlMessages.{
  GetReducerFileGroupResponse,
  HeartbeatFromApplicationResponse
}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcAddress
import org.apache.celeborn.common.rpc.netty.LocalNettyRpcCallContext
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

class CommitManagerSuite extends CelebornFunSuite {

  // Background daemon pools are created inside CommitManager; skip thread audit.
  override protected val enableAutoThreadAudit = false

  private var rpcPool: ThreadPoolExecutor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rpcPool = ThreadUtils.newDaemonCachedThreadPool("test-cm-rpc")
  }

  override def afterAll(): Unit = {
    if (rpcPool != null) rpcPool.shutdownNow()
    super.afterAll()
  }

  private def worker(host: String): WorkerInfo = new WorkerInfo(host, 1, 2, 3, 4)

  private def pendingContext(): (LocalNettyRpcCallContext, Promise[Any]) = {
    val p = Promise[Any]()
    (new LocalNettyRpcCallContext(RpcAddress("localhost", 0), p), p)
  }

  private def makeManager(
      conf: CelebornConf,
      allocatedWorkers: ShuffleAllocatedWorkers): (CommitManager, WorkerStatusTracker) = {
    val tracker = new WorkerStatusTracker(conf, null)
    val lm = mock(classOf[LifecycleManager])

    doAnswer(new Answer[Unit] {
      override def answer(inv: InvocationOnMock): Unit =
        tracker.registerWorkerStatusListener(inv.getArgument[WorkerStatusListener](0))
    }).when(lm).registerWorkerStatusListener(any(classOf[WorkerStatusListener]))

    when(lm.shuffleAllocatedWorkers).thenReturn(allocatedWorkers)
    when(lm.getPartitionType(anyInt())).thenReturn(PartitionType.REDUCE)
    when(lm.workerStatusTracker).thenReturn(tracker)
    when(lm.rpcSharedThreadPool).thenReturn(rpcPool)

    val mgr = new CommitManager("test-app", conf, lm)
    mgr.start()
    (mgr, tracker)
  }

  private def baseConf(
      dataLostEnabled: Boolean = true,
      replicateEnabled: Boolean = false): CelebornConf = {
    val c = new CelebornConf()
    c.set(CLIENT_SHUFFLE_DATA_LOST_ON_UNKNOWN_WORKER_ENABLED, dataLostEnabled)
    c.set(CLIENT_PUSH_REPLICATE_ENABLED, replicateEnabled)
    c.set(CLIENT_BATCH_HANDLE_COMMIT_PARTITION_ENABLED, false)
    c
  }

  private def unknownHeartbeat(tracker: WorkerStatusTracker, workers: WorkerInfo*): Unit =
    tracker.handleHeartbeatResponse(HeartbeatFromApplicationResponse(
      StatusCode.SUCCESS,
      new util.ArrayList[WorkerInfo](),
      new util.ArrayList[WorkerInfo](workers.asJava),
      new util.ArrayList[WorkerInfo](),
      new util.ArrayList[Integer](),
      null))

  private def allocate(
      alloc: ShuffleAllocatedWorkers,
      shuffleId: Int,
      w: WorkerInfo): Unit = {
    val m = JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
    m.put(w.toUniqueId, new ShufflePartitionLocationInfo(w))
    alloc.put(shuffleId, m)
  }

  test("UnknownWorkerListener replies SHUFFLE_DATA_LOST to pending GetReducerFileGroup when worker goes unknown") {
    val w = worker("crashed")
    val alloc = new ShuffleAllocatedWorkers()
    val shuffleId = 1
    allocate(alloc, shuffleId, w)

    val (mgr, tracker) = makeManager(baseConf(), alloc)
    mgr.registerShuffle(shuffleId, 2, false, 4)

    val (ctx, promise) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx, shuffleId, SerdeVersion.V1)
    assert(!promise.isCompleted, "request must be pending before heartbeat")

    unknownHeartbeat(tracker, w)

    assert(promise.isCompleted)
    val resp = Await.result(promise.future, 1.second).asInstanceOf[GetReducerFileGroupResponse]
    assert(resp.status == StatusCode.SHUFFLE_DATA_LOST)
  }

  test("UnknownWorkerListener is a no-op when replication is enabled") {
    val w = worker("crashed")
    val alloc = new ShuffleAllocatedWorkers()
    val shuffleId = 2
    allocate(alloc, shuffleId, w)

    val (mgr, tracker) = makeManager(baseConf(replicateEnabled = true), alloc)
    mgr.registerShuffle(shuffleId, 2, false, 4)

    val (ctx, promise) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx, shuffleId, SerdeVersion.V1)

    unknownHeartbeat(tracker, w)
    assert(!promise.isCompleted)
  }

  test("UnknownWorkerListener is a no-op when feature is disabled") {
    val w = worker("crashed")
    val alloc = new ShuffleAllocatedWorkers()
    val shuffleId = 3
    allocate(alloc, shuffleId, w)

    val (mgr, tracker) = makeManager(baseConf(dataLostEnabled = false), alloc)
    mgr.registerShuffle(shuffleId, 2, false, 4)

    val (ctx, promise) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx, shuffleId, SerdeVersion.V1)

    unknownHeartbeat(tracker, w)
    assert(!promise.isCompleted)
  }

  test("UnknownWorkerListener is a no-op when the crashed worker holds no shuffle data") {
    val dataWorker = worker("healthy")
    val crashedWorker = worker("crashed")
    val alloc = new ShuffleAllocatedWorkers()
    val shuffleId = 4
    allocate(alloc, shuffleId, dataWorker)

    val (mgr, tracker) = makeManager(baseConf(), alloc)
    mgr.registerShuffle(shuffleId, 2, false, 4)

    val (ctx, promise) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx, shuffleId, SerdeVersion.V1)

    unknownHeartbeat(tracker, crashedWorker)

    assert(!promise.isCompleted)
  }

  test("UnknownWorkerListener marks data lost even when stage already ended (post-commit crash)") {
    // The write-side commit succeeded before the crash, so stage ended as SUCCESS.
    // But committed data on a crashed worker is unreadable — restarted reducer tasks
    // must get SHUFFLE_DATA_LOST immediately rather than discovering it mid-read.
    val w = worker("crashed-after-commit")
    val alloc = new ShuffleAllocatedWorkers()
    val shuffleId = 5
    allocate(alloc, shuffleId, w)

    val (mgr, tracker) = makeManager(baseConf(), alloc)
    mgr.registerShuffle(shuffleId, 1, false, 2)

    mgr.setStageEnd(shuffleId)
    assert(mgr.isStageEnd(shuffleId))
    assert(!mgr.getCommitHandler(shuffleId).isStageDataLost(shuffleId))

    unknownHeartbeat(tracker, w)

    assert(mgr.getCommitHandler(shuffleId).isStageDataLost(shuffleId))

    val (ctx, promise) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx, shuffleId, SerdeVersion.V1)
    assert(promise.isCompleted)
    val resp = Await.result(promise.future, 1.second).asInstanceOf[GetReducerFileGroupResponse]
    assert(resp.status == StatusCode.SHUFFLE_DATA_LOST)
  }

  test("UnknownWorkerListener only fast-fails shuffles whose data is on the crashed worker") {
    val crashedWorker = worker("crashed")
    val healthyWorker = worker("healthy")
    val alloc = new ShuffleAllocatedWorkers()
    val affectedId = 10
    val unaffectedId = 11
    allocate(alloc, affectedId, crashedWorker)
    allocate(alloc, unaffectedId, healthyWorker)

    val (mgr, tracker) = makeManager(baseConf(), alloc)
    mgr.registerShuffle(affectedId, 2, false, 4)
    mgr.registerShuffle(unaffectedId, 2, false, 4)

    val (ctx1, p1) = pendingContext()
    val (ctx2, p2) = pendingContext()
    mgr.handleGetReducerFileGroup(ctx1, affectedId, SerdeVersion.V1)
    mgr.handleGetReducerFileGroup(ctx2, unaffectedId, SerdeVersion.V1)

    unknownHeartbeat(tracker, crashedWorker)

    assert(p1.isCompleted)
    assert(
      Await.result(p1.future, 1.second).asInstanceOf[GetReducerFileGroupResponse].status
        == StatusCode.SHUFFLE_DATA_LOST)
    assert(!p2.isCompleted)
  }
}
