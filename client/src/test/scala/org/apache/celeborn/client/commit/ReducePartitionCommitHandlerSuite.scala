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

package org.apache.celeborn.client.commit

import java.util.concurrent.{ScheduledExecutorService, ThreadPoolExecutor}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.WorkerStatusTracker
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.network.protocol.SerdeVersion
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcAddress
import org.apache.celeborn.common.rpc.netty.LocalNettyRpcCallContext
import org.apache.celeborn.common.util.ThreadUtils

class ReducePartitionCommitHandlerSuite extends CelebornFunSuite {

  // The handler spins up daemon pools; skip the thread audit to avoid flaky leak warnings.
  override protected val enableAutoThreadAudit = false

  private var rpcPool: ThreadPoolExecutor = _
  private var commitScheduler: ScheduledExecutorService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rpcPool = ThreadUtils.newDaemonCachedThreadPool("test-reduce-commit-rpc")
    commitScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("test-reduce-commit-scheduler")
  }

  override def afterAll(): Unit = {
    if (rpcPool != null) rpcPool.shutdownNow()
    if (commitScheduler != null) commitScheduler.shutdownNow()
    super.afterAll()
  }

  private def newHandler(): ReducePartitionCommitHandler = {
    val conf = new CelebornConf()
    new ReducePartitionCommitHandler(
      "test-app",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      rpcPool,
      commitScheduler,
      null)
  }

  private def pendingContext(): (LocalNettyRpcCallContext, Promise[Any]) = {
    val p = Promise[Any]()
    (new LocalNettyRpcCallContext(RpcAddress("localhost", 0), p), p)
  }

  test("markShuffleDataLost replies SHUFFLE_DATA_LOST to GetReducerFileGroup contexts") {
    val handler = newHandler()
    val shuffleId = 1
    handler.registerShuffle(shuffleId, numMappers = 2, isSegmentGranularityVisible = false, numPartitions = 4)

    val (ctx1, p1) = pendingContext()
    handler.handleGetReducerFileGroup(ctx1, shuffleId, SerdeVersion.V1)
    assert(!handler.isStageEnd(shuffleId))
    assert(!handler.isStageDataLost(shuffleId))

    handler.markShuffleDataLost(shuffleId)

    val (ctx2, p2) = pendingContext()
    handler.handleGetReducerFileGroup(ctx2, shuffleId, SerdeVersion.V1)
    assert(handler.isStageEnd(shuffleId))
    assert(handler.isStageDataLost(shuffleId))

    Seq(p1, p2).foreach { p =>
      assert(p.isCompleted)
      val resp = Await.result(p.future, 1.second).asInstanceOf[GetReducerFileGroupResponse]
      assert(resp.status == StatusCode.SHUFFLE_DATA_LOST)
    }
  }

  test("markShuffleDataLost marks data lost even when stage already ended (worker crash after commit)") {
    val handler = newHandler()
    val shuffleId = 1
    handler.registerShuffle(shuffleId, numMappers = 1, isSegmentGranularityVisible = false, numPartitions = 2)

    // Clean stage-end
    handler.setStageEnd(shuffleId)
    assert(handler.isStageEnd(shuffleId))
    assert(!handler.isStageDataLost(shuffleId))

    // Worker crashes after commit
    handler.markShuffleDataLost(shuffleId)
    assert(handler.isStageDataLost(shuffleId))
  }
}
