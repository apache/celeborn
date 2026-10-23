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

import org.mockito.Mockito.mock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.{LifecycleManager, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.JavaUtils

class ReducePartitionCommitHandlerSuite extends CelebornFunSuite {
  private val shuffleId = 1

  test("mark data lost when an allocated worker becomes unknown") {
    // Heartbeats may report a different WorkerInfo instance for the same worker identity.
    val handler = newHandler(worker("allocated"))
    assert(handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost when an unrelated worker becomes unknown") {
    val handler = newHandler(worker("unrelated"))
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost when unknown-worker detection is disabled") {
    val handler = newHandler(worker("allocated"), detectionEnabled = false)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost on an unknown worker when replication is enabled") {
    val handler = newHandler(worker("allocated"), replicationEnabled = true)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost for an allocated worker with another failure status") {
    val handler = newHandler(worker("allocated"), status = StatusCode.WORKER_UNRESPONSIVE)
    assert(!handler.isStageDataLost(shuffleId))
  }

  test("do not mark data lost for a shuffle without allocated workers") {
    val handler = newHandler(worker("allocated"))
    assert(!handler.isStageDataLost(shuffleId + 1))
  }

  private def newHandler(
      excludedWorker: WorkerInfo,
      detectionEnabled: Boolean = true,
      replicationEnabled: Boolean = false,
      status: StatusCode = StatusCode.WORKER_UNKNOWN): ReducePartitionCommitHandler = {
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_SHUFFLE_DATA_LOST_ON_UNKNOWN_WORKER_ENABLED, detectionEnabled)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED, replicationEnabled)
    val allocatedWorker = worker("allocated")
    val workers = JavaUtils.newConcurrentHashMap[String, ShufflePartitionLocationInfo]()
    workers.put(allocatedWorker.toUniqueId, new ShufflePartitionLocationInfo(allocatedWorker))
    val allocatedWorkers = new ShuffleAllocatedWorkers()
    allocatedWorkers.put(shuffleId, workers)
    val statusTracker = new WorkerStatusTracker(conf, null)
    statusTracker.excludedWorkers.put(excludedWorker, (status, System.currentTimeMillis()))

    new ReducePartitionCommitHandler(
      "test-app",
      conf,
      allocatedWorkers,
      new CommittedPartitionInfo(),
      statusTracker,
      mock(classOf[ThreadPoolExecutor]),
      mock(classOf[ScheduledExecutorService]),
      mock(classOf[LifecycleManager]))
  }

  private def worker(host: String): WorkerInfo =
    new WorkerInfo(host, 1001, 1002, 1003, 1004)
}
