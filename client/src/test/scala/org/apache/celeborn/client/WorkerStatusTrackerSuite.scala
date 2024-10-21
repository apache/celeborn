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

import org.junit.Assert

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{APPLICATION_HEARTBEAT_WITH_AVAILABLE_WORKERS_ENABLE, CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT}
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplicationResponse
import org.apache.celeborn.common.protocol.message.StatusCode

class WorkerStatusTrackerSuite extends CelebornFunSuite {
  test("handleHeartbeatResponse without availableWorkers") {
    val celebornConf = new CelebornConf()
    celebornConf.set(CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT, 2000L)
    celebornConf.set(APPLICATION_HEARTBEAT_WITH_AVAILABLE_WORKERS_ENABLE, false)
    val statusTracker = new WorkerStatusTracker(celebornConf, null)

    val registerTime = System.currentTimeMillis()
    statusTracker.excludedWorkers.put(mock("host1"), (StatusCode.WORKER_UNKNOWN, registerTime))
    statusTracker.excludedWorkers.put(mock("host2"), (StatusCode.WORKER_SHUTDOWN, registerTime))

    // test reserve (only statusCode list in handleHeartbeatResponse)
    val empty = buildResponse(Array.empty, Array.empty, Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(empty)

    // only reserve host1
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1")),
      (StatusCode.WORKER_UNKNOWN, registerTime))
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host2")))

    // add shutdown/excluded worker
    val response1 =
      buildResponse(Array("host0"), Array("host1", "host3"), Array("host4"), Array.empty)
    statusTracker.handleHeartbeatResponse(response1)

    // test keep Unknown register time
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1")),
      (StatusCode.WORKER_UNKNOWN, registerTime))

    // test new added shutdown/excluded workers
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host0")))
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host3")))
    Assert.assertTrue(!statusTracker.excludedWorkers.containsKey(mock("host4")))
    Assert.assertTrue(statusTracker.shuttingWorkers.contains(mock("host4")))

    // test re heartbeat with shutdown workers
    val response2 = buildResponse(Array.empty, Array.empty, Array("host4"), Array.empty)
    statusTracker.handleHeartbeatResponse(response2)
    Assert.assertTrue(!statusTracker.excludedWorkers.containsKey(mock("host4")))
    Assert.assertTrue(statusTracker.shuttingWorkers.contains(mock("host4")))

    // test remove
    val workers = new util.HashSet[WorkerInfo]
    workers.add(mock("host3"))
    statusTracker.removeFromExcludedWorkers(workers)
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host3")))

    // test register time elapsed
    Thread.sleep(3000)
    val response3 = buildResponse(Array.empty, Array("host5", "host6"), Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(response3)
    Assert.assertEquals(statusTracker.excludedWorkers.size(), 2)
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host1")))

    // test available workers
    Assert.assertEquals(statusTracker.availableWorkers.size(), 0)
    val response4 = buildResponse(
      Array.empty,
      Array.empty,
      Array.empty,
      Array("host5", "host6", "host7", "host8"))
    statusTracker.handleHeartbeatResponse(response4)

    // availableWorkers wont update through heartbeat
    // when APPLICATION_HEARTBEAT_WITH_AVAILABLE_WORKERS_ENABLE set to false
    Assert.assertEquals(statusTracker.availableWorkers.size(), 0)
    // available workers won't overwrite excluded workers
    Assert.assertEquals(statusTracker.excludedWorkers.size(), 2)
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host5")))
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host6")))
  }

  test("handleHeartbeatResponse with availableWorkers") {
    val celebornConf = new CelebornConf()
    celebornConf.set(CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT, 2000L)
    celebornConf.set(APPLICATION_HEARTBEAT_WITH_AVAILABLE_WORKERS_ENABLE, true)
    val statusTracker = new WorkerStatusTracker(celebornConf, null)

    val registerTime = System.currentTimeMillis()
    statusTracker.excludedWorkers.put(mock("host1"), (StatusCode.WORKER_UNKNOWN, registerTime))
    statusTracker.excludedWorkers.put(mock("host2"), (StatusCode.WORKER_SHUTDOWN, registerTime))

    // test reserve (only statusCode list in handleHeartbeatResponse)
    val empty = buildResponse(Array.empty, Array.empty, Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(empty)

    // only reserve host1
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1")),
      (StatusCode.WORKER_UNKNOWN, registerTime))
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host2")))

    // add shutdown/excluded worker
    val response1 =
      buildResponse(Array("host0"), Array("host1", "host3"), Array("host4"), Array.empty)
    statusTracker.handleHeartbeatResponse(response1)

    // test keep Unknown register time
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1")),
      (StatusCode.WORKER_UNKNOWN, registerTime))
    // test new added workers
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host0")))
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host3")))
    Assert.assertTrue(!statusTracker.excludedWorkers.containsKey(mock("host4")))
    Assert.assertTrue(statusTracker.shuttingWorkers.contains(mock("host4")))

    // test re heartbeat with shutdown workers
    val response2 = buildResponse(Array.empty, Array.empty, Array("host4"), Array.empty)
    statusTracker.handleHeartbeatResponse(response2)
    Assert.assertTrue(!statusTracker.excludedWorkers.containsKey(mock("host4")))
    Assert.assertTrue(statusTracker.shuttingWorkers.contains(mock("host4")))

    // test remove
    val workers = new util.HashSet[WorkerInfo]
    workers.add(mock("host3"))
    statusTracker.removeFromExcludedWorkers(workers)
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host3")))

    // test register time elapsed
    Thread.sleep(3000)
    val response3 = buildResponse(Array.empty, Array("host5", "host6"), Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(response3)
    Assert.assertEquals(statusTracker.excludedWorkers.size(), 2)
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host1")))

    // test available workers
    Assert.assertEquals(statusTracker.availableWorkers.size(), 0)
    val response4 = buildResponse(
      Array.empty,
      Array.empty,
      Array.empty,
      Array("host5", "host6", "host7", "host8"))
    statusTracker.handleHeartbeatResponse(response4)
    Assert.assertEquals(statusTracker.availableWorkers.size(), 2)
    // available workers won't overwrite excluded workers
    Assert.assertEquals(statusTracker.excludedWorkers.size(), 2)
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host5")))
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host6")))

    // test re heartbeat with available workers
    val response5 = buildResponse(Array.empty, Array.empty, Array.empty, Array("host8", "host9"))
    statusTracker.handleHeartbeatResponse(response5)
    Assert.assertEquals(statusTracker.availableWorkers.size(), 2)
    Assert.assertFalse(statusTracker.availableWorkers.contains(mock("host7")))
    Assert.assertTrue(statusTracker.availableWorkers.contains(mock("host8")))
    Assert.assertTrue(statusTracker.availableWorkers.contains(mock("host9")))
  }

  private def buildResponse(
      excludedWorkerHosts: Array[String],
      unknownWorkerHosts: Array[String],
      shuttingWorkerHosts: Array[String],
      availableWorkerHosts: Array[String]): HeartbeatFromApplicationResponse = {
    val excludedWorkers = mockWorkers(excludedWorkerHosts)
    val unknownWorkers = mockWorkers(unknownWorkerHosts)
    val shuttingWorkers = mockWorkers(shuttingWorkerHosts)
    val availableWorkers = mockWorkers(availableWorkerHosts)
    HeartbeatFromApplicationResponse(
      StatusCode.SUCCESS,
      excludedWorkers,
      unknownWorkers,
      shuttingWorkers,
      availableWorkers)
  }

  private def mockWorkers(workerHosts: Array[String]): util.ArrayList[WorkerInfo] = {
    val workers = new util.ArrayList[WorkerInfo]
    workerHosts.foreach(h => workers.add(mock(h)))
    workers
  }

  private def mock(host: String): WorkerInfo = {
    new WorkerInfo(host, -1, -1, -1, -1)
  }
}
