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
import org.apache.celeborn.common.CelebornConf.WORKER_EXCLUDED_EXPIRE_TIMEOUT
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplicationResponse
import org.apache.celeborn.common.protocol.message.StatusCode

class WorkerStatusTrackerSuite extends CelebornFunSuite {

  test("handleHeartbeatResponse") {
    val celebornConf = new CelebornConf()
    celebornConf.set(WORKER_EXCLUDED_EXPIRE_TIMEOUT, 2000L);
    val statusTracker = new WorkerStatusTracker(celebornConf, null)

    val registerTime = System.currentTimeMillis()
    statusTracker.blacklist.put(mock("host1"), (StatusCode.UNKNOWN_WORKER, registerTime));
    statusTracker.blacklist.put(mock("host2"), (StatusCode.WORKER_SHUTDOWN, registerTime));

    // test reserve (only statusCode list in handleHeartbeatResponse)
    val empty = buildResponse(Array.empty, Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(empty)

    // only reserve host1
    Assert.assertEquals(
      statusTracker.blacklist.get(mock("host1")),
      (StatusCode.UNKNOWN_WORKER, registerTime))
    Assert.assertFalse(statusTracker.blacklist.containsKey(mock("host2")))

    // add shutdown/blacklist
    val response1 = buildResponse(Array("host0"), Array("host1", "host3"), Array("host4"))
    statusTracker.handleHeartbeatResponse(response1)

    // test keep Unknown register time
    Assert.assertEquals(
      statusTracker.blacklist.get(mock("host1")),
      (StatusCode.UNKNOWN_WORKER, registerTime))

    // test new added workers
    Assert.assertTrue(statusTracker.blacklist.containsKey(mock("host0")))
    Assert.assertTrue(statusTracker.blacklist.containsKey(mock("host3")))
    Assert.assertTrue(statusTracker.blacklist.containsKey(mock("host4")))

    // test remove
    val workers = new util.HashSet[WorkerInfo]
    workers.add(mock("host4"))
    statusTracker.removeFromBlacklist(workers)
    Assert.assertFalse(statusTracker.blacklist.containsKey(mock("host4")))

    // test register time elapsed
    Thread.sleep(3000)
    val response2 = buildResponse(Array.empty, Array("host5", "host6"), Array.empty)
    statusTracker.handleHeartbeatResponse(response2)
    Assert.assertEquals(statusTracker.blacklist.size(), 2)
    Assert.assertFalse(statusTracker.blacklist.containsKey(mock("host1")))
  }

  private def buildResponse(
      blackWorkerHosts: Array[String],
      unknownWorkerHosts: Array[String],
      shuttingWorkerHosts: Array[String]): HeartbeatFromApplicationResponse = {
    val blacklist = mockWorkers(blackWorkerHosts)
    val unknownWorkers = mockWorkers(unknownWorkerHosts)
    val shuttingWorkers = mockWorkers(shuttingWorkerHosts)
    HeartbeatFromApplicationResponse(StatusCode.SUCCESS, blacklist, unknownWorkers, shuttingWorkers)
  }

  private def mockWorkers(workerHosts: Array[String]): util.ArrayList[WorkerInfo] = {
    val workers = new util.ArrayList[WorkerInfo]
    workerHosts.foreach(h => workers.add(mock(h)))
    workers
  }

  private def mock(host: String): WorkerInfo = {
    new WorkerInfo(host, -1, -1, -1, -1);
  }
}
