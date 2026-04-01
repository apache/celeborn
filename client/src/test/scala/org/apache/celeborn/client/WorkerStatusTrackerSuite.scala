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
import java.util.concurrent.{CyclicBarrier, Executors, Future, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.junit.Assert

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.{CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT, CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED}
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.message.ControlMessages.HeartbeatFromApplicationResponse
import org.apache.celeborn.common.protocol.message.StatusCode

class WorkerStatusTrackerSuite extends CelebornFunSuite {
  test("handleHeartbeatResponse without availableWorkers") {
    val celebornConf = new CelebornConf()
    celebornConf.set(CLIENT_EXCLUDED_WORKER_EXPIRE_TIMEOUT, 2000L)
    celebornConf.set(CLIENT_SHUFFLE_DYNAMIC_RESOURCE_ENABLED, false)
    val statusTracker = new WorkerStatusTracker(celebornConf, null)

    val registerTime = System.currentTimeMillis()
    statusTracker.excludedWorkers.put(mock("host1"), (StatusCode.WORKER_UNRESPONSIVE, registerTime))
    statusTracker.excludedWorkers.put(mock("host2"), (StatusCode.WORKER_SHUTDOWN, registerTime))

    // test reserve (only statusCode list in handleHeartbeatResponse)
    val empty = buildResponse(Array.empty, Array.empty, Array.empty)
    statusTracker.handleHeartbeatResponse(empty)

    // only reserve host1
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1")),
      (StatusCode.WORKER_UNRESPONSIVE, registerTime))
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host2")))

    // add shutdown/excluded worker
    val response1 =
      buildResponse(Array("host0"), Array("host1", "host3"), Array("host4"))
    statusTracker.handleHeartbeatResponse(response1)

    // test keep Unknown register time
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host1")))
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host1"))._1,
      StatusCode.WORKER_UNKNOWN)
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host3")))
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host3"))._1,
      StatusCode.WORKER_UNKNOWN)

    // test new added shutdown/excluded workers
    Assert.assertTrue(statusTracker.excludedWorkers.containsKey(mock("host0")))
    Assert.assertEquals(
      statusTracker.excludedWorkers.get(mock("host0"))._1,
      StatusCode.WORKER_EXCLUDED)
    Assert.assertTrue(!statusTracker.excludedWorkers.containsKey(mock("host4")))
    Assert.assertTrue(statusTracker.shuttingWorkers.contains(mock("host4")))

    // test re heartbeat with shutdown workers
    val response2 = buildResponse(Array.empty, Array.empty, Array("host4"))
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
    val response3 = buildResponse(Array.empty, Array("host5", "host6"), Array.empty)
    statusTracker.handleHeartbeatResponse(response3)
    Assert.assertEquals(statusTracker.excludedWorkers.size(), 2)
    Assert.assertFalse(statusTracker.excludedWorkers.containsKey(mock("host1")))
  }

  test("concurrent access to shuttingWorkers should not throw ConcurrentModificationException") {
    val celebornConf = new CelebornConf()
    val statusTracker = new WorkerStatusTracker(celebornConf, null)
    val numWriters = 5
    val numReaders = 5
    val totalThreads = numWriters + numReaders
    val executor = Executors.newFixedThreadPool(totalThreads)
    val barrier = new CyclicBarrier(totalThreads)
    val errors = new AtomicInteger(0)
    val futures = new ArrayBuffer[Future[_]]()

    // Pre-populate the set so iteration takes longer, increasing the
    // window for concurrent modification to trigger a CME with HashSet.
    (1 to 1000).foreach { i =>
      statusTracker.shuttingWorkers.add(mock(s"pre-$i"))
    }

    try {
      // Writers: concurrently add and remove workers
      (1 to numWriters).foreach { i =>
        futures += executor.submit(new Runnable {
          override def run(): Unit = {
            barrier.await()
            (1 to 1000).foreach { j =>
              val worker = mock(s"host-$i-$j")
              statusTracker.shuttingWorkers.add(worker)
              statusTracker.shuttingWorkers.remove(worker)
            }
          }
        })
      }

      // Readers: iterate shuttingWorkers directly, mirroring how currentFailedWorkers iterates it
      (1 to numReaders).foreach { i =>
        futures += executor.submit(new Runnable {
          override def run(): Unit = {
            try {
              barrier.await()
              (1 to 1000).foreach { _ =>
                statusTracker.shuttingWorkers.asScala.foreach(_ => ())
              }
            } catch {
              case _: java.util.ConcurrentModificationException =>
                errors.incrementAndGet()
            }
          }
        })
      }

      // Surface any unexpected exceptions from threads
      futures.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdown()
      Assert.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS))
    }

    Assert.assertEquals(
      "ConcurrentModificationException should not occur with thread-safe set",
      0,
      errors.get())
  }

  private def buildResponse(
      excludedWorkerHosts: Array[String],
      unknownWorkerHosts: Array[String],
      shuttingWorkerHosts: Array[String]): HeartbeatFromApplicationResponse = {
    val excludedWorkers = mockWorkers(excludedWorkerHosts)
    val unknownWorkers = mockWorkers(unknownWorkerHosts)
    val shuttingWorkers = mockWorkers(shuttingWorkerHosts)
    HeartbeatFromApplicationResponse(
      StatusCode.SUCCESS,
      excludedWorkers,
      unknownWorkers,
      shuttingWorkers,
      new util.ArrayList[Integer](),
      null)
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
