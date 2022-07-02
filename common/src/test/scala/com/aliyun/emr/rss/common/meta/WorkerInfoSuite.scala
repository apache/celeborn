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

package com.aliyun.emr.rss.common.meta

import java.util.{ArrayList => jArrayList}
import java.util.{Map => jMap}
import java.util.concurrent.{Future, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.junit.Assert.{assertEquals, assertNotEquals, assertNotNull}

import com.aliyun.emr.RssFunSuite
import com.aliyun.emr.rss.common.util.ThreadUtils

class WorkerInfoSuite extends RssFunSuite {

  test("test") {
    def run(block: () => Unit = () => {}): Unit = {
      block()
    }
    val block = () => {
      println("inside")
    }
    run(block)
  }

  test("Serialize/Deserialize WorkerInfo to String correctly.") {
    val (h1, p1, p2, p3, a1, b1) = ("h1", 1, 2, 3, 10, 11)
    val (h2, p4, p5, p6, a2, b2) = ("h2", 4, 5, 6, 20, 21)
    val (h3, p7, p8, p9, a3, b3) = ("h3", 7, 8, 9, 30, 31)

    val pbList = new jArrayList[String](3)
    pbList.add(WorkerInfo.encodeToPbStr(h1, p1, p2, p3, a1, b1))
    pbList.add(WorkerInfo.encodeToPbStr(h2, p4, p5, p6, a2, b2))
    pbList.add(WorkerInfo.encodeToPbStr(h3, p7, p8, p9, a3, b3))

    val workerInfos = WorkerInfo.decodeFromPbMessage(pbList)
    assertEquals(
      "The number of WorkerInfo decoded from string is wrong.", pbList.size(), workerInfos.size())

    check(h1, p1, p2, p3, b1, workerInfos)
    check(h2, p4, p5, p6, b2, workerInfos)
    check(h3, p7, p8, p9, b3, workerInfos)
  }

  private def check(
                     host: String, rpcPort: Int, pushPort: Int, fetchPort: Int
                     , allocateSize: Int, workerInfos: jMap[WorkerInfo, Integer]): Unit = {
    val worker = new WorkerInfo(host, rpcPort, pushPort, fetchPort, -1 , -1, null)
    val realWorker = workerInfos.get(worker)
    assertNotNull(s"Worker $worker didn't exist.", realWorker)
    assertEquals(allocateSize, realWorker.intValue())
  }

  test("multi-thread modify same WorkerInfo.") {
    val numSlots = 10000
    val worker = new WorkerInfo("localhost", 10000, 10001, 10002, -1 , numSlots, null)

    val allocatedSlots = new AtomicInteger(0)
    val shuffleKey = "appId-shuffleId"
    val es = ThreadUtils.newDaemonFixedThreadPool(8, "workerInfo-unit-test")

    val futures = new ArrayBuffer[Future[_]]()
    (0 until 8).foreach { _ =>
      futures += es.submit(new Runnable {
        override def run(): Unit = {
          val rand = ThreadLocalRandom.current()
          while (true) {
            val allocatedSlot = allocatedSlots.get()
            if (allocatedSlot >= numSlots) {
              return
            }
            var requireSlot = rand.nextInt(100)
            val newAllocatedSlot = Math.min(numSlots, allocatedSlot + requireSlot)
            requireSlot = newAllocatedSlot - allocatedSlot
            if (allocatedSlots.compareAndSet(allocatedSlot, newAllocatedSlot)) {
              worker.allocateSlots(shuffleKey, requireSlot)
            }
          }
        }
      })
    }
    futures.foreach(_.get())
    futures.clear()

    assertEquals(numSlots, allocatedSlots.get())
    assertEquals(numSlots, worker.usedSlots())
    assertEquals(0, worker.freeSlots())

    (0 until 8).foreach { _ =>
      futures += es.submit(new Runnable {
        override def run(): Unit = {
          val rand = ThreadLocalRandom.current()
          while (true) {
            val allocatedSlot = allocatedSlots.get()
            if (allocatedSlot <= 0) {
              return
            }
            var releaseSlot = rand.nextInt(100)
            val newAllocatedSlot = Math.max(0, allocatedSlot - releaseSlot)
            releaseSlot = allocatedSlot - newAllocatedSlot
            if (allocatedSlots.compareAndSet(allocatedSlot, newAllocatedSlot)) {
              worker.releaseSlots(shuffleKey, releaseSlot)
            }
          }
          worker.releaseSlots(shuffleKey)
        }
      })
    }
    futures.foreach(_.get())
    futures.clear()

    assertEquals(0, allocatedSlots.get())
    assertEquals(0, worker.usedSlots())
    assertEquals(numSlots, worker.freeSlots())

    ThreadUtils.shutdown(es, 800.millisecond)
  }

  test("WorkerInfo not equals when host different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, -1 , null)
    val worker2 = new WorkerInfo("h2", 10001, 10002, 10003, 1000, -1 , null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when rpc port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, -1 , null)
    val worker2 = new WorkerInfo("h1", 20001, 10002, 10003, 1000, -1 , null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when push port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, -1 , null)
    val worker2 = new WorkerInfo("h1", 10001, 20002, 10003, 1000, -1 , null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when fetch port different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, -1 , null)
    val worker2 = new WorkerInfo("h1", 10001, 10002, 20003, 1000, -1 , null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo equals when numSlots different.") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, -1 , null)
    val worker2 = new WorkerInfo("h1", 10001, 10002, 10003, 2000, -1 , null)
    assertEquals(worker1, worker2)
  }
}
