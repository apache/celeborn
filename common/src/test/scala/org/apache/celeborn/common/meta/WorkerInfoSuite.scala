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

package org.apache.celeborn.common.meta

import java.util
import java.util.{Map => jMap}
import java.util.concurrent.{Future, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.junit.Assert.{assertEquals, assertNotEquals, assertNotNull}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.metrics.source.Role
import org.apache.celeborn.common.protocol.TransportModuleConstants
import org.apache.celeborn.common.quota.ResourceConsumption
import org.apache.celeborn.common.rpc.{RpcAddress, RpcEndpointAddress, RpcEnv}
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

class WorkerInfoSuite extends CelebornFunSuite {

  test("test") {
    def run(block: () => Unit = () => {}): Unit = {
      block()
    }
    val block = () => {
      println("inside")
    }
    run(block)
  }

  private def check(
      host: String,
      rpcPort: Int,
      pushPort: Int,
      fetchPort: Int,
      replicatePort: Int,
      internalPort: Int,
      workerInfos: jMap[WorkerInfo, util.Map[String, Integer]],
      allocationMap: util.Map[String, Integer]): Unit = {
    val worker =
      new WorkerInfo(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort)
    val realWorker = workerInfos.get(worker)
    assertNotNull(s"Worker $worker didn't exist.", realWorker)
  }

  test("multi-thread modify same WorkerInfo.") {
    val numSlots = 10000
    val disks = new util.HashMap[String, DiskInfo]()
    disks.put("disk1", new DiskInfo("disk1", Int.MaxValue, 1, 1, 0))
    disks.put("disk2", new DiskInfo("disk2", Int.MaxValue, 1, 1, 0))
    disks.put("disk3", new DiskInfo("disk3", Int.MaxValue, 1, 1, 0))
    val userResourceConsumption =
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption]()
    userResourceConsumption.put(UserIdentifier("tenant1", "name1"), ResourceConsumption(1, 1, 1, 1))
    val worker =
      new WorkerInfo(
        "localhost",
        10000,
        10001,
        10002,
        10003,
        10004,
        disks,
        userResourceConsumption)

    val allocatedSlots = new AtomicInteger(0)
    val shuffleKey = "appId-1"
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
              val allocationMap = new util.HashMap[String, Integer]()
              allocationMap.put("disk1", requireSlot)
              worker.allocateSlots(shuffleKey, allocationMap)
            }
          }
        }
      })
    }
    futures.foreach(_.get())
    futures.clear()

    assertEquals(numSlots, allocatedSlots.get())
    assertEquals(numSlots, worker.usedSlots())

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
              val allocations = new util.HashMap[String, Integer]()
              allocations.put("disk1", releaseSlot)
              worker.releaseSlots(shuffleKey, allocations)
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

    ThreadUtils.shutdown(es)
  }

  test("WorkerInfo not equals when host different.") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h2", 10001, 10002, 10003, 1000, 10004, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when rpc port different.") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h1", 20001, 10002, 10003, 1000, 10004, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when push port different.") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h1", 10001, 20002, 10003, 1000, 10004, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when fetch port different.") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h1", 10001, 10002, 20003, 1000, 10004, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo not equals when replicate port different.") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h1", 10001, 10002, 10003, 2000, 10004, null, null)
    assertNotEquals(worker1, worker2)
  }

  test("WorkerInfo equals when diskInfos different") {
    val worker1 = new WorkerInfo(
      "h1",
      10001,
      10002,
      10003,
      1000,
      10004,
      new util.HashMap[String, DiskInfo](),
      null)
    val worker2 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    assertEquals(worker1, worker2)
  }

  test("WorkerInfo equals when userResourceConsumption different") {
    val worker1 = new WorkerInfo(
      "h1",
      10001,
      10002,
      10003,
      1000,
      10004,
      null,
      new util.HashMap[UserIdentifier, ResourceConsumption]())
    val worker2 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    assertEquals(worker1, worker2)
  }

  test("WorkerInfo equals when endpoint different") {
    val worker1 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    val worker2 =
      new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004, null, null)
    assertEquals(worker1, worker2)
  }

  test("WorkerInfo toString output") {
    val worker1 = new WorkerInfo("h1", 10001, 10002, 10003, 1000, 10004)
    val worker2 =
      new WorkerInfo("h2", 20001, 20002, 20003, 2000, 20004, null, null)
    worker2.networkLocation_$eq("/1")

    val worker3 = new WorkerInfo(
      "h3",
      30001,
      30002,
      30003,
      3000,
      30004,
      new util.HashMap[String, DiskInfo](),
      null)

    val disks = new util.HashMap[String, DiskInfo]()
    val diskInfo1 = new DiskInfo("disk1", Int.MaxValue, 1, 1, 10)
    val diskInfo2 = new DiskInfo("disk2", Int.MaxValue, 2, 2, 20)
    val diskInfo3 = new DiskInfo("disk3", Int.MaxValue, 3, 3, 30)
    diskInfo1.setTotalSpace(Int.MaxValue)
    diskInfo2.setTotalSpace(Int.MaxValue)
    diskInfo3.setTotalSpace(Int.MaxValue)

    disks.put("disk1", diskInfo1)
    disks.put("disk2", diskInfo2)
    disks.put("disk3", diskInfo3)
    val userResourceConsumption =
      JavaUtils.newConcurrentHashMap[UserIdentifier, ResourceConsumption]()
    userResourceConsumption.put(
      UserIdentifier("tenant1", "name1"),
      ResourceConsumption(
        20971520,
        1,
        52428800,
        1,
        Map(
          "application_1697697127390_2171854" -> ResourceConsumption(
            20971520,
            1,
            52428800,
            1)).asJava))
    val conf = new CelebornConf()
    val endpointAddress = new RpcEndpointAddress(new RpcAddress("localhost", 12345), "mockRpc")
    var rpcEnv: RpcEnv = null
    try {
      rpcEnv = RpcEnv.create(
        "mockEnv",
        TransportModuleConstants.RPC_SERVICE_MODULE,
        "localhost",
        12345,
        conf,
        64,
        Role.WORKER,
        None,
        None)
      val worker4 = new WorkerInfo(
        "h4",
        40001,
        40002,
        40003,
        4000,
        40004,
        disks,
        userResourceConsumption)

      val placeholder = ""
      val exp1 =
        s"""
           |Host: h1
           |RpcPort: 10001
           |PushPort: 10002
           |FetchPort: 10003
           |ReplicatePort: 1000
           |InternalPort: 10004
           |SlotsUsed: 0
           |LastHeartbeat: 0
           |Disks: empty
           |UserResourceConsumption: empty
           |WorkerRef: null
           |NetworkLocation: /default-rack
           |""".stripMargin

      val exp2 =
        """
          |Host: h2
          |RpcPort: 20001
          |PushPort: 20002
          |FetchPort: 20003
          |ReplicatePort: 2000
          |InternalPort: 20004
          |SlotsUsed: 0
          |LastHeartbeat: 0
          |Disks: empty
          |UserResourceConsumption: empty
          |WorkerRef: null
          |NetworkLocation: /1
          |""".stripMargin
      val exp3 =
        s"""
           |Host: h3
           |RpcPort: 30001
           |PushPort: 30002
           |FetchPort: 30003
           |ReplicatePort: 3000
           |InternalPort: 30004
           |SlotsUsed: 0
           |LastHeartbeat: 0
           |Disks: empty
           |UserResourceConsumption: empty
           |WorkerRef: null
           |NetworkLocation: /default-rack
           |""".stripMargin
      val exp4 =
        s"""
           |Host: h4
           |RpcPort: 40001
           |PushPort: 40002
           |FetchPort: 40003
           |ReplicatePort: 4000
           |InternalPort: 40004
           |SlotsUsed: 60
           |LastHeartbeat: 0
           |Disks: $placeholder
           |  DiskInfo0: DiskInfo(maxSlots: 0, availableSlots: 0, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: disk3, usableSpace: 2048.0 MiB, totalSpace: 2048.0 MiB, avgFlushTime: 3 ns, avgFetchTime: 3 ns, activeSlots: 30, storageType: SSD) status: HEALTHY dirs $placeholder
           |  DiskInfo1: DiskInfo(maxSlots: 0, availableSlots: 0, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: disk1, usableSpace: 2048.0 MiB, totalSpace: 2048.0 MiB, avgFlushTime: 1 ns, avgFetchTime: 1 ns, activeSlots: 10, storageType: SSD) status: HEALTHY dirs $placeholder
           |  DiskInfo2: DiskInfo(maxSlots: 0, availableSlots: 0, committed shuffles 0, running applications 0, shuffleAllocations: Map(), mountPoint: disk2, usableSpace: 2048.0 MiB, totalSpace: 2048.0 MiB, avgFlushTime: 2 ns, avgFetchTime: 2 ns, activeSlots: 20, storageType: SSD) status: HEALTHY dirs $placeholder
           |UserResourceConsumption: $placeholder
           |  UserIdentifier: `tenant1`.`name1`, ResourceConsumption: ResourceConsumption(diskBytesWritten: 20.0 MiB, diskFileCount: 1, hdfsBytesWritten: 50.0 MiB, hdfsFileCount: 1, subResourceConsumptions: (application_1697697127390_2171854 -> ResourceConsumption(diskBytesWritten: 20.0 MiB, diskFileCount: 1, hdfsBytesWritten: 50.0 MiB, hdfsFileCount: 1, subResourceConsumptions: empty)))
           |WorkerRef: null
           |NetworkLocation: /default-rack
           |""".stripMargin

      assertEquals(
        exp1,
        worker1.toString.replaceAll("(HeartbeatElapsedSeconds|WorkerStatus):.*\n", ""))
      assertEquals(
        exp2,
        worker2.toString.replaceAll("(HeartbeatElapsedSeconds|WorkerStatus):.*\n", ""))
      assertEquals(
        exp3,
        worker3.toString.replaceAll("(HeartbeatElapsedSeconds|WorkerStatus):.*\n", ""))
      assertEquals(
        exp4,
        worker4.toString.replaceAll("(HeartbeatElapsedSeconds|WorkerStatus):.*\n", ""))
    } finally {
      if (null != rpcEnv) {
        rpcEnv.shutdown()
      }
    }
  }

  def generateRandomIPv4Address: String = {
    val ipAddress = new StringBuilder
    for (i <- 0 until 4) {
      ipAddress.append(Random.nextInt(256))
      if (i < 3) ipAddress.append(".")
    }
    ipAddress.toString
  }

  test("Test WorkerInfo hashcode") {
    val host = generateRandomIPv4Address
    val rpcPort = Random.nextInt(65536)
    val pushPort = Random.nextInt(65536)
    val fetchPort = Random.nextInt(65536)
    val replicatePort = Random.nextInt(65536)
    val internalPort = Random.nextInt(65536)
    val workerInfo =
      new WorkerInfo(
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        internalPort)

    // origin hashCode() logic
    val state = Seq(
      host,
      rpcPort,
      pushPort,
      fetchPort,
      replicatePort)
    val originHash = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)

    val hashCode1 = workerInfo.hashCode()
    assert(originHash === hashCode1)

    val hashCode2 = workerInfo.hashCode()
    assert(hashCode1 === hashCode2)
  }
}
