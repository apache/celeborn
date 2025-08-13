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

package org.apache.celeborn.service.deploy.cluster

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util
import java.util.Comparator

import scala.collection.JavaConverters._

import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.{color, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.{CompressionCodec, PartitionLocation}
import org.apache.celeborn.service.deploy.MiniClusterFeature

class PartitionSplitSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {
  val masterPort = 19097
  var masterEndpoint = s"localhost:$masterPort"
  val SHUFFLE_ID = 0
  val MAP_ID = 0
  val ATTEMPT_ID = 0
  val MAP_NUM = 17
  val PARTITION_NUM = 11
  val SHUFFLE_CLIENT_NUM = 2
  var throwable: Throwable = null

  override def beforeAll(): Unit = {
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.worker.flusher.buffer.size" -> "0",
      "celeborn.master.port" -> masterPort.toString)
    val workerConf = Map(
      "celeborn.worker.flusher.buffer.size" -> "0",
      "celeborn.master.endpoints" -> s"localhost:$masterPort")
    logInfo("test initialized , setup Celeborn mini cluster")
    setUpMiniCluster(masterConf, workerConf, 7)
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    super.shutdownMiniCluster()
  }

  private def registerShuffleAndCheck(
      shuffleClient: ShuffleClientImpl,
      lifecycleManager: LifecycleManager,
      maxWriteParallelism: Int): Unit = {
    val splitEnd = (if (maxWriteParallelism != -1) maxWriteParallelism else MAP_NUM) - 1
    val partitionLocationMap =
      shuffleClient.getPartitionLocation(SHUFFLE_ID, MAP_NUM, PARTITION_NUM)
    var locStr = ""
    var latestLocStr = ""
    var expectLocStr = ""
    for (partitionId <- 0 until PARTITION_NUM) {
      val partitionLocation = partitionLocationMap.get(partitionId)
      val latestLocations = lifecycleManager.latestPartitionLocation.get(SHUFFLE_ID).get(
        partitionId).getLeafLocations(null)
      val latestLocation = latestLocations.get(0)
      assertEquals(latestLocations.size(), 1)
      locStr =
        s"$locStr ${partitionLocation.getFileName}:${partitionLocation.getSplitStart}~${partitionLocation.getSplitEnd}"
      latestLocStr =
        s"$latestLocStr ${latestLocation.getFileName}:${latestLocation.getSplitStart}~${latestLocation.getSplitEnd}"
      expectLocStr = s"$expectLocStr $partitionId-0-0:0~$splitEnd"
    }
    assertEquals(locStr, expectLocStr)
    assertEquals(latestLocStr, expectLocStr)
    logInfo(s"$locStr")
  }

  private def wrapWithExceptionExport[T](code: => T): Thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        code
      } catch {
        case t: Throwable => throwable = t
      }
    }
  })

  private def checkThrowable(): Unit = {
    if (throwable != null) {
      throw throwable
    }
  }

  private def triggerSplit(
      shuffleClient: ShuffleClientImpl,
      dataList: util.ArrayList[Array[Byte]]): Unit = {
    for (mapId <- 0 until MAP_NUM) {
      for (i <- 0 until PARTITION_NUM) {
        for (j <- 0 until dataList.size()) {
          val data = dataList.get(j)
          shuffleClient.pushData(
            SHUFFLE_ID,
            mapId,
            ATTEMPT_ID,
            i,
            data,
            0,
            data.length,
            MAP_NUM,
            PARTITION_NUM)
        }
      }
    }
  }

  private def generateData(): (util.ArrayList[Array[Byte]], Array[Byte]) = {
    val dataList = new util.ArrayList[Array[Byte]]()
    dataList.add(RandomStringUtils.random(1024 * 100).getBytes(StandardCharsets.UTF_8))
    dataList.add(RandomStringUtils.random(1024).getBytes(StandardCharsets.UTF_8))
    var dataLen = 0
    for (i <- 0 until dataList.size()) {
      dataLen = dataLen + dataList.get(i).length
    }
    val mergeData = new Array[Byte](dataLen * MAP_NUM)
    var currCopiedLen = 0
    for (i <- 0 until MAP_NUM) {
      for (j <- 0 until dataList.size()) {
        System.arraycopy(dataList.get(j), 0, mergeData, currCopiedLen, dataList.get(j).length)
        currCopiedLen = currCopiedLen + dataList.get(j).length
      }
    }
    (dataList, mergeData)
  }

  private def readData(shuffleClient: ShuffleClientImpl, expectData: Array[Byte]): Unit = {
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }
    for (partitionId <- 0 until PARTITION_NUM) {
      // test if there is SHUFFLE_DATA_LOST
      val inputStream = shuffleClient.readPartition(
        SHUFFLE_ID,
        partitionId,
        0,
        0,
        0,
        Integer.MAX_VALUE,
        metricsCallback)
      val outputStream = new ByteArrayOutputStream()
      var b = inputStream.read()
      while (b != -1) {
        outputStream.write(b)
        b = inputStream.read()
      }
      val readBytes = outputStream.toByteArray
      assertEquals(readBytes.length, expectData.length)
      inputStream.close()
      outputStream.close()
    }
    logInfo(s"$shuffleClient complete read")

    shuffleClient.shutdown()
  }

  private def getInitLocs(lifecycleManager: LifecycleManager)
      : util.HashMap[Int, PartitionLocation] = {
    val initLocs = new util.HashMap[Int, PartitionLocation]()
    lifecycleManager.workerSnapshots(SHUFFLE_ID)
      .values()
      .asScala
      .flatMap(_.getAllPrimaryLocationsWithMaxEpoch())
      .filter(_.getEpoch == 0)
      .foreach(loc => {
        assertFalse(initLocs.containsKey(loc.getId))
        initLocs.put(loc.getId, loc)
      })
    assertEquals(initLocs.size(), PARTITION_NUM)
    initLocs
  }

  private def toIntFunction[T](f: T => Int): java.util.function.ToIntFunction[T] =
    new java.util.function.ToIntFunction[T] {
      override def applyAsInt(value: T): Int = f(value)
    }

  private def traverseLocations(
      initLocs: util.HashMap[Int, PartitionLocation],
      lifecycleManager: LifecycleManager,
      maxWriteParallelism: Int): Unit = {
    for (i <- 0 until PARTITION_NUM) {
      val queue = new util.LinkedList[PartitionLocation]()
      queue.add(initLocs.get(i))
      val leafLocations = new util.ArrayList[PartitionLocation]()
      while (!queue.isEmpty) {
        val curr = queue.removeFirst()
        val currChildren =
          lifecycleManager.latestPartitionLocation.get(SHUFFLE_ID).get(i).getChildren(curr)
        if (currChildren == null || currChildren.isEmpty) {
          leafLocations.add(curr)
        } else {
          val children = new util.ArrayList[PartitionLocation](currChildren)
          children.sort(Comparator.comparingInt(toIntFunction(_.getSplitStart)))
          var lastEndpoint = children.get(0).getSplitEnd
          queue.addLast(children.get(0))
          for (j <- 1 until children.size()) {
            val location = children.get(j)
            assertEquals(location.getSplitStart, lastEndpoint + 1)
            assertTrue(location.getSplitEnd >= location.getSplitStart)
            lastEndpoint = location.getSplitEnd
            queue.addLast(location)
          }
        }
      }
      leafLocations.sort(Comparator.comparingInt(toIntFunction(_.getSplitStart)))
      var lastEndpoint = -1
      val locStr = leafLocations.asScala.map(loc =>
        loc.getFileName + "_" + loc.getSplitStart + "_" + loc.getSplitEnd).mkString(",")
      logInfo(s"leaf partitionId=$i $locStr")
      for (j <- 0 until leafLocations.size()) {
        val location = leafLocations.get(j)
        assertEquals(location.getSplitStart, lastEndpoint + 1)
        assertTrue(location.getSplitEnd >= location.getSplitStart)
        lastEndpoint = location.getSplitEnd
      }
      assertEquals(lastEndpoint, maxWriteParallelism - 1)
    }
  }

  test("test partition split") {
    val (dataList, mergeData) = generateData()
    throwable = null
    val params = Array(
      (true, true, -1, 2),
      (true, true, 1, 2),
      (true, true, 3, 2),
      (true, true, -1, 1),
      (true, true, 1, 1),
      (true, true, 3, 1),
      (true, true, -1, 3),
      (true, true, 1, 3),
      (true, true, 3, 3),
      (true, true, -1, MAP_NUM),
      (true, true, 1, MAP_NUM),
      (true, true, 3, MAP_NUM),
      (true, false, -1, 2),
      (true, false, 1, 2),
      (true, false, 3, 2),
      (true, false, -1, 1),
      (true, false, 1, 1),
      (true, false, 3, 1),
      (true, false, -1, 3),
      (true, false, 1, 3),
      (true, false, 3, 3),
      (true, false, -1, MAP_NUM),
      (true, false, 1, MAP_NUM),
      (true, false, 3, MAP_NUM),
      (false, true, -1, 2),
      (false, true, 1, 2),
      (false, true, 3, 2),
      (false, true, -1, 1),
      (false, true, 1, 1),
      (false, true, 3, 1),
      (false, true, -1, 3),
      (false, true, 1, 3),
      (false, true, 3, 3),
      (false, true, -1, MAP_NUM),
      (false, true, 1, MAP_NUM),
      (false, true, 3, MAP_NUM),
      (false, false, -1, 2),
      (false, false, 1, 2),
      (false, false, 3, 2),
      (false, false, -1, 1),
      (false, false, 1, 1),
      (false, false, 3, 1),
      (false, false, -1, 3),
      (false, false, 1, 3),
      (false, false, 3, 3),
      (false, false, -1, MAP_NUM),
      (false, false, 1, MAP_NUM),
      (false, false, 3, MAP_NUM))
    for (pi <- params.indices) {
      val (
        mockReserveFailure: Boolean,
        enableReplicate: Boolean,
        maxWriteParallelism: Int,
        splitNum: Int) = params(pi)
      logInfo(s"start test mockReserveFailure=$mockReserveFailure, enableReplicate=$enableReplicate, maxWriteParallelism=$maxWriteParallelism, splitNum=$splitNum")
      var maxSplitEnd = maxWriteParallelism
      if (maxWriteParallelism == -1) {
        maxSplitEnd = MAP_NUM
      }
      val APP = s"app-${System.currentTimeMillis()}"
      val clientConf = new CelebornConf()
        .set(CelebornConf.MASTER_ENDPOINTS.key, masterEndpoint)
        .set(CelebornConf.CLIENT_PUSH_MAX_REVIVE_TIMES.key, "1")
        .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key, "5K")
        .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
        .set(CelebornConf.SHUFFLE_PARTITION_SPLIT_MODE.key, "HARD")
        .set(CelebornConf.CLEINT_PATITION_MAX_WRITE_PARALLELISM.key, maxWriteParallelism.toString)
        .set(CelebornConf.CLEINT_PATITION_SPLIT_NUM.key, splitNum.toString)
        .set(CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key, mockReserveFailure.toString)
        .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, enableReplicate.toString)
      val lifecycleManager = new LifecycleManager(APP, clientConf)
      val shuffleClients = new util.ArrayList[ShuffleClientImpl]()
      for (i <- 0 until SHUFFLE_CLIENT_NUM) {
        val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
        shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)
        shuffleClients.add(shuffleClient)
        val registerCheckThread = wrapWithExceptionExport(registerShuffleAndCheck(
          shuffleClients.get(i),
          lifecycleManager,
          maxWriteParallelism))
        registerCheckThread.start()
        registerCheckThread.join(10000)
      }
      checkThrowable()
      val initLocs = getInitLocs(lifecycleManager)
      traverseLocations(initLocs, lifecycleManager, maxSplitEnd)
      lifecycleManager.setReserveFailureMocked(false)
      for (i <- 0 until SHUFFLE_CLIENT_NUM) {
        val shuffleClient = shuffleClients.get(i)
        val triggerSplitThread = wrapWithExceptionExport(triggerSplit(shuffleClient, dataList))
        triggerSplitThread.start()
        triggerSplitThread.join(10000)
      }
      checkThrowable()
      for (i <- 0 until SHUFFLE_CLIENT_NUM) {
        for (j <- 0 until MAP_NUM) {
          val mapperEndThread = wrapWithExceptionExport(shuffleClients.get(i).mapperEnd(
            SHUFFLE_ID,
            j,
            ATTEMPT_ID,
            MAP_NUM,
            PARTITION_NUM))
          mapperEndThread.start()
          mapperEndThread.join(10000)
        }
      }
      checkThrowable()
      traverseLocations(initLocs, lifecycleManager, maxSplitEnd)
      for (j <- 0 until SHUFFLE_CLIENT_NUM) {
        val readDataThread = wrapWithExceptionExport(readData(shuffleClients.get(j), mergeData))
        readDataThread.start()
        readDataThread.join(10000)
      }
      checkThrowable()
      traverseLocations(initLocs, lifecycleManager, maxSplitEnd)
      logInfo(s"finish test mockReserveFailure=$mockReserveFailure, enableReplicate=$enableReplicate, maxWriteParallelism=$maxWriteParallelism, splitNum=$splitNum")
      lifecycleManager.stop()
    }
  }
}
