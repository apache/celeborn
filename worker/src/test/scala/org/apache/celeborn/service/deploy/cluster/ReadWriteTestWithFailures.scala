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
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.MetricsCallback
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.service.deploy.MiniClusterFeature

class ReadWriteTestWithFailures extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val (m, _) = setupMiniClusterWithRandomPorts(workerConf =
      Map("celeborn.shuffle.chunk.size" -> "100B", "celeborn.worker.flusher.buffer.size" -> "10B"))
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  test(s"test MiniCluster with connection resets, ensure no duplicate reads") {
    Assert.assertEquals(performTest("true"), 0)
  }

  test(s"test MiniCluster with connection resets, assert duplicate reads") {
    Assert.assertTrue(performTest("false") > 0)
  }

  def performTest(workerChunkLevelCheckpointEnabled: String): Long = {
    val APP = "app-1"

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
      .set("celeborn.client.fetch.maxReqsInFlight", "1")
      .set("celeborn.client.shuffle.compression.codec", CompressionCodec.NONE.toString)
      .set(CelebornConf.TEST_CLIENT_FETCH_FAILURE.key, "true")
      .set(
        CelebornConf.WORKER_PARTITION_READER_CHECKPOINT_ENABLED.key,
        workerChunkLevelCheckpointEnabled)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")

    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    // push 100 random strings
    val numuuids = 100
    val stringSet = new util.HashSet[String]()
    for (i <- 0 until numuuids) {
      val str = UUID.randomUUID().toString
      stringSet.add(str)
      val data = ("_" + str).getBytes(StandardCharsets.UTF_8)
      shuffleClient.pushData(1, 0, 0, 0, data, 0, data.length, 1, 1)
    }
    shuffleClient.pushMergedData(1, 0, 0)
    Thread.sleep(1000)

    shuffleClient.mapperEnd(1, 0, 0, 1)

    var duplicateBytesRead = new AtomicLong(0)
    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
      override def incDuplicateBytesRead(bytesRead: Long): Unit = {
        duplicateBytesRead.addAndGet(bytesRead)
      }
    }
    val inputStream = shuffleClient.readPartition(
      1,
      1,
      0,
      0,
      0,
      0,
      Integer.MAX_VALUE,
      null,
      null,
      null,
      null,
      null,
      null,
      metricsCallback)

    val outputStream = new ByteArrayOutputStream()
    var b = inputStream.read()
    while (b != -1) {
      outputStream.write(b)
      b = inputStream.read()
    }

    val readStrings =
      new String(outputStream.toByteArray, StandardCharsets.UTF_8).substring(1).split("_")
    Assert.assertEquals(readStrings.length, numuuids)
    readStrings.foreach { str =>
      Assert.assertTrue(stringSet.contains(str))
    }

    Thread.sleep(5000L)
    shuffleClient.shutdown()
    lifecycleManager.rpcEnv.shutdown()

    duplicateBytesRead.get()
  }
}
