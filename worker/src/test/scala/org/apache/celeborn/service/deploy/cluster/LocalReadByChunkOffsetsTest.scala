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
import java.util.{Collections, HashMap => JHashMap}

import scala.collection.mutable

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.lang3.tuple.Pair
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

class LocalReadByChunkOffsetsTest extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {
  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val workerConfig = Map(
      CelebornConf.SHUFFLE_CHUNK_SIZE.key -> "8k",
      CelebornConf.WORKER_FLUSHER_BUFFER_SIZE.key -> "8k")
    val (m, _) = setupMiniClusterWithRandomPorts(workerConf = workerConfig)
    masterPort = m.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("CELEBORN-1857: test LocalPartitionReader read partition by chunkOffsets when enable optimize skew partition read") {
    val APP = "CELEBORN-1857"

    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED, false)
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "1k")
      .set(CelebornConf.CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ_ENABLED, true)
      .set(CelebornConf.READ_LOCAL_SHUFFLE_FILE, true)
      .set("celeborn.data.io.numConnectionsPerPeer", "1")
    val lifecycleManager = new LifecycleManager(APP, clientConf)
    val shuffleClient = new ShuffleClientImpl(APP, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    val dataPrefix = Array("000000", "111111", "222222", "333333", "444444", "555555")
    val dataPrefixMap = new mutable.HashMap[String, String]
    val STR1 = dataPrefix(0) + RandomStringUtils.random(4 * 1024)
    dataPrefixMap.put(dataPrefix(0), STR1)
    val DATA1 = STR1.getBytes(StandardCharsets.UTF_8)
    val OFFSET1 = 0
    val LENGTH1 = DATA1.length
    shuffleClient.pushData(1, 0, 0, 0, DATA1, OFFSET1, LENGTH1, 1, 1)

    val STR2 = dataPrefix(1) + RandomStringUtils.random(3 * 1024)
    dataPrefixMap.put(dataPrefix(1), STR2)
    val DATA2 = STR2.getBytes(StandardCharsets.UTF_8)
    val OFFSET2 = 0
    val LENGTH2 = DATA2.length
    shuffleClient.pushData(1, 0, 0, 0, DATA2, OFFSET2, LENGTH2, 1, 1)
    Thread.sleep(1000)

    val STR3 = dataPrefix(2) + RandomStringUtils.random(4 * 1024)
    dataPrefixMap.put(dataPrefix(2), STR3)
    val DATA3 = STR3.getBytes(StandardCharsets.UTF_8)
    val LENGTH3 = DATA3.length
    shuffleClient.pushData(1, 0, 0, 0, DATA3, 0, LENGTH3, 1, 1)
    Thread.sleep(1000)

    val STR4 = dataPrefix(3) + RandomStringUtils.random(2 * 1024)
    dataPrefixMap.put(dataPrefix(3), STR4)
    val DATA4 = STR4.getBytes(StandardCharsets.UTF_8)
    val LENGTH4 = DATA4.length
    shuffleClient.pushData(1, 0, 0, 0, DATA4, 0, LENGTH4, 1, 1)
    Thread.sleep(1000)

    val STR5 = dataPrefix(4) + RandomStringUtils.random(2 * 1024)
    dataPrefixMap.put(dataPrefix(4), STR5)
    val DATA5 = STR5.getBytes(StandardCharsets.UTF_8)
    val LENGTH5 = DATA5.length
    shuffleClient.pushData(1, 0, 0, 0, DATA5, 0, LENGTH5, 1, 1)
    Thread.sleep(1000)

    val STR6 = dataPrefix(5) + RandomStringUtils.random(6 * 1024)
    dataPrefixMap.put(dataPrefix(5), STR6)
    val DATA6 = STR6.getBytes(StandardCharsets.UTF_8)
    val LENGTH6 = DATA6.length
    shuffleClient.pushData(1, 0, 0, 0, DATA6, 0, LENGTH6, 1, 1)
    shuffleClient.pushMergedData(1, 0, 0)
    Thread.sleep(1000)

    shuffleClient.mapperEnd(1, 0, 0, 1, 0)

    val metricsCallback = new MetricsCallback {
      override def incBytesRead(bytesWritten: Long): Unit = {}
      override def incReadTime(time: Long): Unit = {}
    }

    // chunkOffset is [0, 9404, 25913, 35393, 49576]
    // chunk0 -> DATA1, chunk1 -> DATA2+DATA3, chunk2 -> DATA4+DATA5, chunk3 -> DATA6
    val subMap = new JHashMap[String, Pair[Integer, Integer]]()
    // pair of (1, 2) means read chunk1 and chunk 2
    // why not test pair of (0, 1), because we want to test fileChannel.position of chunk index not 0
    subMap.put("0-0", Pair.of(1, 2))

    val inputStream = shuffleClient.readPartition(
      1,
      1,
      0,
      0,
      0,
      3, // startMapId > endMapId, means sub-partition size
      1, // sub-partition index
      null,
      null,
      null,
      Collections.emptyMap(), // failed batch could not be null
      subMap, // sub-partition chunk range
      null,
      metricsCallback,
      true)
    val outputStream = new ByteArrayOutputStream()

    var b = inputStream.read()
    while (b != -1) {
      outputStream.write(b)
      b = inputStream.read()
    }

    val readBytes = outputStream.toByteArray
    val dataPrefix1 = Array("111111", "222222", "333333", "444444")
    val readStringMap = getReadStringMap(readBytes, dataPrefix1, dataPrefixMap)

    Assert.assertEquals(LENGTH2 + LENGTH3 + LENGTH4 + LENGTH5, readBytes.length)
    for ((prefix, data) <- readStringMap) {
      Assert.assertEquals(dataPrefixMap(prefix), data)
    }

    Thread.sleep(5000L)
    shuffleClient.shutdown()
    lifecycleManager.rpcEnv.shutdown()
  }

  def getReadStringMap(
      readBytes: Array[Byte],
      dataPrefix: Array[String],
      dataPrefixMap: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    val readString = new String(readBytes, StandardCharsets.UTF_8)
    val prefixStringMap = new mutable.HashMap[String, String]

    var remainingString = readString
    while (remainingString.nonEmpty) {
      dataPrefix.find(prefix => remainingString.startsWith(prefix)) match {
        case Some(prefix) =>
          val expectedLength = dataPrefixMap.get(prefix).get.length
          val subString = remainingString.substring(0, expectedLength)
          prefixStringMap.put(prefix, subString)
          remainingString = remainingString.substring(expectedLength)
        case None =>
          remainingString = ""
      }
    }

    prefixStringMap
  }

}
