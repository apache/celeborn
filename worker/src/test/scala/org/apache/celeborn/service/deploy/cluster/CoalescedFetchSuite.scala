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

import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl}
import org.apache.celeborn.client.read.{CoalescedPartitionInfo, MetricsCallback, SharedCoalescedStream}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{CompressionCodec, MessageType, PbCoalescedStreamEntry, PbCoalescedStreamHandler, PbOpenCoalescedStream}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.MiniClusterFeature

class CoalescedFetchSuite extends AnyFunSuite
  with Logging with MiniClusterFeature with BeforeAndAfterAll {

  var masterPort = 0

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val workerConf = Map(CelebornConf.CLIENT_COALESCED_REMOTE_READ_ENABLED.key -> "true")
    val (master, _) = setupMiniClusterWithRandomPorts(workerConf = workerConf, workerNum = 1)
    masterPort = master.conf.masterPort
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop Celeborn mini cluster")
    shutdownMiniCluster()
  }

  test("coalesced fetch preserves reducer boundaries end to end") {
    val appId = "coalesced-fetch-e2e"
    val clientConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.SHUFFLE_COMPRESSION_CODEC.key, CompressionCodec.NONE.name)
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED, false)
      .set(CelebornConf.CLIENT_COALESCED_REMOTE_READ_ENABLED.key, "true")
      .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")
      .set(CelebornConf.CLIENT_FETCH_TIMEOUT.key, "5s")
      .set("celeborn.data.io.numConnectionsPerPeer", "1")

    val lifecycleManager = new LifecycleManager(appId, clientConf)
    val shuffleClient = new ShuffleClientImpl(appId, clientConf, UserIdentifier("mock", "mock"))
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    try {
      val partition0Batch0 = ("000000" + "a" * 200).getBytes(StandardCharsets.UTF_8)
      val partition0Batch1 = ("111111" + "b" * 200).getBytes(StandardCharsets.UTF_8)
      val partition1Batch0 = ("222222" + "c" * 200).getBytes(StandardCharsets.UTF_8)
      val partition1Batch1 = ("333333" + "d" * 200).getBytes(StandardCharsets.UTF_8)

      shuffleClient.pushData(1, 0, 0, 0, partition0Batch0, 0, partition0Batch0.length, 1, 2)
      shuffleClient.pushData(1, 0, 0, 0, partition0Batch1, 0, partition0Batch1.length, 1, 2)
      shuffleClient.pushData(1, 0, 0, 1, partition1Batch0, 0, partition1Batch0.length, 1, 2)
      shuffleClient.pushData(1, 0, 0, 1, partition1Batch1, 0, partition1Batch1.length, 1, 2)
      shuffleClient.pushMergedData(1, 0, 0)
      Thread.sleep(1000)
      shuffleClient.mapperEnd(1, 0, 0, 1)

      val fileGroups = shuffleClient.updateFileGroup(1, 0)
      val location0 = fileGroups.partitionGroups.get(0).iterator().next()
      val location1 = fileGroups.partitionGroups.get(1).iterator().next()
      val shuffleKey = Utils.makeShuffleKey(appId, 1)
      val dataClient = shuffleClient.getDataClientFactory.createClient(
        location0.getHost,
        location0.getFetchPort)
      val request = PbOpenCoalescedStream.newBuilder()
        .setShuffleKey(shuffleKey)
        .setMaxChunkBytes(128)
        .addEntry(PbCoalescedStreamEntry.newBuilder()
          .setFileName(location0.getFileName)
          .setReducerId(0)
          .setStartIndex(0)
          .setEndIndex(Integer.MAX_VALUE)
          .setReadLocalShuffle(false))
        .addEntry(PbCoalescedStreamEntry.newBuilder()
          .setFileName(location1.getFileName)
          .setReducerId(1)
          .setStartIndex(0)
          .setEndIndex(Integer.MAX_VALUE)
          .setReadLocalShuffle(false))
        .build()
      val response = dataClient.sendRpcSync(
        new TransportMessage(MessageType.OPEN_COALESCED_STREAM, request.toByteArray).toByteBuffer,
        clientConf.clientFetchTimeoutMs)
      val handler =
        TransportMessage.fromByteBuffer(response).getParsedPayload[PbCoalescedStreamHandler]
      val sharedStream =
        new SharedCoalescedStream(
          clientConf,
          shuffleKey,
          location0,
          handler,
          shuffleClient.getDataClientFactory)

      try {
        val metricsCallback = new MetricsCallback {
          override def incBytesRead(bytesWritten: Long): Unit = {}
          override def incReadTime(time: Long): Unit = {}
        }
        val expected = Seq(
          partition0Batch0 ++ partition0Batch1,
          partition1Batch0 ++ partition1Batch1)
        Seq(location0, location1).zipWithIndex.foreach { case (location, partitionId) =>
          val coalescedInfos = new util.HashMap[String, CoalescedPartitionInfo]()
          coalescedInfos.put(
            location.getUniqueId,
            new CoalescedPartitionInfo(sharedStream, handler.getBoundaries(partitionId)))
          val locations =
            new util.ArrayList[org.apache.celeborn.common.protocol.PartitionLocation]()
          locations.add(location)
          val inputStream = shuffleClient.readPartition(
            1,
            1,
            partitionId,
            0,
            0,
            0,
            Integer.MAX_VALUE,
            null,
            locations,
            null,
            fileGroups.pushFailedBatches,
            null,
            coalescedInfos,
            fileGroups.mapAttempts,
            metricsCallback,
            true)
          try {
            val output = new ByteArrayOutputStream()
            var b = inputStream.read()
            while (b != -1) {
              output.write(b)
              b = inputStream.read()
            }
            Assert.assertArrayEquals(expected(partitionId), output.toByteArray)
          } finally {
            inputStream.close()
          }
        }

        val replayResponse = dataClient.sendRpcSync(
          new TransportMessage(MessageType.OPEN_COALESCED_STREAM, request.toByteArray).toByteBuffer,
          clientConf.clientFetchTimeoutMs)
        val replayHandler =
          TransportMessage.fromByteBuffer(replayResponse).getParsedPayload[PbCoalescedStreamHandler]
        val replayStream =
          new SharedCoalescedStream(
            clientConf,
            shuffleKey,
            location0,
            replayHandler,
            shuffleClient.getDataClientFactory)
        try {
          val sameReducerInfos = new util.HashMap[String, CoalescedPartitionInfo]()
          sameReducerInfos.put(
            location0.getUniqueId,
            new CoalescedPartitionInfo(replayStream, replayHandler.getBoundaries(0)))
          sameReducerInfos.put(
            location1.getUniqueId,
            new CoalescedPartitionInfo(replayStream, replayHandler.getBoundaries(1)))
          val sameReducerLocations =
            new util.ArrayList[org.apache.celeborn.common.protocol.PartitionLocation]()
          sameReducerLocations.add(location0)
          sameReducerLocations.add(location1)
          val sameReducerInputStream = shuffleClient.readPartition(
            1,
            1,
            0,
            0,
            0,
            0,
            Integer.MAX_VALUE,
            null,
            sameReducerLocations,
            null,
            fileGroups.pushFailedBatches,
            null,
            sameReducerInfos,
            fileGroups.mapAttempts,
            metricsCallback,
            true)
          try {
            val output = new ByteArrayOutputStream()
            var b = sameReducerInputStream.read()
            while (b != -1) {
              output.write(b)
              b = sameReducerInputStream.read()
            }
            Assert.assertArrayEquals(
              partition0Batch0 ++ partition0Batch1 ++ partition1Batch0 ++ partition1Batch1,
              output.toByteArray)
          } finally {
            sameReducerInputStream.close()
          }
        } finally {
          replayStream.close()
        }
      } finally {
        sharedStream.close()
      }
    } finally {
      shuffleClient.shutdown()
      lifecycleManager.rpcEnv.shutdown()
    }
  }
}
