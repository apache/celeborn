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

package org.apache.spark.shuffle.celeborn

import java.nio.file.Files
import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.concurrent.TimeoutException

import org.apache.spark.{Dependency, ShuffleDependency, TaskContext}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{DummyShuffleClient, ShuffleClient}
import org.apache.celeborn.client.read.{CoalescedPartitionInfo, SharedCoalescedStream}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.TransportClient
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol.{MessageType, PartitionLocation, PbCoalescedChunkBoundary, StreamType}

class CelebornShuffleReaderSuite extends AnyFunSuite {

  private class BaseMetricsReporter extends ShuffleReadMetricsReporter {
    override def incRemoteBlocksFetched(v: Long): Unit = {}
    override def incLocalBlocksFetched(v: Long): Unit = {}
    override def incRemoteBytesRead(v: Long): Unit = {}
    override def incRemoteBytesReadToDisk(v: Long): Unit = {}
    override def incLocalBytesRead(v: Long): Unit = {}
    override def incFetchWaitTime(v: Long): Unit = {}
    override def incRecordsRead(v: Long): Unit = {}
    override def incCorruptMergedBlockChunks(v: Long): Unit = {}
    override def incMergedFetchFallbackCount(v: Long): Unit = {}
    override def incRemoteMergedBlocksFetched(v: Long): Unit = {}
    override def incLocalMergedBlocksFetched(v: Long): Unit = {}
    override def incRemoteMergedChunksFetched(v: Long): Unit = {}
    override def incLocalMergedChunksFetched(v: Long): Unit = {}
    override def incRemoteMergedBytesRead(v: Long): Unit = {}
    override def incLocalMergedBytesRead(v: Long): Unit = {}
    override def incRemoteReqsDuration(v: Long): Unit = {}
    override def incRemoteMergedReqsDuration(v: Long): Unit = {}
  }

  private class CelebornMetricsReporter extends BaseMetricsReporter {
    var openStreamTime = 0L
    var partitionReaderWaitTime = 0L
    var readerChunkCount = 0L
    var chunkFetchRequestCount = 0L
    var chunkFetchSuccessCount = 0L
    var chunkFetchFailureCount = 0L

    def incCelebornOpenStreamTime(v: Long): Unit = openStreamTime += v
    def incCelebornPartitionReaderWaitTime(v: Long): Unit = partitionReaderWaitTime += v
    def incCelebornReaderChunkCount(v: Long): Unit = readerChunkCount += v
    def incCelebornChunkFetchRequestCount(v: Long): Unit = chunkFetchRequestCount += v
    def incCelebornChunkFetchSuccessCount(v: Long): Unit = chunkFetchSuccessCount += v
    def incCelebornChunkFetchFailureCount(v: Long): Unit = chunkFetchFailureCount += v
  }

  private class FailingCelebornMetricsReporter extends BaseMetricsReporter {
    var openStreamAttempts = 0

    def incCelebornOpenStreamTime(v: Long): Unit = {
      openStreamAttempts += 1
      throw new IllegalStateException("metric sink unavailable")
    }
  }

  /**
   * Due to spark limitations, spark local mode can not test speculation tasks ,
   * test the method `checkAndReportFetchFailureForUpdateFileGroupFailure`
   */
  test("CELEBORN-1838 test check report fetch failure exceptions ") {
    val dependency = Mockito.mock(classOf[ShuffleDependency[Int, Int, Int]])
    val handler = new CelebornShuffleHandle[Int, Int, Int](
      "APP",
      "HOST1",
      1,
      UserIdentifier.apply("a", "b"),
      0,
      true,
      1,
      dependency)
    val context = Mockito.mock(classOf[TaskContext])
    val metricReporter = Mockito.mock(classOf[ShuffleReadMetricsReporter])
    val conf = new CelebornConf()

    val tmpFile = Files.createTempFile("test", ".tmp").toFile
    mockStatic(classOf[ShuffleClient]).when(() =>
      ShuffleClient.get(any(), any(), any(), any(), any(), any())).thenReturn(
      new DummyShuffleClient(conf, tmpFile))

    val shuffleReader =
      new CelebornShuffleReader[Int, Int](handler, 0, 0, 0, 0, context, conf, metricReporter, null)

    val exception1: Throwable = new CelebornIOException("test1", new InterruptedException("test1"))
    val exception2: Throwable = new CelebornIOException("test2", new TimeoutException("test2"))
    val exception3: Throwable = new CelebornIOException("test3")
    val exception4: Throwable = new CelebornIOException("test4")

    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception1)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception2)
    } catch {
      case _: Throwable =>
    }
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception3)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 1)
    try {
      shuffleReader.checkAndReportFetchFailureForUpdateFileGroupFailure(0, exception4)
    } catch {
      case _: Throwable =>
    }
    assert(
      shuffleReader.shuffleClient.asInstanceOf[DummyShuffleClient].fetchFailureCount.get() === 2)

  }

  test("forward Celeborn shuffle read metrics when Spark exposes them") {
    val metrics = new CelebornMetricsReporter
    val metricsCallback = CelebornShuffleReader.createMetricsCallback(metrics)

    metricsCallback.incOpenStreamTime(11L)
    metricsCallback.incPartitionReaderWaitTime(12L)
    metricsCallback.incReaderChunkCount(13L)
    metricsCallback.incChunkFetchRequestCount(14L)
    metricsCallback.incChunkFetchSuccessCount(15L)
    metricsCallback.incChunkFetchFailureCount(16L)

    assert(metrics.openStreamTime === 11L)
    assert(metrics.partitionReaderWaitTime === 12L)
    assert(metrics.readerChunkCount === 13L)
    assert(metrics.chunkFetchRequestCount === 14L)
    assert(metrics.chunkFetchSuccessCount === 15L)
    assert(metrics.chunkFetchFailureCount === 16L)
  }

  test("ignore Celeborn shuffle read metrics when Spark does not expose them") {
    val metricReporter = new BaseMetricsReporter
    val metricsCallback = CelebornShuffleReader.createMetricsCallback(metricReporter)

    metricsCallback.incOpenStreamTime(11L)
    metricsCallback.incPartitionReaderWaitTime(12L)
    metricsCallback.incReaderChunkCount(13L)
    metricsCallback.incChunkFetchRequestCount(14L)
    metricsCallback.incChunkFetchSuccessCount(15L)
    metricsCallback.incChunkFetchFailureCount(16L)
  }

  test("disable Celeborn shuffle read metrics after reflective invocation failure") {
    val metrics = new FailingCelebornMetricsReporter
    val metricsCallback = CelebornShuffleReader.createMetricsCallback(metrics)

    metricsCallback.incOpenStreamTime(11L)
    metricsCallback.incOpenStreamTime(12L)

    assert(metrics.openStreamAttempts === 1)
  }

  test("preserve multiple coalesced locations for one reducer") {
    val infos = new JHashMap[Int, JMap[String, CoalescedPartitionInfo]]()
    val stream = Mockito.mock(classOf[SharedCoalescedStream])
    val location0 = Mockito.mock(classOf[PartitionLocation])
    val location1 = Mockito.mock(classOf[PartitionLocation])
    when(location0.getUniqueId).thenReturn("7-0")
    when(location1.getUniqueId).thenReturn("7-1")
    val boundary0 = PbCoalescedChunkBoundary.newBuilder().setReducerId(7).build()
    val boundary1 = PbCoalescedChunkBoundary.newBuilder().setReducerId(7).build()

    CelebornShuffleReader.addCoalescedPartitionInfo(infos, location0, stream, boundary0)
    CelebornShuffleReader.addCoalescedPartitionInfo(infos, location1, stream, boundary1)

    assert(infos.size() === 1)
    assert(infos.get(7).size() === 2)
    assert(infos.get(7).get("7-0").boundary eq boundary0)
    assert(infos.get(7).get("7-1").boundary eq boundary1)
  }

  test("close coalesced chunk stream") {
    val client = Mockito.mock(classOf[TransportClient])

    CelebornShuffleReader.closeChunkStream(client, 17L)

    val captor = org.mockito.ArgumentCaptor.forClass(classOf[java.nio.ByteBuffer])
    verify(client).sendRpc(captor.capture())
    val message = TransportMessage.fromByteBuffer(captor.getValue)
    assert(message.getMessageTypeValue === MessageType.BUFFER_STREAM_END.getNumber)
    val payload = message.getParsedPayload[org.apache.celeborn.common.protocol.PbBufferStreamEnd]
    assert(payload.getStreamId === 17L)
    assert(payload.getStreamType === StreamType.ChunkStream)
  }
}
