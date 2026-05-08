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
import java.util.concurrent.TimeoutException

import org.apache.spark.{Dependency, ShuffleDependency, TaskContext}
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{DummyShuffleClient, ShuffleClient}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier

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
    var duplicateBytesRead = 0L
    var remoteReadRetryCount = 0L
    var partitionReaderWaitTime = 0L
    var readerChunkCount = 0L
    var chunkFetchRequestCount = 0L
    var chunkFetchSuccessCount = 0L
    var chunkFetchFailureCount = 0L
    var distinctRemoteWorkersRead = 0L
    var remoteWorkerStreamsRead = 0L

    def incCelebornOpenStreamTime(v: Long): Unit = openStreamTime += v
    def incCelebornDuplicateBytesRead(v: Long): Unit = duplicateBytesRead += v
    def incCelebornRemoteReadRetryCount(v: Long): Unit = remoteReadRetryCount += v
    def incCelebornPartitionReaderWaitTime(v: Long): Unit = partitionReaderWaitTime += v
    def incCelebornReaderChunkCount(v: Long): Unit = readerChunkCount += v
    def incCelebornChunkFetchRequestCount(v: Long): Unit = chunkFetchRequestCount += v
    def incCelebornChunkFetchSuccessCount(v: Long): Unit = chunkFetchSuccessCount += v
    def incCelebornChunkFetchFailureCount(v: Long): Unit = chunkFetchFailureCount += v
    def incCelebornDistinctRemoteWorkersRead(v: Long): Unit = distinctRemoteWorkersRead += v
    def incCelebornRemoteWorkerStreamsRead(v: Long): Unit = remoteWorkerStreamsRead += v
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
    metricsCallback.incDuplicateBytesRead(12L)
    metricsCallback.incRemoteReadRetryCount(13L)
    metricsCallback.incPartitionReaderWaitTime(14L)
    metricsCallback.incReaderChunkCount(15L)
    metricsCallback.incChunkFetchRequestCount(16L)
    metricsCallback.incChunkFetchSuccessCount(17L)
    metricsCallback.incChunkFetchFailureCount(18L)
    metricsCallback.recordRemoteReadWorker("worker-1:19098")
    metricsCallback.recordRemoteReadWorker("worker-1:19098")
    metricsCallback.recordRemoteReadWorker("worker-2:19098")
    metricsCallback.incRemoteWorkerStreamsRead(3L)

    assert(metrics.openStreamTime === 11L)
    assert(metrics.duplicateBytesRead === 12L)
    assert(metrics.remoteReadRetryCount === 13L)
    assert(metrics.partitionReaderWaitTime === 14L)
    assert(metrics.readerChunkCount === 15L)
    assert(metrics.chunkFetchRequestCount === 16L)
    assert(metrics.chunkFetchSuccessCount === 17L)
    assert(metrics.chunkFetchFailureCount === 18L)
    assert(metrics.distinctRemoteWorkersRead === 2L)
    assert(metrics.remoteWorkerStreamsRead === 3L)
  }

  test("ignore Celeborn shuffle read metrics when Spark does not expose them") {
    val metricReporter = new BaseMetricsReporter
    val metricsCallback = CelebornShuffleReader.createMetricsCallback(metricReporter)

    metricsCallback.incOpenStreamTime(11L)
    metricsCallback.incDuplicateBytesRead(12L)
    metricsCallback.incRemoteReadRetryCount(13L)
    metricsCallback.incPartitionReaderWaitTime(14L)
    metricsCallback.incReaderChunkCount(15L)
    metricsCallback.incChunkFetchRequestCount(16L)
    metricsCallback.incChunkFetchSuccessCount(17L)
    metricsCallback.incChunkFetchFailureCount(18L)
    metricsCallback.recordRemoteReadWorker("worker-1:19098")
    metricsCallback.incRemoteWorkerStreamsRead(1L)
  }

  test("disable Celeborn shuffle read metrics after reflective invocation failure") {
    val metrics = new FailingCelebornMetricsReporter
    val metricsCallback = CelebornShuffleReader.createMetricsCallback(metrics)

    metricsCallback.incOpenStreamTime(11L)
    metricsCallback.incOpenStreamTime(12L)

    assert(metrics.openStreamAttempts === 1)
  }
}
