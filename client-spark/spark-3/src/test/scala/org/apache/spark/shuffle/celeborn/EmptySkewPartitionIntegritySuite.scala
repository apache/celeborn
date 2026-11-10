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

import java.util
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.mockito.Answers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.{ShuffleClient, ShuffleClientImpl, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.client.commit.ReducePartitionCommitHandler
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.network.client.TransportClientFactory
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetReducerFileGroup, GetReducerFileGroupResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.util.ThreadUtils

class EmptySkewPartitionIntegritySuite extends AnyFunSuite with BeforeAndAfterAll {
  private val shuffleId = 7
  private val partitionId = 1
  private var previousStreamCreatorPool: java.util.concurrent.ThreadPoolExecutor = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    previousStreamCreatorPool = CelebornShuffleReader.streamCreatorPool
  }

  override def afterAll(): Unit = {
    try {
      if (previousStreamCreatorPool == null && CelebornShuffleReader.streamCreatorPool != null) {
        ThreadUtils.shutdown(CelebornShuffleReader.streamCreatorPool)
        CelebornShuffleReader.streamCreatorPool = null
      }
    } finally {
      super.afterAll()
    }
  }

  // The reader, chunk splitter, client RPC serialization and driver validator are production
  // implementations. Only RPC transport and the unused worker connection factory are mocked.
  private class Fixture(
      integrityEnabled: Boolean = true,
      numPartitions: Int = 3,
      nonemptyParent: Boolean = true)
    extends AutoCloseable {
    val conf = new CelebornConf()
      .set("celeborn.client.shuffle.integrityCheck.enabled", integrityEnabled.toString)
      .set("celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled", "true")
    private val rpcPool = ThreadUtils.newDaemonCachedThreadPool("empty-skew-test-rpc")
    private val scheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("empty-skew-test-scheduler")
    val handler = new ReducePartitionCommitHandler(
      "empty-skew-test",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      rpcPool,
      scheduler,
      null)
    val expected = new CommitMetadata()
    private val payload = "nonempty parent with one chunk".getBytes("UTF-8")
    if (nonemptyParent) expected.addDataWithOffsetAndLength(payload, 0, payload.length)
    handler.registerShuffle(shuffleId, 2, isSegmentGranularityVisible = false, numPartitions)
    (0 until 2).foreach { mapId =>
      val metadata = if (mapId == 1) expected else new CommitMetadata()
      val checksums = new Array[Int](numPartitions)
      val bytes = new Array[Long](numPartitions)
      checksums(partitionId) = metadata.getChecksum
      bytes(partitionId) = metadata.getBytes
      assert(handler.finishMapperAttempt(
        shuffleId,
        mapId,
        0,
        2,
        partitionId,
        null,
        _ => (),
        numPartitions,
        checksums,
        bytes) ==
        ((true, mapId == 1)))
    }

    val location = new PartitionLocation(
      partitionId,
      0,
      "localhost",
      19001,
      19002,
      19003,
      19004,
      PartitionLocation.Mode.PRIMARY)
    location.setStorageInfo(new StorageInfo(
      StorageInfo.Type.HDD,
      "mount",
      true,
      "file",
      StorageInfo.LOCAL_DISK_MASK,
      payload.length.toLong,
      util.Arrays.asList(0L, payload.length.toLong)))
    val fileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    fileGroups.put(partitionId, util.Collections.singleton(location))
    val reports = new CopyOnWriteArrayList[PbReadReducerPartitionEnd]()
    val validations = new CopyOnWriteArrayList[(Boolean, String)]()
    val workerFactory = mock(classOf[TransportClientFactory])
    private val endpoint = mock(
      classOf[RpcEndpointRef],
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): AnyRef = {
          if (invocation.getMethod.getName != "askSync") {
            Answers.RETURNS_DEFAULTS.answer(invocation)
          } else {
            invocation.getArgument[AnyRef](0) match {
              case _: GetReducerFileGroup =>
                GetReducerFileGroupResponse(
                  StatusCode.SUCCESS,
                  fileGroups,
                  handler.getMapperAttempts(shuffleId))
              case request: PbReadReducerPartitionEnd =>
                reports.add(request)
                val result = handler.finishPartition(
                  request.getShuffleId,
                  request.getPartitionId,
                  request.getStartMaxIndex,
                  request.getEndMapIndex,
                  new CommitMetadata(request.getCrc32, request.getBytesWritten))
                validations.add(result)
                PbReadReducerPartitionEndResponse.newBuilder()
                  .setStatus(if (result._1) StatusCode.SUCCESS.getValue
                  else StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue)
                  .setErrorMsg(result._2).build()
              case _: PbGetStageEnd =>
                PbGetStageEndResponse.newBuilder().setStageEnd(true).build()
              case _: PbGetShuffleId =>
                PbGetShuffleIdResponse.newBuilder().setShuffleId(shuffleId).setSuccess(true).build()
              case _: PbReportShuffleFetchFailure =>
                PbReportShuffleFetchFailureResponse.newBuilder().setSuccess(true).build()
              case other => throw new AssertionError(s"Unexpected RPC: $other")
            }
          }
        }
      })
    val client = new ShuffleClientImpl("empty-skew-test", conf, UserIdentifier("test", "test")) {
      override def getDataClientFactory(): TransportClientFactory = workerFactory
    }
    client.setupLifecycleManagerRef(endpoint)

    def report(metadata: CommitMetadata, count: Int, index: Int): Unit = {
      client.readReducerPartitionEnd(
        shuffleId,
        partitionId,
        count,
        index,
        metadata.getChecksum,
        metadata.getBytes)
    }

    override def close(): Unit = {
      client.shutdown()
      rpcPool.shutdownNow()
      scheduler.shutdownNow()
    }
  }

  private def withFixture(
      integrityEnabled: Boolean = true,
      numPartitions: Int = 3,
      nonemptyParent: Boolean = true)(
      body: Fixture => Unit): Unit = {
    val fixture = new Fixture(integrityEnabled, numPartitions, nonemptyParent)
    try body(fixture)
    finally fixture.close()
  }

  private def reader(
      fixture: Fixture,
      count: Int = 2,
      index: Int = 0,
      firstPartition: Int = partitionId,
      lastPartition: Int = partitionId + 1,
      stageRerun: Boolean = false): CelebornShuffleReader[Int, Int] = {
    val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
    when(dependency.serializer).thenReturn(new JavaSerializer(new SparkConf(false)))
    when(dependency.keyOrdering).thenReturn(None)
    when(dependency.aggregator).thenReturn(None)
    val handle = new CelebornShuffleHandle[Int, Int, Int](
      "empty-skew-test",
      "localhost",
      0,
      UserIdentifier("test", "test"),
      shuffleId,
      stageRerun,
      2,
      dependency)
    val context = mock(classOf[TaskContext])
    when(context.taskMetrics()).thenReturn(TaskMetrics.empty)
    val factory = mockStatic(classOf[ShuffleClient])
    try {
      factory.when(() => ShuffleClient.get(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(fixture.client)
      new CelebornShuffleReader[Int, Int](
        handle,
        firstPartition,
        lastPartition,
        count,
        index,
        context,
        fixture.conf,
        mock(classOf[ShuffleReadMetricsReporter]),
        new ExecutorShuffleIdTracker())
    } finally {
      factory.close()
    }
  }

  for {
    count <- Seq(2, 4)
    emptyFirst <- Seq(false, true)
  } {
    test(
      s"empty chunk selections complete parent validation: count=$count, emptyFirst=$emptyFirst") {
      withFixture() { fixture =>
        if (!emptyFirst) fixture.report(fixture.expected, count, count - 1)
        (0 until count - 1).foreach { index =>
          assert(CelebornPartitionUtil.splitSkewedPartitionLocations(
            new util.ArrayList(fixture.fileGroups.get(partitionId)),
            count,
            index).isEmpty)
          val iter = reader(fixture, count, index).read()
          assert(!iter.hasNext)
          assert(!iter.hasNext)
        }
        if (emptyFirst) fixture.report(fixture.expected, count, count - 1)
        assert(fixture.reports.size() == count)
        val emptyReports = fixture.reports.asScala.filter(_.getEndMapIndex < count - 1)
        assert(emptyReports.map(_.getEndMapIndex).toSet == (0 until count - 1).toSet)
        assert(emptyReports.forall(r =>
          r.getStartMaxIndex == count &&
            r.getBytesWritten == 0 && r.getCrc32 == 0))
        assert(fixture.validations.asScala.last == ((true, "Partition is complete")))
        verifyNoInteractions(fixture.workerFactory)
      }
    }
  }

  Seq(false, true).foreach { stageRerun =>
    test(s"an empty child's final report exposes a corrupt sibling: stageRerun=$stageRerun") {
      withFixture() { fixture =>
        fixture.report(
          new CommitMetadata(fixture.expected.getChecksum ^ 1, fixture.expected.getBytes),
          2,
          1)
        assert(fixture.validations.asScala.last._1)
        val error = intercept[Exception] {
          reader(fixture, stageRerun = stageRerun).read().toList
        }
        val integrityError =
          if (stageRerun) {
            assert(error.isInstanceOf[FetchFailedException])
            error.getCause
          } else error
        assert(integrityError.isInstanceOf[CelebornIOException])
        assert(integrityError.getMessage.contains("failed validation check"))
        assert(fixture.reports.size() == 2)
        assert(!fixture.validations.asScala.last._1)
        verifyNoInteractions(fixture.workerFactory)
      }
    }
  }

  test("ordinary empty partitions keep the zero-RPC and zero-stream-task fast path") {
    withFixture(numPartitions = 10002) { fixture =>
      val emptyReader =
        reader(fixture, count = 0, index = Int.MaxValue, firstPartition = 2, lastPartition = 10002)
      val tasksBefore = Option(CelebornShuffleReader.streamCreatorPool)
        .map(_.getTaskCount).getOrElse(0L)
      assert(emptyReader.read().toList.isEmpty)
      assert(fixture.reports.isEmpty)
      assert(CelebornShuffleReader.streamCreatorPool.getTaskCount == tasksBefore)
      verifyNoInteractions(fixture.workerFactory)
    }
  }

  test("an empty parent location entry does not gain an integrity RPC") {
    withFixture(nonemptyParent = false) { fixture =>
      fixture.fileGroups.put(partitionId, util.Collections.emptySet[PartitionLocation]())
      assert(reader(fixture).read().toList.isEmpty)
      assert(fixture.reports.isEmpty)
      verifyNoInteractions(fixture.workerFactory)
    }
  }

  test("disabled integrity checking does not report empty AQE children") {
    withFixture(integrityEnabled = false) { fixture =>
      assert(reader(fixture).read().toList.isEmpty)
      assert(fixture.reports.isEmpty)
      verifyNoInteractions(fixture.workerFactory)
    }
  }
}
