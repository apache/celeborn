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

package org.apache.celeborn.client.commit

import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.mockito.Answers
import org.mockito.Mockito.mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.{ShuffleClientImpl, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.client.read.CelebornInputStream
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.exception.CelebornIOException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages.{GetReducerFileGroup, GetReducerFileGroupResponse}
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcEndpointRef
import org.apache.celeborn.common.util.{ThreadUtils, Utils}

/**
 * Fault-injection regressions for empty reads of requested partitions.
 *
 * Only the RPC transport is mocked. Mapper metadata registration, file-group loading,
 * partition reading, report creation, and driver integrity comparison use production code.
 */
object MissingPartitionIntegrityTestSupport {

  val shuffleId = 7
  val partitionId = 1
  val payload = "missing partition payload".getBytes(UTF_8)

  class Fixture(
      nonempty: Boolean = true,
      emptyLocationEntry: Boolean = false,
      fileGroupStatus: StatusCode = StatusCode.SUCCESS,
      integrityCheckEnabled: Boolean = true,
      optimizedSkewRead: Boolean = false,
      numMappers: Int = 1) extends AutoCloseable {
    val conf = new CelebornConf()
    conf.set("celeborn.client.shuffle.integrityCheck.enabled", integrityCheckEnabled.toString)
    conf.set(
      "celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled",
      optimizedSkewRead.toString)

    private val rpcPool = ThreadUtils.newDaemonCachedThreadPool("missing-partition-repro-rpc")
    private val scheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("missing-partition-repro-scheduler")
    val handler = new ReducePartitionCommitHandler(
      "missing-partition-repro",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      rpcPool,
      scheduler,
      null)

    val expected = new CommitMetadata()
    if (nonempty) {
      expected.addDataWithOffsetAndLength(payload, 0, payload.length)
    }
    handler.registerShuffle(shuffleId, numMappers, isSegmentGranularityVisible = false, 3)
    (0 until numMappers).foreach { mapId =>
      val metadata = if (mapId == numMappers - 1) expected else new CommitMetadata()
      assert(handler.finishMapperAttempt(
        shuffleId,
        mapId,
        0,
        numMappers,
        partitionId,
        null,
        _ => (),
        3,
        Array(0, metadata.getChecksum, 0),
        Array(0L, metadata.getBytes, 0L)) == ((true, mapId == numMappers - 1)))
    }

    val fileGroupRequests = new AtomicInteger()
    val fetchFailures = new AtomicInteger()
    val reports = new ArrayBuffer[PbReadReducerPartitionEnd]()
    val fileGroups = new util.HashMap[Integer, util.Set[PartitionLocation]]()
    // Keep another partition visible so this is a missing partition within a known shuffle.
    fileGroups.put(
      0,
      util.Collections.singleton(new PartitionLocation(
        0,
        0,
        "localhost",
        19001,
        19002,
        19003,
        19004,
        PartitionLocation.Mode.PRIMARY)))
    if (emptyLocationEntry) {
      fileGroups.put(partitionId, new util.HashSet[PartitionLocation]())
    }

    private val endpoint = mock(
      classOf[RpcEndpointRef],
      new Answer[AnyRef] {
        override def answer(invocation: InvocationOnMock): AnyRef = {
          if (invocation.getMethod.getName == "askSync") {
            invocation.getArgument[AnyRef](0) match {
              case request: GetReducerFileGroup =>
                assert(request.shuffleId == shuffleId)
                fileGroupRequests.incrementAndGet()
                GetReducerFileGroupResponse(
                  fileGroupStatus,
                  fileGroups,
                  handler.getMapperAttempts(shuffleId))
              case request: PbReadReducerPartitionEnd =>
                reports += request
                val (valid, error) = handler.finishPartition(
                  request.getShuffleId,
                  request.getPartitionId,
                  request.getStartMaxIndex,
                  request.getEndMapIndex,
                  new CommitMetadata(request.getCrc32, request.getBytesWritten))
                PbReadReducerPartitionEndResponse.newBuilder()
                  .setStatus(
                    if (valid) StatusCode.SUCCESS.getValue
                    else StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue)
                  .setErrorMsg(error)
                  .build()
              case _: PbGetStageEnd =>
                PbGetStageEndResponse.newBuilder().setStageEnd(true).build()
              case _: PbGetShuffleId =>
                PbGetShuffleIdResponse.newBuilder().setShuffleId(shuffleId).setSuccess(true).build()
              case _: PbReportShuffleFetchFailure =>
                fetchFailures.incrementAndGet()
                PbReportShuffleFetchFailureResponse.newBuilder().setSuccess(true).build()
              case other =>
                throw new AssertionError(s"Unexpected RPC: $other")
            }
          } else {
            Answers.RETURNS_DEFAULTS.answer(invocation)
          }
        }
      })

    val client = new ShuffleClientImpl(
      "missing-partition-repro",
      conf,
      UserIdentifier("repro", "repro"))
    client.setupLifecycleManagerRef(endpoint)

    def openPartition(
        startMapIndex: Int = 0,
        endMapIndex: Int = Int.MaxValue,
        alreadyLoaded: Boolean = false,
        requestedShuffleId: Int = shuffleId): CelebornInputStream = {
      val loaded = if (alreadyLoaded) client.updateFileGroup(shuffleId, partitionId) else null
      val locations =
        if (loaded == null) {
          null
        } else if (loaded.partitionGroups.containsKey(partitionId)) {
          new util.ArrayList[PartitionLocation](loaded.partitionGroups.get(partitionId))
        } else {
          new util.ArrayList[PartitionLocation]()
        }
      client.readPartition(
        requestedShuffleId,
        shuffleId,
        partitionId,
        0,
        0L,
        startMapIndex,
        endMapIndex,
        null,
        locations,
        null,
        null,
        null,
        if (loaded == null) null else loaded.mapAttempts,
        null,
        true)
    }

    def readToEnd(
        startMapIndex: Int = 0,
        endMapIndex: Int = Int.MaxValue,
        alreadyLoaded: Boolean = false): Long = {
      val stream = openPartition(startMapIndex, endMapIndex, alreadyLoaded)
      try {
        val buffer = new Array[Byte](128)
        var total = 0L
        var count = stream.read(buffer)
        while (count != -1) {
          total += count
          count = stream.read(buffer)
        }
        total
      } finally {
        stream.close()
      }
    }

    def report(
        metadata: CommitMetadata,
        startMapIndex: Int = 0,
        endMapIndex: Int = Int.MaxValue): Unit = {
      client.readReducerPartitionEnd(
        shuffleId,
        partitionId,
        startMapIndex,
        endMapIndex,
        metadata.getChecksum,
        metadata.getBytes)
    }

    override def close(): Unit = {
      client.shutdown()
      rpcPool.shutdownNow()
      scheduler.shutdownNow()
    }
  }
}

class MissingPartitionIntegritySuite extends CelebornFunSuite {

  import MissingPartitionIntegrityTestSupport._

  for {
    emptyLocationEntry <- Seq(false, true)
    alreadyLoaded <- Seq(false, true)
  } {
    test(s"nonempty partition must reject empty read: emptyLocationEntry=$emptyLocationEntry, alreadyLoaded=$alreadyLoaded") {
      val fixture = new Fixture(emptyLocationEntry = emptyLocationEntry)
      try {
        var readBytes = -1L
        val outcome = scala.util.Try {
          readBytes = fixture.readToEnd(alreadyLoaded = alreadyLoaded)
        }
        assert(fixture.fileGroupRequests.get() == 1)
        assert(fixture.expected.getBytes > 0)
        assert(
          outcome.failed.toOption.exists(error =>
            error.isInstanceOf[CelebornIOException] &&
              error.getMessage.contains("CommitMetadata mismatch")),
          s"Missing partition was accepted: expected=${fixture.expected.getBytes}, " +
            s"actual=$readBytes, integrityReports=${fixture.reports.size}")
        assert(fixture.reports.size == 1)
        val report = fixture.reports.head
        assert(report.getPartitionId == partitionId)
        assert(report.getStartMaxIndex == 0 && report.getEndMapIndex == Int.MaxValue)
        assert(report.getBytesWritten == 0 && report.getCrc32 == 0)
      } finally {
        fixture.close()
      }
    }
  }

  test("control: driver rejects an explicit zero-byte report for the same nonempty partition") {
    val fixture = new Fixture()
    try {
      val error = intercept[CelebornIOException] {
        fixture.report(new CommitMetadata())
      }
      assert(error.getMessage.contains("CommitMetadata mismatch"))
      assert(fixture.reports.size == 1)
      assert(fixture.reports.head.getBytesWritten == 0)
      info(s"CONTROL zero-byte report rejected by the real handler: ${error.getMessage}")
    } finally {
      fixture.close()
    }
  }

  test("control: driver accepts the actual payload metadata") {
    val fixture = new Fixture()
    try {
      fixture.report(fixture.expected)
      assert(fixture.reports.size == 1)
      assert(fixture.reports.head.getBytesWritten == payload.length)
    } finally {
      fixture.close()
    }
  }

  test("control: a legitimately empty partition reads empty and passes zero-byte validation") {
    val fixture = new Fixture(nonempty = false)
    try {
      assert(fixture.expected.getBytes == 0)
      assert(fixture.readToEnd() == 0)
      assert(fixture.reports.size == 1)
      assert(fixture.reports.head.getBytesWritten == 0)
    } finally {
      fixture.close()
    }
  }

  test("control: a SHUFFLE_DATA_LOST response is rejected before returning an empty stream") {
    val fixture = new Fixture(fileGroupStatus = StatusCode.SHUFFLE_DATA_LOST)
    try {
      val error = intercept[CelebornIOException] {
        fixture.readToEnd()
      }
      assert(error.getMessage.contains("SHUFFLE_DATA_LOST"))
      assert(fixture.fileGroupRequests.get() == 1)
      assert(fixture.reports.isEmpty)
    } finally {
      fixture.close()
    }
  }

  Seq(false, true).foreach { emptyLocationEntry =>
    test(
      s"disabled integrity checking preserves empty reads: emptyLocationEntry=$emptyLocationEntry") {
      val fixture = new Fixture(
        emptyLocationEntry = emptyLocationEntry,
        integrityCheckEnabled = false)
      try {
        assert(fixture.readToEnd() == 0)
        assert(fixture.reports.isEmpty)
      } finally {
        fixture.close()
      }
    }
  }

  test("an unknown shuffle ID has no registered metadata to validate") {
    val fixture = new Fixture()
    try {
      val stream = fixture.openPartition(requestedShuffleId = Utils.UNKNOWN_APP_SHUFFLE_ID)
      try {
        assert(stream.read() == -1)
        assert(fixture.fileGroupRequests.get() == 0)
        assert(fixture.reports.isEmpty)
      } finally {
        stream.close()
      }
    } finally {
      fixture.close()
    }
  }

  Seq(false, true).foreach { nullLocations =>
    test(s"direct input-stream factory validates empty locations: nullLocations=$nullLocations") {
      val fixture = new Fixture()
      try {
        val error = intercept[CelebornIOException] {
          val stream = CelebornInputStream.create(
            fixture.conf,
            null,
            "missing-partition-repro-7",
            if (nullLocations) null else new util.ArrayList[PartitionLocation](),
            null,
            Array(0),
            null,
            null,
            0,
            0L,
            0,
            Int.MaxValue,
            null,
            fixture.client,
            shuffleId,
            shuffleId,
            partitionId,
            null,
            null,
            true,
            Optional.empty())
          try {
            stream.read()
          } finally {
            stream.close()
          }
        }
        assert(error.getMessage.contains("CommitMetadata mismatch"))
        assert(fixture.reports.size == 1)
      } finally {
        fixture.close()
      }
    }
  }

  Seq(false, true).foreach { optimizedSkewRead =>
    val first = if (optimizedSkewRead) (2, 0) else (0, 1)
    val second = if (optimizedSkewRead) (2, 1) else (1, 2)

    test(s"a legitimate empty AQE subpartition completes validation with its sibling: optimized=$optimizedSkewRead") {
      val fixture = new Fixture(optimizedSkewRead = optimizedSkewRead, numMappers = 2)
      try {
        assert(fixture.readToEnd(first._1, first._2) == 0)
        assert(fixture.reports.size == 1)
        assert(fixture.reports.head.getStartMaxIndex == first._1)
        assert(fixture.reports.head.getEndMapIndex == first._2)
        fixture.report(fixture.expected, second._1, second._2)
        assert(fixture.reports.size == 2)
        assert(fixture.handler.finishPartition(
          shuffleId,
          partitionId,
          second._1,
          second._2,
          new CommitMetadata(fixture.expected.getChecksum, fixture.expected.getBytes)) ==
          ((true, "Partition is complete")))
      } finally {
        fixture.close()
      }
    }

    test(s"all-empty AQE reports reject a nonempty parent: optimized=$optimizedSkewRead") {
      val fixture = new Fixture(optimizedSkewRead = optimizedSkewRead, numMappers = 2)
      try {
        assert(fixture.readToEnd(first._1, first._2) == 0)
        val error = intercept[CelebornIOException] {
          fixture.readToEnd(second._1, second._2)
        }
        assert(error.getMessage.contains("failed validation check"))
        assert(fixture.reports.size == 2)
      } finally {
        fixture.close()
      }
    }
  }

  test("repeated EOF reads do not report an empty partition more than once") {
    val fixture = new Fixture(nonempty = false)
    try {
      val stream = fixture.openPartition()
      try {
        assert(stream.read() == -1)
        assert(stream.read(new Array[Byte](8)) == -1)
        assert(stream.read() == -1)
        assert(fixture.reports.size == 1)
      } finally {
        stream.close()
      }
      assert(fixture.reports.size == 1)
    } finally {
      fixture.close()
    }
  }
}
