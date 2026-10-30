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

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, anyInt}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.{CommitManager, LifecycleManager, ShuffleCommittedInfo, WorkerStatusTracker}
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.protocol.{PartitionType, PbReadReducerPartitionEndResponse}
import org.apache.celeborn.common.protocol.message.ControlMessages.ReadReducerPartitionEnd
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc.RpcCallContext
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils}

class MapPartitionCommitHandlerTest extends CelebornFunSuite {

  // The handler spins up daemon pools; skip the thread audit to avoid flaky leak warnings.
  override protected val enableAutoThreadAudit = false

  private var rpcPool: ThreadPoolExecutor = _
  private var commitScheduler: ScheduledExecutorService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rpcPool = ThreadUtils.newDaemonCachedThreadPool("test-map-commit-rpc")
    commitScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("test-map-commit-scheduler")
  }

  override def afterAll(): Unit = {
    if (rpcPool != null) {
      rpcPool.shutdownNow()
    }
    if (commitScheduler != null) {
      commitScheduler.shutdownNow()
    }
    super.afterAll()
  }

  private def newHandler(integrityCheckEnabled: Boolean): MapPartitionCommitHandler = {
    val conf = new CelebornConf()
    conf.set(
      CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED.key,
      integrityCheckEnabled.toString)
    new MapPartitionCommitHandler(
      "test-app",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      rpcPool,
      commitScheduler,
      null)
  }

  /** Write-side per-subpartition checksum/byte arrays computed over the given payloads. */
  private def writeSideMetadata(payloads: Seq[Array[Byte]]): (Array[Int], Array[Long]) = {
    val crc32PerSubPartition = new Array[Int](payloads.length)
    val bytesPerSubPartition = new Array[Long](payloads.length)
    payloads.zipWithIndex.foreach { case (payload, subIndex) =>
      val commitMetadata = new CommitMetadata()
      commitMetadata.addDataWithOffsetAndLength(payload, 0, payload.length)
      crc32PerSubPartition(subIndex) = commitMetadata.getChecksum
      bytesPerSubPartition(subIndex) = commitMetadata.getBytes
    }
    (crc32PerSubPartition, bytesPerSubPartition)
  }

  /** Read-side commit metadata accumulated over the inclusive subpartition range. */
  private def readSideMetadata(
      payloads: Seq[Array[Byte]],
      startSubIndex: Int,
      endSubIndex: Int): CommitMetadata = {
    val commitMetadata = new CommitMetadata()
    (startSubIndex to endSubIndex).foreach { subIndex =>
      val payload = payloads(subIndex)
      commitMetadata.addDataWithOffsetAndLength(payload, 0, payload.length)
    }
    commitMetadata
  }

  private val shuffleId = 1
  private val mapPartitionId = 0
  private val payloads =
    Seq("sub-0-bytes".getBytes(), "sub-1-bytes".getBytes(), "sub-2-bytes".getBytes())
  private val numPartitions = payloads.length

  test("finishPartition returns success without validation when integrity check is disabled") {
    val handler = newHandler(integrityCheckEnabled = false)
    // No metadata recorded, yet a disabled check must short-circuit to success.
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions - 1, new CommitMetadata())
    assert(isValid)
    assert(message.isEmpty)
  }

  test("finishPartition fails when no commit metadata was recorded for the shuffle") {
    val handler = newHandler(integrityCheckEnabled = true)
    // Neither registerShuffle nor recordMapPartitionCommitMetadata called for this shuffle.
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions - 1, new CommitMetadata())
    assert(!isValid)
    assert(message.contains(s"No write-side commit metadata recorded for shuffle $shuffleId"))
  }

  test("finishPartition fails when no commit metadata was recorded for the map partition") {
    val handler = newHandler(integrityCheckEnabled = true)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    // Record a different map partition so the outer map exists but this one's entry is absent.
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId + 1,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions - 1, new CommitMetadata())
    assert(!isValid)
    assert(message.contains(s"shuffle $shuffleId map partition $mapPartitionId"))
  }

  test("recordMapPartitionCommitMetadata skips reported arrays that do not match numPartitions") {
    val handler = newHandler(integrityCheckEnabled = true)
    handler.registerShuffle(shuffleId, 1, isSegmentGranularityVisible = true, numPartitions)
    // A crc32 array shorter than numPartitions is rejected, so validation later fails closed.
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition.dropRight(1),
      bytesPerSubPartition)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions - 1, new CommitMetadata())
    assert(!isValid)
    assert(message.contains(s"map partition $mapPartitionId"))
  }

  test("finishPartition fails closed after the shuffle metadata was expired/removed") {
    val handler = newHandler(integrityCheckEnabled = true)
    handler.registerShuffle(shuffleId, 1, isSegmentGranularityVisible = true, numPartitions)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    // Cleanup racing a late reader stream end drops the metadata; validation must fail, not pass.
    handler.removeExpiredShuffle(shuffleId)
    val actual = readSideMetadata(payloads, 0, numPartitions - 1)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions - 1, actual)
    assert(!isValid)
    assert(message.contains(s"No write-side commit metadata recorded for shuffle $shuffleId"))
  }

  test("finishPartition fails for an out-of-bounds subpartition range") {
    val handler = newHandler(integrityCheckEnabled = true)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, 0, numPartitions, new CommitMetadata())
    assert(!isValid)
    assert(message.contains("Invalid subpartition range"))
  }

  test("finishPartition succeeds when the read-side metadata matches the write-side range") {
    val handler = newHandler(integrityCheckEnabled = true)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    val startSubIndex = 0
    val endSubIndex = 1
    val actual = readSideMetadata(payloads, startSubIndex, endSubIndex)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, startSubIndex, endSubIndex, actual)
    assert(isValid)
    assert(message.isEmpty)
  }

  test("finishPartition fails when the read-side metadata does not match the write-side range") {
    val handler = newHandler(integrityCheckEnabled = true)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    val startSubIndex = 0
    val endSubIndex = 1
    // Flip a byte in the consumed payload to simulate a corrupted read.
    val corrupted = payloads(endSubIndex).clone()
    corrupted(0) = (corrupted(0) ^ 0x01).toByte
    val actual =
      readSideMetadata(payloads.updated(endSubIndex, corrupted), startSubIndex, endSubIndex)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, startSubIndex, endSubIndex, actual)
    assert(!isValid)
    assert(message.contains("Integrity check failed"))
  }

  test(
    "finishPartition fails on a byte-count mismatch even when the checksum coincidentally matches") {
    val handler = newHandler(integrityCheckEnabled = true)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)
    handler.recordMapPartitionCommitMetadata(
      shuffleId,
      mapPartitionId,
      numPartitions,
      crc32PerSubPartition,
      bytesPerSubPartition)
    val startSubIndex = 0
    val endSubIndex = 1
    // Expected checksum but a wrong byte count (as if a CRC32 collision hid a short/over read).
    val expected = new CommitMetadata()
    (startSubIndex to endSubIndex).foreach { i =>
      expected.addCommitData(crc32PerSubPartition(i), bytesPerSubPartition(i))
    }
    val actual = new CommitMetadata(expected.getChecksum, expected.getBytes + 1)
    assert(expected.getChecksum == actual.getChecksum)
    assert(expected.getBytes != actual.getBytes)
    val (isValid, message) =
      handler.finishPartition(shuffleId, mapPartitionId, startSubIndex, endSubIndex, actual)
    assert(!isValid)
    assert(message.contains("Integrity check failed"))
  }

  test("recordMapPartitionCommitMetadata is safe under concurrent mappers") {
    val handler = newHandler(integrityCheckEnabled = true)
    val numMapPartitions = 16
    handler.registerShuffle(
      shuffleId,
      numMapPartitions,
      isSegmentGranularityVisible = true,
      numPartitions)
    val (crc32PerSubPartition, bytesPerSubPartition) = writeSideMetadata(payloads)

    val startLatch = new CountDownLatch(1)
    val doneLatch = new CountDownLatch(numMapPartitions)
    val errors = new ConcurrentLinkedQueue[Throwable]()
    (0 until numMapPartitions).foreach { mapPartition =>
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          try {
            startLatch.await()
            handler.recordMapPartitionCommitMetadata(
              shuffleId,
              mapPartition,
              numPartitions,
              crc32PerSubPartition,
              bytesPerSubPartition)
          } catch {
            case t: Throwable => errors.add(t)
          } finally {
            doneLatch.countDown()
          }
        }
      })
      thread.setDaemon(true)
      thread.start()
    }
    // Release all threads at once to maximize contention on the shuffle's metadata map.
    startLatch.countDown()
    assert(doneLatch.await(30, TimeUnit.SECONDS), "concurrent record timed out")
    assert(errors.isEmpty, s"concurrent record threw: ${errors.toArray.mkString(", ")}")

    // Every mapper's metadata must have been recorded and independently validate.
    (0 until numMapPartitions).foreach { mapPartition =>
      val actual = readSideMetadata(payloads, 0, numPartitions - 1)
      val (isValid, message) =
        handler.finishPartition(shuffleId, mapPartition, 0, numPartitions - 1, actual)
      assert(isValid, s"map partition $mapPartition failed validation: $message")
    }
  }

  Seq((0, 0), (0, 1)).foreach { case (startSubIndex, endSubIndex) =>
    test(s"MAP integrity failure preserves independent reads and retry for range [$startSubIndex, $endSubIndex]") {
      val handler = newHandler(integrityCheckEnabled = true)
      val (checksums, bytesWritten) = writeSideMetadata(payloads)
      Seq(mapPartitionId, mapPartitionId + 1).foreach { id =>
        handler.recordMapPartitionCommitMetadata(
          shuffleId,
          id,
          numPartitions,
          checksums,
          bytesWritten)
      }

      val manager = mock(classOf[LifecycleManager], CALLS_REAL_METHODS)
      val commit = mock(classOf[CommitManager])
      when(commit.finishPartition(anyInt(), anyInt(), anyInt(), anyInt(), any[CommitMetadata]))
        .thenAnswer((invocation: InvocationOnMock) =>
          handler.finishPartition(
            invocation.getArgument[Int](0),
            invocation.getArgument[Int](1),
            invocation.getArgument[Int](2),
            invocation.getArgument[Int](3),
            invocation.getArgument[CommitMetadata](4)))
      val partitionTypes = JavaUtils.newConcurrentHashMap[Int, PartitionType]()
      partitionTypes.put(shuffleId, PartitionType.MAP)
      val fields = Map[String, AnyRef](
        "commitManager" -> commit,
        "shufflePartitionType" -> partitionTypes,
        "failedIntegrityShuffles" -> JavaUtils.newConcurrentHashMap[Int, String](),
        "shuffleIdMapping" -> JavaUtils.newConcurrentHashMap[
          Int,
          mutable.LinkedHashMap[String, (Int, Boolean)]]())
      fields.foreach { case (name, value) =>
        val field = classOf[LifecycleManager].getDeclaredFields
          .find(f => f.getName == name || f.getName.endsWith("$$" + name)).get
        field.setAccessible(true)
        field.set(manager, value)
      }

      def report(
          partitionId: Int,
          start: Int,
          end: Int,
          metadata: CommitMetadata): PbReadReducerPartitionEndResponse = {
        val context = mock(classOf[RpcCallContext])
        manager.receiveAndReply(context)(ReadReducerPartitionEnd(
          shuffleId,
          partitionId,
          start,
          end,
          metadata.getChecksum,
          metadata.getBytes))
        val response = ArgumentCaptor.forClass(classOf[Object])
        verify(context).reply(response.capture())
        response.getValue.asInstanceOf[PbReadReducerPartitionEndResponse]
      }

      val valid = readSideMetadata(payloads, startSubIndex, endSubIndex)
      val corrupt = new CommitMetadata(valid.getChecksum ^ 1, valid.getBytes)
      assert(report(mapPartitionId, startSubIndex, endSubIndex, corrupt)
        .getStatus == StatusCode.READ_REDUCER_PARTITION_END_FAILED.getValue)
      val otherRange = readSideMetadata(payloads, 2, 2)
      assert(report(mapPartitionId, 2, 2, otherRange).getStatus == StatusCode.SUCCESS.getValue)
      assert(report(mapPartitionId + 1, startSubIndex, endSubIndex, valid)
        .getStatus == StatusCode.SUCCESS.getValue)
      assert(report(mapPartitionId, startSubIndex, endSubIndex, valid)
        .getStatus == StatusCode.SUCCESS.getValue)
    }
  }
}
