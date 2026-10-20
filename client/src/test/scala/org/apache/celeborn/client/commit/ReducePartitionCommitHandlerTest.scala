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

import java.lang.management.ManagementFactory
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, FutureTask, ScheduledExecutorService, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.client.CommitManager.CommittedPartitionInfo
import org.apache.celeborn.client.LifecycleManager.ShuffleAllocatedWorkers
import org.apache.celeborn.client.WorkerStatusTracker
import org.apache.celeborn.common.{CelebornConf, CommitMetadata}
import org.apache.celeborn.common.util.ThreadUtils

class ReducePartitionCommitHandlerTest extends CelebornFunSuite with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(10, Millis))

  private var rpcPool: ThreadPoolExecutor = _
  private var commitScheduler: ScheduledExecutorService = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rpcPool = ThreadUtils.newDaemonCachedThreadPool("test-reduce-commit-rpc")
    commitScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("test-reduce-commit-scheduler")
  }

  override def afterAll(): Unit = {
    try {
      if (rpcPool != null) {
        ThreadUtils.shutdown(rpcPool)
      }
      if (commitScheduler != null) {
        ThreadUtils.shutdown(commitScheduler)
      }
    } finally {
      super.afterAll()
    }
  }

  private def newHandler(integrityCheckEnabled: Boolean = true): ReducePartitionCommitHandler = {
    val conf = new CelebornConf()
      .set(CelebornConf.CLIENT_SHUFFLE_INTEGRITY_CHECK_ENABLED, integrityCheckEnabled)
    new ReducePartitionCommitHandler(
      "test-app",
      conf,
      new ShuffleAllocatedWorkers(),
      new CommittedPartitionInfo(),
      new WorkerStatusTracker(conf, null),
      rpcPool,
      commitScheduler,
      null)
  }

  private def finishMapper(
      handler: ReducePartitionCommitHandler,
      shuffleId: Int = 1,
      mapId: Int = 0,
      attemptId: Int = 0,
      numMappers: Int = 1,
      checksums: Array[Int] = Array(10, 20),
      bytes: Array[Long] = Array(100L, 200L)): (Boolean, Boolean) = {
    handler.finishMapperAttempt(
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      partitionId = -1,
      pushFailedBatches = null,
      recordWorkerFailure = _ => (),
      numPartitions = 2,
      checksums,
      bytes)
  }

  private class PausedCommitMetadata extends CommitMetadata {
    val entered = new CountDownLatch(1)
    val resume = new CountDownLatch(1)
    private val pauseNextUpdate = new AtomicBoolean(true)

    override def addCommitData(checksum: Int, numBytes: Long): Unit = {
      if (pauseNextUpdate.getAndSet(false)) {
        entered.countDown()
        assert(resume.await(30, TimeUnit.SECONDS), "metadata aggregation was not resumed")
      }
      super.addCommitData(checksum, numBytes)
    }
  }

  private def pauseMetadata(
      handler: ReducePartitionCommitHandler,
      numMappers: Int = 1): PausedCommitMetadata = {
    handler.registerShuffle(1, numMappers, isSegmentGranularityVisible = false, numPartitions = 2)
    // Pause the last partition so the first partition has already been aggregated.
    val field = classOf[ReducePartitionCommitHandler].getDeclaredField("commitMetadataForReducer")
    field.setAccessible(true)
    val metadata =
      field.get(handler).asInstanceOf[ConcurrentHashMap[Integer, Array[CommitMetadata]]]
    val paused = new PausedCommitMetadata()
    metadata.get(1)(1) = paused
    paused
  }

  private def startMapper(
      body: => (Boolean, Boolean)): (Thread, FutureTask[(Boolean, Boolean)]) = {
    val task = new FutureTask[(Boolean, Boolean)](new Callable[(Boolean, Boolean)] {
      override def call(): (Boolean, Boolean) = body
    })
    val thread = new Thread(task, "test-mapper-end")
    thread.setDaemon(true)
    thread.start()
    (thread, task)
  }

  private def join(thread: Thread): Unit = {
    thread.join(10000)
    assert(!thread.isAlive, "MapperEnd thread did not exit")
  }

  private def awaitBlockedOrFinished(
      thread: Thread,
      task: FutureTask[(Boolean, Boolean)],
      lockOwner: Thread): Unit = {
    eventually {
      val info = ManagementFactory.getThreadMXBean.getThreadInfo(thread.getId)
      assert(task.isDone || (info != null &&
        info.getThreadState == Thread.State.BLOCKED && info.getLockOwnerId == lockOwner.getId))
    }
  }

  private def checkMetadata(
      handler: ReducePartitionCommitHandler,
      checksums: Array[Int],
      bytes: Array[Long]): Unit = {
    checksums.indices.foreach { partitionId =>
      val (valid, message) = handler.finishPartition(
        1,
        partitionId,
        0,
        Int.MaxValue,
        new CommitMetadata(checksums(partitionId), bytes(partitionId)))
      assert(valid, message)
    }
  }

  test("mapper completion is published only after all integrity metadata is aggregated") {
    val handler = newHandler()
    val paused = pauseMetadata(handler)
    val (thread, task) = startMapper(finishMapper(handler))
    try {
      assert(paused.entered.await(10, TimeUnit.SECONDS))
      assert(!handler.isMapperEnded(1, 0))
      assert(!handler.areAllMapperAttemptsFinished(1))
      assert(handler.getMapperAttempts(1).sameElements(Array(-1)))

      paused.resume.countDown()
      assert(task.get(10, TimeUnit.SECONDS) == ((true, true)))
      assert(handler.isMapperEnded(1, 0))
      assert(handler.areAllMapperAttemptsFinished(1))
      checkMetadata(handler, Array(10, 20), Array(100L, 200L))
    } finally {
      paused.resume.countDown()
      join(thread)
    }
  }

  Seq((0, "retried"), (1, "speculative")).foreach { case (attemptId, description) =>
    test(s"a $description MapperEnd cannot return before the accepted attempt records metadata") {
      val handler = newHandler()
      val paused = pauseMetadata(handler)
      val (acceptedThread, acceptedTask) = startMapper(finishMapper(handler))
      var duplicateThread: Thread = null
      try {
        assert(paused.entered.await(10, TimeUnit.SECONDS))
        val (thread, duplicateTask) = startMapper {
          // A speculative attempt may produce different bytes; only the accepted attempt counts.
          finishMapper(
            handler,
            attemptId = attemptId,
            checksums = if (attemptId == 0) Array(10, 20) else Array(30, 40),
            bytes = if (attemptId == 0) Array(100L, 200L) else Array(300L, 400L))
        }
        duplicateThread = thread
        awaitBlockedOrFinished(thread, duplicateTask, acceptedThread)
        // LifecycleManager replies SUCCESS even for a duplicate MapperEnd. Returning here
        // would let the mapper finish while the accepted attempt's metadata is incomplete.
        assert(!duplicateTask.isDone, "duplicate MapperEnd returned before metadata was aggregated")

        paused.resume.countDown()
        assert(acceptedTask.get(10, TimeUnit.SECONDS) == ((true, true)))
        assert(duplicateTask.get(10, TimeUnit.SECONDS) == ((false, false)))
        checkMetadata(handler, Array(10, 20), Array(100L, 200L))
      } finally {
        paused.resume.countDown()
        join(acceptedThread)
        if (duplicateThread != null) {
          join(duplicateThread)
        }
      }
    }
  }

  test("another mapper cannot finish the stage before earlier mapper metadata is aggregated") {
    val handler = newHandler()
    val paused = pauseMetadata(handler, numMappers = 2)
    val (firstThread, firstTask) = startMapper(finishMapper(handler, numMappers = 2))
    var lastThread: Thread = null
    try {
      assert(paused.entered.await(10, TimeUnit.SECONDS))
      val (thread, lastTask) = startMapper {
        finishMapper(
          handler,
          mapId = 1,
          numMappers = 2,
          checksums = Array(30, 40),
          bytes = Array(300L, 400L))
      }
      lastThread = thread
      awaitBlockedOrFinished(thread, lastTask, firstThread)
      assert(!lastTask.isDone, "the last MapperEnd returned before earlier metadata was aggregated")
      assert(!handler.areAllMapperAttemptsFinished(1))

      paused.resume.countDown()
      assert(firstTask.get(10, TimeUnit.SECONDS) == ((true, false)))
      assert(lastTask.get(10, TimeUnit.SECONDS) == ((true, true)))
      assert(handler.areAllMapperAttemptsFinished(1))
      checkMetadata(handler, Array(40, 60), Array(400L, 600L))
    } finally {
      paused.resume.countDown()
      join(firstThread)
      if (lastThread != null) {
        join(lastThread)
      }
    }
  }

  test("metadata aggregation does not block MapperEnd for another shuffle") {
    val handler = newHandler()
    val paused = pauseMetadata(handler)
    val (firstThread, firstTask) = startMapper(finishMapper(handler))
    var otherThread: Thread = null
    try {
      assert(paused.entered.await(10, TimeUnit.SECONDS))
      val (thread, otherTask) = startMapper(finishMapper(handler, shuffleId = 2))
      otherThread = thread
      assert(otherTask.get(10, TimeUnit.SECONDS) == ((true, true)))
      assert(handler.areAllMapperAttemptsFinished(2))
      assert(!firstTask.isDone)
      paused.resume.countDown()
      assert(firstTask.get(10, TimeUnit.SECONDS) == ((true, true)))
    } finally {
      paused.resume.countDown()
      join(firstThread)
      if (otherThread != null) {
        join(otherThread)
      }
    }
  }

  test("MapperEnd accepts empty metadata arrays when integrity checking is disabled") {
    val handler = newHandler(integrityCheckEnabled = false)
    assert(finishMapper(
      handler,
      numMappers = 2,
      checksums = Array.emptyIntArray,
      bytes = Array.emptyLongArray) == ((true, false)))
    assert(finishMapper(
      handler,
      numMappers = 2,
      attemptId = 1,
      checksums = Array.emptyIntArray,
      bytes = Array.emptyLongArray) == ((false, false)))
    assert(finishMapper(
      handler,
      mapId = 1,
      numMappers = 2,
      checksums = Array.emptyIntArray,
      bytes = Array.emptyLongArray) == ((true, true)))
    assert(handler.areAllMapperAttemptsFinished(1))
  }
}
