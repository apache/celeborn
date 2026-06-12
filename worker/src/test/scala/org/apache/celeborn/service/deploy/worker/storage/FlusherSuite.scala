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

package org.apache.celeborn.service.deploy.worker.storage

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import io.netty.buffer.{ByteBufAllocator, CompositeByteBuf, PooledByteBufAllocator, UnpooledByteBufAllocator}
import org.mockito.Mockito.{timeout, verify}
import org.mockito.MockitoSugar.{mock, spy}
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout => patienceTimeout}
import org.scalatest.time.SpanSugar._

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.DiskStatus
import org.apache.celeborn.common.metrics.source.AbstractSource
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class FlusherSuite extends CelebornFunSuite {

  // The flusher worker threads block in poll() for up to 1000ms (see Flusher)
  // before falling through to the idle/trim branch, so verifications must allow
  // at least that long plus scheduling slack.
  private val VERIFY_TIMEOUT_MS = 5000

  private val flushers = new ArrayBuffer[TestFlusher]()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    MemoryManager.reset()
  }

  override protected def afterEach(): Unit = {
    flushers.foreach(_.shutdown())
    flushers.clear()
    MemoryManager.reset()
    super.afterEach()
  }

  /**
   * Minimal concrete [[Flusher]] used to exercise the worker loop in isolation.
   * The base class starts its worker threads in `init()` on construction.
   */
  private class TestFlusher(
      workerSource: AbstractSource,
      allocator: ByteBufAllocator,
      threadCount: Int)
    extends Flusher(
      workerSource,
      threadCount,
      allocator,
      16,
      null,
      "test-mount",
      false,
      1024L) {

    override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {}

    override def getFlushTimeMetric(): String = WorkerSource.FLUSH_LOCAL_DATA_TIME

    def shutdown(): Unit = {
      stopFlag.set(true)
      // Interrupt the threads blocked in poll() so they exit promptly and the
      // suite's ThreadAudit does not report a leak.
      workers.foreach(worker => if (worker != null) worker.shutdownNow())
    }
  }

  private def newFlusher(
      allocator: ByteBufAllocator,
      threadCount: Int = 1,
      workerSource: AbstractSource = mock[AbstractSource]): TestFlusher = {
    val flusher = new TestFlusher(workerSource, allocator, threadCount)
    flushers += flusher
    flusher
  }

  test("trimCurrentThreadCache is invoked when the working queue stays empty") {
    val allocator = spy(new PooledByteBufAllocator(true))
    newFlusher(allocator)

    // With no tasks ever submitted, the idle branch must trim the pooled
    // allocator's thread-local cache, returning that memory to the shared pool.
    verify(allocator, timeout(VERIFY_TIMEOUT_MS).atLeastOnce()).trimCurrentThreadCache
  }

  test("each idle flusher worker thread trims its own thread cache") {
    val threadCount = 3
    val allocator = spy(new PooledByteBufAllocator(true))
    newFlusher(allocator, threadCount)

    // trimCurrentThreadCache trims only the calling thread's cache, so every
    // idle worker thread must call it - expect at least one call per thread.
    verify(allocator, timeout(VERIFY_TIMEOUT_MS).atLeast(threadCount)).trimCurrentThreadCache
  }

  test("non-pooled allocator skips trim and the worker loop keeps processing tasks") {
    MemoryManager.initialize(new CelebornConf())
    val workerSource = new WorkerSource(new CelebornConf())
    // A non-pooled allocator has no thread cache: the idle branch must take the
    // `case _ =>` no-op path. UnpooledByteBufAllocator has no trimCurrentThreadCache
    // to call, so the only way this test passes is if that path is a clean no-op.
    val flusher = newFlusher(UnpooledByteBufAllocator.DEFAULT, workerSource = workerSource)

    // Let the worker spin through several idle poll cycles before submitting work,
    // so we know the idle branch ran without disrupting the loop.
    Thread.sleep(2500)
    assertTaskIsFlushed(flusher, workerSource)
  }

  test("submitted task is still flushed after switching take() to poll()") {
    MemoryManager.initialize(new CelebornConf())
    val workerSource = new WorkerSource(new CelebornConf())
    val allocator = spy(new PooledByteBufAllocator(true))
    val flusher = newFlusher(allocator, workerSource = workerSource)

    assertTaskIsFlushed(flusher, workerSource)
  }

  /** Submits one task and asserts it is flushed and its pending count drained. */
  private def assertTaskIsFlushed(flusher: TestFlusher, source: AbstractSource): Unit = {
    val bytes = "flush-after-poll".getBytes("UTF-8")
    val buffer: CompositeByteBuf = UnpooledByteBufAllocator.DEFAULT.compositeBuffer()
    buffer.writeBytes(bytes)

    val notifier = new FlushNotifier()
    notifier.numPendingFlushes.incrementAndGet()
    val flushed = new AtomicBoolean(false)
    val task = new FlushTask(buffer, notifier, false, source) {
      override def flush(copyBytes: Array[Byte]): Unit = flushed.set(true)
    }

    assert(flusher.addTask(task, 1000, 0))

    eventually(patienceTimeout(VERIFY_TIMEOUT_MS.millis), interval(50.millis)) {
      assert(flushed.get(), "flush() should have been invoked via the poll() path")
      assert(
        notifier.numPendingFlushes.get() == 0,
        "pending flush count should be decremented after the task is processed")
    }
  }
}
