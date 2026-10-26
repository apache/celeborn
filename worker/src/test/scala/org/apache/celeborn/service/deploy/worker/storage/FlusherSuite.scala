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
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import io.netty.buffer.{CompositeByteBuf, UnpooledByteBufAllocator}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.DiskStatus
import org.apache.celeborn.service.deploy.worker.WorkerSource

class FlusherSuite extends AnyFunSuite with Eventually {
  private class TestFlusher(conf: CelebornConf) extends Flusher(
      new WorkerSource(conf),
      1,
      UnpooledByteBufAllocator.DEFAULT,
      16,
      null,
      "test",
      false,
      1024,
      conf) {
    val returned = new AtomicInteger()
    val errors = new AtomicInteger()

    override def returnBuffer(buffer: CompositeByteBuf, keepBuffer: Boolean): Unit = {
      buffer.release()
      returned.incrementAndGet()
    }

    override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit =
      errors.incrementAndGet()

    override def getFlushTimeMetric(): String = WorkerSource.FLUSH_LOCAL_DATA_TIME

    def stop(): Unit = {
      stopFlag.set(true)
      workers.foreach(_.shutdownNow())
      workers.foreach(worker => assert(worker.awaitTermination(5, TimeUnit.SECONDS)))
    }
  }

  private def task(flusher: Flusher, notifier: FlushNotifier)(body: => Unit): FlushTask = {
    notifier.numPendingFlushes.incrementAndGet()
    new FlushTask(
      UnpooledByteBufAllocator.DEFAULT.compositeBuffer(),
      notifier,
      false,
      flusher.workerSource) {
      override def flush(copyBytes: Array[Byte]): Unit = body
    }
  }

  for (enabled <- Seq(false, true)) {
    test(s"committing writer priority enabled=$enabled preserves in-flight and per-file order") {
      val conf = new CelebornConf().set(
        CelebornConf.WORKER_FLUSHER_COMMITTING_WRITERS_PRIORITY_ENABLED.key,
        enabled.toString)
      val flusher = new TestFlusher(conf)
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val seen = new ConcurrentLinkedQueue[String]()
      val writer = new FlushNotifier
      val other = new FlushNotifier
      val first = task(flusher, writer) {
        started.countDown()
        assert(release.await(10, TimeUnit.SECONDS))
        seen.add("first")
      }
      val normal = task(flusher, other) { seen.add("normal") }
      val middle = task(flusher, writer) { seen.add("middle") }
      val last = task(flusher, writer) { seen.add("last") }
      try {
        assert(flusher.addTask(first, 1000, 0))
        assert(started.await(5, TimeUnit.SECONDS))
        Seq(normal, middle, last).foreach(t => assert(flusher.addTask(t, 1000, 0)))
        flusher.closeWriter(writer, 0)
        release.countDown()
        eventually(timeout(5.seconds)) {
          assert(writer.numPendingFlushes.get() == 0)
          assert(other.numPendingFlushes.get() == 0)
        }
        val expected =
          if (enabled) Seq("first", "middle", "last", "normal")
          else Seq("first", "normal", "middle", "last")
        assert(seen.asScala.toSeq == expected)
        assert(flusher.returned.get() == 4)
        Seq(first, normal, middle, last).foreach(t => assert(t.buffer.refCnt() == 0))
      } finally {
        release.countDown()
        flusher.stop()
      }
    }
  }

  test("failed priority flushes release each buffer and pending count once") {
    val flusher = new TestFlusher(new CelebornConf())
    val started = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val blocker = new FlushNotifier
    val failed = new FlushNotifier
    val normal = new FlushNotifier
    val attempts = new AtomicInteger()
    val first = task(flusher, blocker) {
      started.countDown()
      assert(release.await(10, TimeUnit.SECONDS))
    }
    val ordinary = task(flusher, normal) {}
    val failure = task(flusher, failed) {
      attempts.incrementAndGet()
      throw new IOException("injected flush failure")
    }
    val skipped = task(flusher, failed) { attempts.incrementAndGet() }
    try {
      assert(flusher.addTask(first, 1000, 0))
      assert(started.await(5, TimeUnit.SECONDS))
      Seq(ordinary, failure, skipped).foreach(t => assert(flusher.addTask(t, 1000, 0)))
      flusher.closeWriter(failed, 0)
      release.countDown()
      eventually(timeout(5.seconds)) {
        Seq(blocker, failed, normal).foreach(n => assert(n.numPendingFlushes.get() == 0))
      }
      assert(failed.hasException)
      assert(attempts.get() == 1)
      assert(flusher.errors.get() == 1)
      assert(flusher.returned.get() == 4)
      Seq(first, ordinary, failure, skipped).foreach(t => assert(t.buffer.refCnt() == 0))
    } finally {
      release.countDown()
      flusher.stop()
    }
  }
}
