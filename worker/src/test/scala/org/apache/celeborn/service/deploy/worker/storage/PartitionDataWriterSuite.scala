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
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.{ByteBuf, CompositeByteBuf, Unpooled}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar._
import org.mockito.invocation.InvocationOnMock
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.WorkerSource

class PartitionDataWriterSuite extends AnyFunSuite {
  private class TestTierWriter(
      conf: CelebornConf,
      pending: AtomicInteger,
      notifier: FlushNotifier,
      source: WorkerSource,
      fileInfo: MemoryFileInfo,
      storageManager: StorageManager)
    extends TierWriterBase(
      conf,
      new ReducePartitionMetaHandler(false, fileInfo),
      pending,
      notifier,
      fileInfo,
      source,
      StorageInfo.Type.MEMORY,
      "0-0",
      "app-1",
      storageManager,
      fileInfo.getUserIdentifier) {
    val draining = new CountDownLatch(1)
    val drainCalls = new AtomicInteger()

    override def waitOnNoPendingWrites(): Unit = {
      drainCalls.incrementAndGet()
      draining.countDown()
      super.waitOnNoPendingWrites()
    }

    override def writeInternal(buf: ByteBuf): Unit = {
      flushBuffer.addComponent(true, buf.retain())
      accountAddedBytes(buf.readableBytes())
    }

    override protected def accountAddedBytes(numBytes: Int): Unit = {
      metaHandler.afterFlush(numBytes)
    }

    override def needEvict(): Boolean = false
    override def evict(writer: TierWriterBase): Unit = {}
    override def handleException(): Unit = {}
    override def closeStreams(): Unit = {}
    override def genFlushTask(finalFlush: Boolean, keepBuffer: Boolean): FlushTask = null
    override def addFlushTask(task: FlushTask): Unit = {}
    override def takeBufferInternal(): CompositeByteBuf = Unpooled.compositeBuffer()
    override def returnBufferInternal(destroy: Boolean): Unit = {
      if (flushBuffer != null) {
        flushBuffer.release()
        flushBuffer = null
      }
    }
    override def getFlusher(): Flusher = null
  }

  private def prepareWriter(): (PartitionDataWriter, TestTierWriter) = {
    val conf = new CelebornConf()
    conf.set("celeborn.worker.writer.close.timeout", "5s")
    val source = mock[WorkerSource]
    val storageManager = mock[StorageManager]
    val storagePolicy = mock[StoragePolicy]
    when(storageManager.storagePolicy).thenReturn(storagePolicy)
    val user = UserIdentifier("tenant", "user")
    val context = new PartitionDataWriterContext(
      1024 * 1024,
      PartitionSplitMode.HARD,
      false,
      new PartitionLocation(0, 0, "localhost", 1, 2, 3, 4, PartitionLocation.Mode.PRIMARY),
      "app-1",
      1,
      user,
      PartitionType.REDUCE,
      false,
      false)
    var tier: TestTierWriter = null
    when(storagePolicy.createFileWriter(
      any[PartitionDataWriterContext],
      any[AtomicInteger],
      any[FlushNotifier])).thenAnswer { invocation: InvocationOnMock =>
      tier = new TestTierWriter(
        conf,
        invocation.getArgument[AtomicInteger](1),
        invocation.getArgument[FlushNotifier](2),
        source,
        new MemoryFileInfo(user, false, new ReduceFileMeta(conf.shuffleChunkSize)),
        storageManager)
      tier
    }
    val writer = new PartitionDataWriter(
      storageManager,
      source,
      conf,
      DeviceMonitor.EmptyMonitor(),
      context)
    (writer, tier)
  }

  test("close drains admitted pushes and rejects late pushes without changing pending writes") {
    val (writer, tier) = prepareWriter()
    val executor = Executors.newSingleThreadExecutor()
    val buf = Unpooled.wrappedBuffer(Array[Byte](1, 2, 3))
    try {
      assert(writer.tryIncrementPendingWrites())
      val close = executor.submit(new Callable[Long] {
        override def call(): Long = writer.close()
      })
      assert(tier.draining.await(2, TimeUnit.SECONDS))
      assert(!close.isDone)
      assert(!writer.tryIncrementPendingWrites())
      assert(tier.numPendingWrites.get() == 1)
      // The admitted push must acquire the writer monitor while close is draining.
      writer.write(buf)
      assert(close.get(2, TimeUnit.SECONDS) == 3)
      assert(writer.isClosed)
      assert(tier.numPendingWrites.get() == 0)
      assert(!writer.tryIncrementPendingWrites())
      assert(tier.numPendingWrites.get() == 0)
    } finally {
      buf.release()
      writer.destroy(new IOException("test cleanup"))
      executor.shutdownNow()
      assert(executor.awaitTermination(2, TimeUnit.SECONDS))
    }
  }

  test("cancelling an admitted push lets close finish") {
    val (writer, tier) = prepareWriter()
    val executor = Executors.newSingleThreadExecutor()
    try {
      assert(writer.tryIncrementPendingWrites())
      val close = executor.submit(new Callable[Long] {
        override def call(): Long = writer.close()
      })
      assert(tier.draining.await(2, TimeUnit.SECONDS))
      writer.decrementPendingWrites()
      assert(close.get(2, TimeUnit.SECONDS) == 0)
      assert(tier.numPendingWrites.get() == 0)
      assert(!writer.tryIncrementPendingWrites())
    } finally {
      writer.destroy(new IOException("test cleanup"))
      executor.shutdownNow()
      assert(executor.awaitTermination(2, TimeUnit.SECONDS))
    }
  }

  test("close drains writes across tier eviction and closes the current tier") {
    val (writer, tier) = prepareWriter()
    val replacement = new TestTierWriter(
      tier.conf,
      tier.numPendingWrites,
      tier.notifier,
      tier.source.asInstanceOf[WorkerSource],
      new MemoryFileInfo(
        UserIdentifier("tenant", "user"),
        false,
        new ReduceFileMeta(tier.conf.shuffleChunkSize)),
      tier.storageManager)
    when(tier.storageManager.storagePolicy.getEvictedFileWriter(
      any[TierWriterBase],
      any[PartitionDataWriterContext],
      any[AtomicInteger],
      any[FlushNotifier])).thenReturn(replacement)
    val executor = Executors.newSingleThreadExecutor()
    val buf = Unpooled.wrappedBuffer(Array[Byte](1, 2, 3))
    try {
      assert(writer.tryIncrementPendingWrites())
      val close = executor.submit(new Callable[Long] {
        override def call(): Long = writer.close()
      })
      assert(tier.draining.await(2, TimeUnit.SECONDS))
      writer.evict(true)
      assert(!writer.tryIncrementPendingWrites())
      writer.write(buf)
      assert(close.get(2, TimeUnit.SECONDS) == 3)
      assert(replacement.closed)
      assert(writer.getCurrentFileInfo eq replacement.fileInfo)
      assert(tier.numPendingWrites.get() == 0)
    } finally {
      buf.release()
      writer.destroy(new IOException("test cleanup"))
      tier.returnBuffer(false)
      executor.shutdownNow()
      assert(executor.awaitTermination(2, TimeUnit.SECONDS))
    }
  }

  test("destroyed writers reject pushes without registering pending writes") {
    val (writer, tier) = prepareWriter()
    writer.destroy(new IOException("destroy writer"))
    assert(!writer.tryIncrementPendingWrites())
    assert(tier.numPendingWrites.get() == 0)
  }

  test("destroy can finish while close is draining admitted writes") {
    val (writer, tier) = prepareWriter()
    val executor = Executors.newSingleThreadExecutor()
    try {
      assert(writer.tryIncrementPendingWrites())
      val close = executor.submit(new Callable[Long] {
        override def call(): Long = writer.close()
      })
      assert(tier.draining.await(2, TimeUnit.SECONDS))
      writer.destroy(new IOException("destroy while closing"))
      assert(!writer.tryIncrementPendingWrites())
      writer.decrementPendingWrites()
      val error = intercept[ExecutionException](close.get(2, TimeUnit.SECONDS))
      assert(error.getCause.isInstanceOf[AlreadyClosedException])
      assert(tier.flushBuffer == null)
      assert(tier.numPendingWrites.get() == 0)
    } finally {
      writer.destroy(new IOException("test cleanup"))
      executor.shutdownNow()
      assert(executor.awaitTermination(2, TimeUnit.SECONDS))
    }
  }

  test("concurrent closes drain once and preserve the already closed failure") {
    val (writer, tier) = prepareWriter()
    val executor = Executors.newFixedThreadPool(2)
    val secondStarted = new CountDownLatch(1)
    try {
      assert(writer.tryIncrementPendingWrites())
      val first = executor.submit(new Callable[Long] {
        override def call(): Long = writer.close()
      })
      assert(tier.draining.await(2, TimeUnit.SECONDS))
      val second = executor.submit(new Callable[Long] {
        override def call(): Long = {
          secondStarted.countDown()
          writer.close()
        }
      })
      assert(secondStarted.await(2, TimeUnit.SECONDS))
      assert(tier.drainCalls.get() == 1)
      writer.decrementPendingWrites()
      assert(first.get(2, TimeUnit.SECONDS) == 0)
      val error = intercept[ExecutionException](second.get(2, TimeUnit.SECONDS))
      assert(error.getCause.isInstanceOf[AlreadyClosedException])
      assert(tier.drainCalls.get() == 1)
    } finally {
      writer.destroy(new IOException("test cleanup"))
      executor.shutdownNow()
      assert(executor.awaitTermination(2, TimeUnit.SECONDS))
    }
  }

  test("pending write timeout closes the writer and repeated close does not drain again") {
    val (writer, tier) = prepareWriter()
    tier.writerCloseTimeoutMs = 0
    assert(writer.tryIncrementPendingWrites())
    assert(writer.close() == 0)
    assert(writer.isClosed)
    assert(!writer.tryIncrementPendingWrites())
    assert(tier.numPendingWrites.get() == 1)
    assert(tier.drainCalls.get() == 1)
    intercept[AlreadyClosedException](writer.close())
    assert(tier.drainCalls.get() == 1)
    writer.decrementPendingWrites()
    assert(tier.numPendingWrites.get() == 0)
  }

  test("close failure releases resources and keeps write admission closed") {
    val (writer, tier) = prepareWriter()
    val error = new IOException("flush failed")
    tier.notifier.setException(error)
    assert(intercept[IOException](writer.close()) eq error)
    assert(tier.flushBuffer == null)
    assert(!writer.tryIncrementPendingWrites())
    assert(tier.numPendingWrites.get() == 0)
  }

  test("repeated close preserves the already closed failure") {
    val (writer, tier) = prepareWriter()
    assert(writer.close() == 0)
    intercept[AlreadyClosedException](writer.close())
    assert(!writer.tryIncrementPendingWrites())
    assert(tier.numPendingWrites.get() == 0)
  }
}
