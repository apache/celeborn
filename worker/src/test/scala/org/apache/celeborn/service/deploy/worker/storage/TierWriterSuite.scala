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
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.netty.buffer.{AbstractByteBufAllocator, ByteBuf, CompositeByteBuf, DuplicatedByteBuf, UnpooledByteBufAllocator}
import org.mockito.Mockito
import org.mockito.MockitoSugar.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterSuite extends AnyFunSuite with BeforeAndAfterEach {
  private class FailBeforeAddCompositeByteBuf(failure: Throwable)
    extends CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, Int.MaxValue) {
    override def addComponent(
        increaseWriterIndex: Boolean,
        buffer: ByteBuf): CompositeByteBuf = throw failure
  }

  private class LargeReadableByteBuf(buffer: ByteBuf, virtualSize: Int)
    extends DuplicatedByteBuf(buffer) {
    override def capacity(): Int = virtualSize

    override def readableBytes(): Int = virtualSize
  }

  private class OomByteBufAllocator(failure: OutOfMemoryError)
    extends AbstractByteBufAllocator(false) {
    override protected def newHeapBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
      throw failure

    override protected def newDirectBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
      throw failure

    override def isDirectBufferPooled(): Boolean = false
  }

  private def capacityOverflowFlushBuffer(): CompositeByteBuf = {
    val flushBuffer = new CompositeByteBuf(
      UnpooledByteBufAllocator.DEFAULT,
      false,
      Int.MaxValue)
    val component = UnpooledByteBufAllocator.DEFAULT.buffer(1, Int.MaxValue).writeByte(0)
    flushBuffer.addComponent(
      true,
      new LargeReadableByteBuf(component, Int.MaxValue - 512))
    flushBuffer
  }

  private def consolidationOomFlushBuffer(failure: OutOfMemoryError): CompositeByteBuf = {
    val flushBuffer = new CompositeByteBuf(new OomByteBufAllocator(failure), false, 1)
    flushBuffer.addComponent(true, UnpooledByteBufAllocator.DEFAULT.buffer(0, 0))
    flushBuffer
  }

  private def restoreCounters(memoryCounterBefore: Long, diskCounterBefore: Long): Unit = {
    val memoryManager = MemoryManager.instance()
    val memoryCounterDelta = memoryManager.getMemoryFileStorageCounter - memoryCounterBefore
    if (memoryCounterDelta > 0) {
      memoryManager.releaseMemoryFileStorage(memoryCounterDelta.toInt)
    } else if (memoryCounterDelta < 0) {
      memoryManager.incrementMemoryFileStorage((-memoryCounterDelta).toInt)
    }
    val diskCounterDelta = memoryManager.getDiskBufferCounter.get() - diskCounterBefore
    if (diskCounterDelta > 0) {
      memoryManager.releaseDiskBuffer(diskCounterDelta.toInt)
    } else if (diskCounterDelta < 0) {
      memoryManager.incrementDiskBuffer((-diskCounterDelta).toInt)
    }
  }

  private def prepareMemoryWriter: MemoryTierWriter = {

    val celebornConf = new CelebornConf()
    celebornConf.set("celeborn.worker.memoryFileStorage.maxFileSize", "80k")
    val reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize)
    val userIdentifier = UserIdentifier("`aa`.`bb`")
    val memoryFileInfo = new MemoryFileInfo(userIdentifier, false, reduceFileMeta)
    val numPendingWriters = new AtomicInteger()
    val flushNotifier = new FlushNotifier()

    val SPLIT_THRESHOLD = 256 * 1024 * 1024L
    val splitMode = PartitionSplitMode.HARD

    val writerContext = new PartitionDataWriterContext(
      SPLIT_THRESHOLD,
      splitMode,
      false,
      new PartitionLocation(
        1,
        0,
        "host",
        1111,
        1112,
        1113,
        1114,
        PartitionLocation.Mode.PRIMARY,
        null),
      "app1-1",
      1,
      userIdentifier,
      PartitionType.REDUCE,
      false,
      false)

    val source = new WorkerSource(celebornConf)

    val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])
    val transConf = new TransportConf("shuffle", new CelebornConf)
    val allocator = NettyUtils.getByteBufAllocator(transConf, source, false)
    val evictedFileCount = new AtomicLong()
    val evictedLocalFileCount = new AtomicLong()
    val evictedDfsFileCount = new AtomicLong()
    when(storageManager.storageBufferAllocator).thenReturn(allocator)
    when(storageManager.localOrDfsStorageAvailable).thenReturn(true)
    when(storageManager.evictedFileCount).thenReturn(evictedFileCount)
    when(storageManager.evictedLocalFileCount).thenReturn(evictedLocalFileCount)
    when(storageManager.evictedDfsFileCount).thenReturn(evictedDfsFileCount)

    MemoryManager.initialize(celebornConf, storageManager, null)

    val tierMemoryWriter = new MemoryTierWriter(
      celebornConf,
      new ReducePartitionMetaHandler(celebornConf.shuffleRangeReadFilterEnabled, memoryFileInfo),
      numPendingWriters,
      flushNotifier,
      source,
      memoryFileInfo,
      StorageInfo.Type.MEMORY,
      writerContext,
      storageManager)
    tierMemoryWriter
  }

  test("test memory tier writer case1") {

    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    for (i <- 2 to 80) {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      memoryTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
      assert(memoryTierWriter.fileInfo.getFileLength === 1024 * i)
    }

    // 8 MB is lesser than the evict threshold
    assert(memoryTierWriter.needEvict() === false)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(WriterUtils.generateSparkFormatData(
      UnpooledByteBufAllocator.DEFAULT,
      0))

    assert(memoryTierWriter.needEvict() === true)

    val filelen = memoryTierWriter.close()
    assert(filelen === 81 * 1024)

    assert(memoryTierWriter.closed === true)

    try {
      memoryTierWriter.write((WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0)))
      // expect already closed exception here
      assert(false)
    } catch {
      case e: AlreadyClosedException =>
        assert(true)
    }

  }

  test("test memory tier writer case2") {

    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    memoryTierWriter.destroy(new IOException("test"))
    assert(memoryTierWriter.flushBuffer.refCnt() === 0)

    try {
      memoryTierWriter.write((WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0)))
      // expect already closed exception here
      assert(false)
    } catch {
      case e: AlreadyClosedException =>
        assert(true)
    }

  }

  private def prepareLocalTierWriter(
      rangeFilter: Boolean,
      celebornConf: CelebornConf = new CelebornConf()): LocalTierWriter = {
    celebornConf.set("celeborn.worker.memoryFileStorage.maxFileSize", "80k")
    celebornConf.set("celeborn.client.shuffle.rangeReadFilter.enabled", rangeFilter.toString)
    val reduceFileMeta = new ReduceFileMeta(celebornConf.shuffleChunkSize)
    val userIdentifier = UserIdentifier("`aa`.`bb`")
    val tmpFile = Files.createTempFile("celeborn", "local-test").toString
    val diskFileInfo =
      new DiskFileInfo(userIdentifier, false, reduceFileMeta, tmpFile, StorageInfo.Type.HDD)
    val numPendingWriters = new AtomicInteger()
    val flushNotifier = new FlushNotifier()
    val source = new WorkerSource(celebornConf)

    val writerContext = new PartitionDataWriterContext(
      1024 * 1024 * 1024,
      PartitionSplitMode.SOFT,
      false,
      new PartitionLocation(
        1,
        0,
        "host",
        1111,
        1112,
        1113,
        1114,
        PartitionLocation.Mode.PRIMARY,
        null),
      "app1-1",
      1,
      userIdentifier,
      PartitionType.REDUCE,
      false,
      false)

    val flusher = new LocalFlusher(
      source,
      DeviceMonitor.EmptyMonitor(),
      1,
      NettyUtils.getByteBufAllocator(new TransportConf("test", celebornConf), null, true),
      256,
      "disk1",
      StorageInfo.Type.HDD,
      null,
      celebornConf.workerFlusherBufferSize)
    val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])

    MemoryManager.initialize(celebornConf, storageManager, null)

    new LocalTierWriter(
      celebornConf,
      new ReducePartitionMetaHandler(celebornConf.shuffleRangeReadFilterEnabled, diskFileInfo),
      numPendingWriters,
      flushNotifier,
      flusher,
      source,
      diskFileInfo,
      StorageInfo.Type.HDD,
      writerContext,
      storageManager)
  }

  test("test memory tier evict to local tier writer") {
    val memoryTierWriter: MemoryTierWriter = prepareMemoryWriter
    val localTierWriter = prepareLocalTierWriter(true)

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(buf1)
    assert(memoryTierWriter.fileInfo.getFileLength === 1024)

    val needEvict = memoryTierWriter.needEvict()
    assert(needEvict === false)

    for (i <- 2 to 80) {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      memoryTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
      assert(memoryTierWriter.fileInfo.getFileLength === 1024 * i)
    }

    // 8 MB is lesser than the evict threshold
    assert(memoryTierWriter.needEvict() === false)
    memoryTierWriter.numPendingWrites.incrementAndGet()
    memoryTierWriter.write(WriterUtils.generateSparkFormatData(
      UnpooledByteBufAllocator.DEFAULT,
      0))

    assert(memoryTierWriter.needEvict() === true)
    memoryTierWriter.evict(localTierWriter)

    localTierWriter.close()
    assert(localTierWriter.fileInfo.getFileLength === 82944)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 82944)
  }

  test("test local tier writer with range filter on ") {
    val localTierWriter = prepareLocalTierWriter(true)
    for (i <- 1 to 10) {
      localTierWriter.numPendingWrites.incrementAndGet()
      localTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
    }

    localTierWriter.close()

    assert(10240 === localTierWriter.fileInfo.getFileLength)
    assert(
      localTierWriter.metaHandler.asInstanceOf[ReducePartitionMetaHandler].mapIdBitMap.isDefined)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 10240)
  }

  test("test local tier writer with range filter off ") {
    val localTierWriter = prepareLocalTierWriter(false)
    for (i <- 1 to 10) {
      localTierWriter.numPendingWrites.incrementAndGet()
      localTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
    }

    localTierWriter.close()

    assert(10240 === localTierWriter.fileInfo.getFileLength)
    assert(
      !localTierWriter.metaHandler.asInstanceOf[ReducePartitionMetaHandler].mapIdBitMap.isDefined)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getChunkOffsets.size() == 2)
    assert(
      localTierWriter.fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == 10240)

  }

  test("test local tier writer with fsync enabled") {
    val conf = new CelebornConf()
    conf.set("celeborn.worker.commitFiles.fsync", "true")
    val localTierWriter = prepareLocalTierWriter(false, conf)

    assert(localTierWriter.commitFilesFsync === true)
    for (i <- 1 to 10) {
      localTierWriter.numPendingWrites.incrementAndGet()
      localTierWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
    }

    val fileLen = localTierWriter.close()
    assert(fileLen == 10240)
    assert(localTierWriter.closed === true)
  }

  test("memory tier writer should not account data when insertion fails before adding") {
    val memoryTierWriter = prepareMemoryWriter
    val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val fileLengthBefore = memoryTierWriter.fileInfo.getFileLength
    val failure = new OutOfMemoryError("insertion failed before adding component")
    val failingBuffer = new FailBeforeAddCompositeByteBuf(failure)
    val originalFlushBuffer = memoryTierWriter.flushBuffer
    memoryTierWriter.flushBuffer = failingBuffer
    originalFlushBuffer.release()
    var callerReferenceReleased = false

    try {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[OutOfMemoryError](memoryTierWriter.write(buf))

      assert(thrown eq failure)
      assert(failingBuffer.writerIndex() === 0)
      assert(failingBuffer.numComponents() === 0)
      assert(memoryManager.getMemoryFileStorageCounter === memoryCounterBefore)
      assert(memoryTierWriter.fileInfo.getFileLength === fileLengthBefore)
      assert(buf.refCnt() === refCntBeforeWrite)
      assert(buf.release())
      callerReferenceReleased = true
      assert(buf.refCnt() === 0)
    } finally {
      memoryTierWriter.destroy(new IOException("test cleanup"))
      restoreCounters(memoryCounterBefore, diskCounterBefore)
      if (!callerReferenceReleased && buf.refCnt() > 0) {
        buf.release(buf.refCnt())
      }
    }
  }

  test("local tier writer should not account data when insertion fails before adding") {
    val localTierWriter = prepareLocalTierWriter(false)
    val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val failure = new OutOfMemoryError("insertion failed before adding component")
    val failingBuffer = new FailBeforeAddCompositeByteBuf(failure)
    val originalFlushBuffer = localTierWriter.flushBuffer
    localTierWriter.flushBuffer = failingBuffer
    localTierWriter.getFlusher.returnBuffer(originalFlushBuffer, false)
    var callerReferenceReleased = false

    try {
      localTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[OutOfMemoryError](localTierWriter.write(buf))

      assert(thrown eq failure)
      assert(failingBuffer.writerIndex() === 0)
      assert(failingBuffer.numComponents() === 0)
      assert(memoryManager.getDiskBufferCounter.get() === diskCounterBefore)
      assert(buf.refCnt() === refCntBeforeWrite)
      assert(buf.release())
      callerReferenceReleased = true
      assert(buf.refCnt() === 0)
    } finally {
      if (localTierWriter.flushBuffer != null) {
        localTierWriter.flushBuffer.release()
        localTierWriter.flushBuffer = null
      }
      localTierWriter.numPendingWrites.set(0)
      localTierWriter.close()
      restoreCounters(memoryCounterBefore, diskCounterBefore)
      if (!callerReferenceReleased && buf.refCnt() > 0) {
        buf.release(buf.refCnt())
      }
    }
  }

  test("memory tier writer should preserve state on composite buffer capacity overflow") {
    val memoryTierWriter = prepareMemoryWriter
    val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val fileLengthBefore = memoryTierWriter.fileInfo.getFileLength
    val overflowBuffer = capacityOverflowFlushBuffer()
    val writerIndexBefore = overflowBuffer.writerIndex()
    val numComponentsBefore = overflowBuffer.numComponents()
    val originalFlushBuffer = memoryTierWriter.flushBuffer
    memoryTierWriter.flushBuffer = overflowBuffer
    originalFlushBuffer.release()
    var callerReferenceReleased = false

    try {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[IllegalArgumentException](memoryTierWriter.write(buf))

      assert(thrown.getMessage.contains("overflow"))
      assert(overflowBuffer.writerIndex() === writerIndexBefore)
      assert(overflowBuffer.numComponents() === numComponentsBefore)
      assert(memoryManager.getMemoryFileStorageCounter === memoryCounterBefore)
      assert(memoryTierWriter.fileInfo.getFileLength === fileLengthBefore)
      assert(buf.refCnt() === refCntBeforeWrite)
      assert(buf.release())
      callerReferenceReleased = true
      assert(buf.refCnt() === 0)
    } finally {
      memoryTierWriter.destroy(new IOException("test cleanup"))
      restoreCounters(memoryCounterBefore, diskCounterBefore)
      if (!callerReferenceReleased && buf.refCnt() > 0) {
        buf.release(buf.refCnt())
      }
    }
  }

  test("memory tier writer should account data added before consolidation OOM") {
    val memoryTierWriter = prepareMemoryWriter
    val memoryFileInfo = memoryTierWriter.fileInfo.asInstanceOf[MemoryFileInfo]
    val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    val numBytes = buf.readableBytes()
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val fileLengthBefore = memoryTierWriter.fileInfo.getFileLength
    val failure = new OutOfMemoryError("consolidation failed")
    val failingBuffer = consolidationOomFlushBuffer(failure)
    val numComponentsBefore = failingBuffer.numComponents()
    val originalFlushBuffer = memoryTierWriter.flushBuffer
    memoryTierWriter.flushBuffer = failingBuffer
    originalFlushBuffer.release()
    var memoryBufferReleased = false
    var callerReferenceReleased = false

    try {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[OutOfMemoryError](memoryTierWriter.write(buf))

      assert(thrown eq failure)
      assert(failingBuffer.writerIndex() === numBytes)
      assert(failingBuffer.readableBytes() === numBytes)
      assert(failingBuffer.numComponents() === numComponentsBefore + 1)
      assert(memoryManager.getMemoryFileStorageCounter === memoryCounterBefore + numBytes)
      assert(memoryTierWriter.fileInfo.getFileLength === fileLengthBefore + numBytes)
      assert(buf.refCnt() === refCntBeforeWrite + 1)

      memoryTierWriter.numPendingWrites.set(0)
      assert(memoryTierWriter.close() === fileLengthBefore + numBytes)
      val releasedBytes = memoryFileInfo.releaseMemoryBuffers()
      memoryBufferReleased = true
      memoryManager.releaseMemoryFileStorage(releasedBytes)
      assert(releasedBytes === numBytes)
      assert(memoryManager.getMemoryFileStorageCounter === memoryCounterBefore)
      assert(buf.refCnt() === refCntBeforeWrite)
      assert(buf.release())
      callerReferenceReleased = true
      assert(buf.refCnt() === 0)
    } finally {
      if (!memoryTierWriter.closed) {
        memoryTierWriter.destroy(new IOException("test cleanup"))
      } else if (!memoryBufferReleased && memoryFileInfo.getBuffer.refCnt() > 0) {
        val releasedBytes = memoryFileInfo.releaseMemoryBuffers()
        memoryManager.releaseMemoryFileStorage(releasedBytes)
      }
      restoreCounters(memoryCounterBefore, diskCounterBefore)
      if (!callerReferenceReleased && buf.refCnt() > 0) {
        buf.release(buf.refCnt())
      }
    }
  }

  test("local tier writer should account data added before consolidation OOM") {
    val localTierWriter = prepareLocalTierWriter(false)
    val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    val numBytes = buf.readableBytes()
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val failure = new OutOfMemoryError("consolidation failed")
    val failingBuffer = consolidationOomFlushBuffer(failure)
    val numComponentsBefore = failingBuffer.numComponents()
    val originalFlushBuffer = localTierWriter.flushBuffer
    localTierWriter.flushBuffer = failingBuffer
    localTierWriter.getFlusher.returnBuffer(originalFlushBuffer, false)
    var flushBufferReleased = false
    var callerReferenceReleased = false

    try {
      localTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[OutOfMemoryError](localTierWriter.write(buf))

      assert(thrown eq failure)
      assert(failingBuffer.writerIndex() === numBytes)
      assert(failingBuffer.readableBytes() === numBytes)
      assert(failingBuffer.numComponents() === numComponentsBefore + 1)
      assert(memoryManager.getDiskBufferCounter.get() === diskCounterBefore + numBytes)
      assert(buf.refCnt() === refCntBeforeWrite + 1)

      localTierWriter.returnBuffer(false)
      assert(memoryManager.getDiskBufferCounter.get() === diskCounterBefore)
      assert(failingBuffer.writerIndex() === 0)
      assert(failingBuffer.numComponents() === 0)
      assert(buf.refCnt() === refCntBeforeWrite)
      val returnedBuffer = localTierWriter.getFlusher.takeBuffer()
      assert(returnedBuffer eq failingBuffer)
      assert(returnedBuffer.release())
      flushBufferReleased = true

      localTierWriter.numPendingWrites.set(0)
      localTierWriter.close()
      assert(buf.release())
      callerReferenceReleased = true
      assert(buf.refCnt() === 0)
    } finally {
      if (!flushBufferReleased) {
        if (localTierWriter.flushBuffer != null) {
          localTierWriter.flushBuffer.release()
          localTierWriter.flushBuffer = null
        } else if (failingBuffer.refCnt() > 0) {
          val returnedBuffer = localTierWriter.getFlusher.takeBuffer()
          returnedBuffer.release()
        }
      }
      localTierWriter.numPendingWrites.set(0)
      if (!localTierWriter.closed) {
        localTierWriter.close()
      }
      restoreCounters(memoryCounterBefore, diskCounterBefore)
      if (!callerReferenceReleased && buf.refCnt() > 0) {
        buf.release(buf.refCnt())
      }
    }
  }
}
