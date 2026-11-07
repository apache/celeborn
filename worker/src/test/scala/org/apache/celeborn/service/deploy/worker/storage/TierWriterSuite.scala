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

import java.io.{IOException, OutputStream}
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._

import io.netty.buffer.{AbstractByteBufAllocator, ByteBuf, CompositeByteBuf, DuplicatedByteBuf, UnpooledByteBufAllocator}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Progressable
import org.mockito.Mockito
import org.mockito.MockitoSugar.when
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, DiskStatus, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.common.util.Utils
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterSuite extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {
  private class LargeReadableByteBuf(buffer: ByteBuf, virtualSize: Int)
    extends DuplicatedByteBuf(buffer) {
    override def capacity(): Int = virtualSize

    override def readableBytes(): Int = virtualSize
  }

  private class PreInsertionOomByteBuf(buffer: ByteBuf, failure: OutOfMemoryError)
    extends DuplicatedByteBuf(buffer) {
    override def readerIndex(): Int = throw failure
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
      memoryManager.releaseMemoryFileStorage(Math.toIntExact(memoryCounterDelta))
    } else if (memoryCounterDelta < 0) {
      memoryManager.incrementMemoryFileStorage(Math.toIntExact(-memoryCounterDelta))
    }
    val diskCounterDelta = memoryManager.getDiskBufferCounter.get() - diskCounterBefore
    if (diskCounterDelta > 0) {
      memoryManager.releaseDiskBuffer(Math.toIntExact(diskCounterDelta))
    } else if (diskCounterDelta < 0) {
      memoryManager.incrementDiskBuffer(Math.toIntExact(-diskCounterDelta))
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
      celebornConf.workerFlusherBufferSize,
      celebornConf)
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

  test("memory tier writer should preserve state on pre-insertion OOM") {
    val memoryTierWriter = prepareMemoryWriter
    val failure = new OutOfMemoryError("component creation failed")
    val buf = new PreInsertionOomByteBuf(
      WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0),
      failure)
    val refCntBeforeWrite = buf.refCnt()
    val memoryManager = MemoryManager.instance()
    val memoryCounterBefore = memoryManager.getMemoryFileStorageCounter
    val diskCounterBefore = memoryManager.getDiskBufferCounter.get()
    val fileLengthBefore = memoryTierWriter.fileInfo.getFileLength
    val writerIndexBefore = memoryTierWriter.flushBuffer.writerIndex()
    val numComponentsBefore = memoryTierWriter.flushBuffer.numComponents()
    var callerReferenceReleased = false

    try {
      memoryTierWriter.numPendingWrites.incrementAndGet()
      val thrown = intercept[OutOfMemoryError](memoryTierWriter.write(buf))

      assert(thrown eq failure)
      assert(memoryTierWriter.flushBuffer.writerIndex() === writerIndexBefore)
      assert(memoryTierWriter.flushBuffer.numComponents() === numComponentsBefore)
      assert(memoryManager.getMemoryFileStorageCounter === memoryCounterBefore)
      assert(memoryManager.getDiskBufferCounter.get() === diskCounterBefore)
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

  /** A DFS flusher the suite can stop; production flushers live as long as the worker. */
  private class StoppableDfsFlusher(conf: CelebornConf) extends Flusher(
      new WorkerSource(conf),
      1,
      NettyUtils.getByteBufAllocator(new TransportConf("test", conf), null, true),
      256,
      null,
      "HDFS",
      false,
      conf.workerHdfsFlusherBufferSize,
      conf) {
    override def processIOException(e: IOException, deviceErrorType: DiskStatus): Unit = {}

    override def getFlushTimeMetric(): String = WorkerSource.FLUSH_HDFS_DATA_TIME

    def stop(): Unit = {
      stopFlag.set(true)
      workers.foreach(_.shutdownNow())
      bufferQueue.asScala.foreach(_.release())
      bufferQueue.clear()
    }
  }

  private lazy val dfsFlusher = new StoppableDfsFlusher(new CelebornConf())

  override def afterAll(): Unit = {
    dfsFlusher.stop()
    super.afterAll()
  }

  /** Runs `body` on a DFS writer holding 1024 bytes; creating a file ending in `failOn` fails. */
  private def withDfsTierWriter(failOn: String = null, failAppendClose: Boolean = false)(
      body: (DfsTierWriter, DiskFileInfo, FileSystem) => Unit): Unit = {
    val conf = new CelebornConf()
      .set(CelebornConf.WORKER_WRITER_HDFS_CREATE_AUXILIARY_FILE_MAX_RETRIES.key, "0")
      .set(CelebornConf.WORKER_REUSE_HDFS_OUTPUT_STREAM_ENABLED.key, "false")
    val fs = new RawLocalFileSystem() {
      override def create(
          f: Path,
          overwrite: Boolean,
          bufferSize: Int,
          replication: Short,
          blockSize: Long,
          progress: Progressable): FSDataOutputStream = {
        if (failOn != null && f.getName.endsWith(failOn)) throw new IOException("injected")
        super.create(f, overwrite, bufferSize, replication, blockSize, progress)
      }

      // Drops the appended bytes and fails on close, like an append that never persisted.
      override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
        if (!failAppendClose) super.append(f, bufferSize, progress)
        else new FSDataOutputStream(
          new OutputStream {
            override def write(b: Int): Unit = {}
            override def close(): Unit = throw new IOException("append close failed")
          },
          null)
    }
    fs.initialize(URI.create("file:///"), new Configuration())
    val hadoopFsBefore = StorageManager.hadoopFs
    StorageManager.hadoopFs = Map[StorageInfo.Type, FileSystem](StorageInfo.Type.HDFS -> fs).asJava
    val dir = Files.createTempDirectory("celeborn-dfs")
    try {
      val userIdentifier = UserIdentifier("`aa`.`bb`")
      val fileInfo = new DiskFileInfo(
        userIdentifier,
        false,
        new ReduceFileMeta(conf.shuffleChunkSize),
        dir.resolve("1-0-0").toUri.toString,
        StorageInfo.Type.HDFS)
      val writerContext = new PartitionDataWriterContext(
        1024 * 1024 * 1024,
        PartitionSplitMode.SOFT,
        false,
        new PartitionLocation(1, 0, "host", 1111, 1112, 1113, 1114, PartitionLocation.Mode.PRIMARY),
        "app1-1",
        1,
        userIdentifier,
        PartitionType.REDUCE,
        false,
        false)
      val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])
      MemoryManager.initialize(conf, storageManager, null)
      val dfsTierWriter = new DfsTierWriter(
        conf,
        new ReducePartitionMetaHandler(conf.shuffleRangeReadFilterEnabled, fileInfo),
        new AtomicInteger(),
        new FlushNotifier(),
        dfsFlusher,
        new WorkerSource(conf),
        fileInfo,
        StorageInfo.Type.HDFS,
        writerContext,
        storageManager)
      val buf = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
      dfsTierWriter.numPendingWrites.incrementAndGet()
      dfsTierWriter.write(buf)
      buf.release()
      body(dfsTierWriter, fileInfo, fs)
    } finally {
      StorageManager.hadoopFs = hadoopFsBefore
      fs.close()
      FileUtils.deleteDirectory(dir.toFile)
    }
  }

  test("dfs tier writer keeps its copy when the peer success file exists") {
    withDfsTierWriter() { (dfsTierWriter, fileInfo, fs) =>
      val dataPath = fileInfo.getDfsPath.toString
      fs.create(new Path(dataPath.dropRight(1) + "1" + Utils.SUFFIX_HDFS_WRITE_SUCCESS)).close()

      assert(dfsTierWriter.close() === 1024)
      assert(fs.getFileStatus(fileInfo.getDfsPath).getLen === 1024)
      assert(fs.exists(fileInfo.getDfsIndexPath))
      assert(fs.exists(fileInfo.getDfsWriterSuccessPath))
    }
  }

  test("dfs tier writer fails close when its index cannot be written") {
    withDfsTierWriter(failOn = Utils.INDEX_SUFFIX) { (dfsTierWriter, fileInfo, fs) =>
      intercept[IOException](dfsTierWriter.close())
      assert(!fs.exists(fileInfo.getDfsWriterSuccessPath))
    }
  }

  test("dfs tier writer reports the flush failure when its index also fails") {
    withDfsTierWriter(failOn = Utils.INDEX_SUFFIX) { (dfsTierWriter, _, _) =>
      dfsTierWriter.notifier.setException(new IOException("flush failed"))
      assert(intercept[IOException](dfsTierWriter.close()).getMessage === "flush failed")
    }
  }

  test("dfs tier writer fails close when its append stream cannot be closed") {
    // Stream reuse is off, so each flush appends and closes its own stream.
    withDfsTierWriter(failAppendClose = true) { (dfsTierWriter, fileInfo, fs) =>
      assert(intercept[IOException](dfsTierWriter.close()).getMessage === "append close failed")
      assert(fs.getFileStatus(fileInfo.getDfsPath).getLen === 0)
    }
  }

  test("dfs tier writer still commits when only the success file cannot be written") {
    withDfsTierWriter(failOn = Utils.SUFFIX_HDFS_WRITE_SUCCESS) { (dfsTierWriter, fileInfo, fs) =>
      assert(dfsTierWriter.close() === 1024)
      assert(fs.exists(fileInfo.getDfsIndexPath))
    }
  }
}
