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
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.UnpooledByteBufAllocator
import org.mockito.Mockito
import org.mockito.MockitoSugar.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.exception.AlreadyClosedException
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.network.util.{NettyUtils, TransportConf}
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, StorageInfo}
import org.apache.celeborn.service.deploy.worker.WorkerSource
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager

class TierWriterSuite extends AnyFunSuite with BeforeAndAfterEach {

  private def prepareMemoryWriter = {
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
      false)

    val source = new WorkerSource(celebornConf)

    val storageManager: StorageManager = Mockito.mock(classOf[StorageManager])
    val transConf = new TransportConf("shuffle", new CelebornConf)
    val allocator = NettyUtils.getByteBufAllocator(transConf, source, false)
    when(storageManager.storageBufferAllocator).thenReturn(allocator)
    when(storageManager.localOrDfsStorageAvailable).thenReturn(true)

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

    val tierMemoryWriter: MemoryTierWriter = prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    tierMemoryWriter.numPendingWrites.incrementAndGet()
    tierMemoryWriter.write(buf1)
    assert(tierMemoryWriter.fileInfo.getFileLength === 1024)

    val needEvict = tierMemoryWriter.needEvict()
    assert(needEvict === false)

    for (i <- 2 to 80) {
      tierMemoryWriter.numPendingWrites.incrementAndGet()
      tierMemoryWriter.write(WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0))
      assert(tierMemoryWriter.fileInfo.getFileLength === 1024 * i)
    }

    // 8 MB is lesser than the evict threshold
    assert(tierMemoryWriter.needEvict() === false)
    tierMemoryWriter.numPendingWrites.incrementAndGet()
    tierMemoryWriter.write(WriterUtils.generateSparkFormatData(
      UnpooledByteBufAllocator.DEFAULT,
      0))

    assert(tierMemoryWriter.needEvict() === true)

    val filelen = tierMemoryWriter.close()
    assert(filelen === 81 * 1024)

    assert(tierMemoryWriter.closed === true)

    try {
      tierMemoryWriter.write((WriterUtils.generateSparkFormatData(
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

    val tierMemoryWriter
        : _root_.org.apache.celeborn.service.deploy.worker.storage.MemoryTierWriter =
      prepareMemoryWriter

    val buf1 = WriterUtils.generateSparkFormatData(UnpooledByteBufAllocator.DEFAULT, 0)
    tierMemoryWriter.numPendingWrites.incrementAndGet()
    tierMemoryWriter.write(buf1)
    assert(tierMemoryWriter.fileInfo.getFileLength === 1024)

    val needEvict = tierMemoryWriter.needEvict()
    assert(needEvict === false)

    tierMemoryWriter.destroy(new IOException("test"))
    assert(tierMemoryWriter.flushBuffer.refCnt() === 0)

    try {
      tierMemoryWriter.write((WriterUtils.generateSparkFormatData(
        UnpooledByteBufAllocator.DEFAULT,
        0)))
      // expect already closed exception here
      assert(false)
    } catch {
      case e: AlreadyClosedException =>
        assert(true)
    }

  }
}
