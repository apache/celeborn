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

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Files

import io.netty.buffer.{ByteBuf, UnpooledByteBufAllocator}

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.meta.{DiskFileInfo, MapFileMeta, ReduceFileMeta}
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.service.deploy.worker.storage.WriterUtils.{generateFlinkFormatData, generateSparkFormatData}

class PartitionMetaHandlerSuite extends CelebornFunSuite with MockitoHelper {

  test("test map partition meta handler") {
    val byteBufAllocator = UnpooledByteBufAllocator.DEFAULT

    val tmpFilePath = Files.createTempFile("abc", "def")
    val fileMeta = new MapFileMeta()
    val notifier = new FlushNotifier()
    val diskFileInfo = new DiskFileInfo(
      UserIdentifier.apply("`a`.`b`"),
      fileMeta,
      tmpFilePath.toString,
      StorageInfo.Type.HDD)

    val mapMetaHandler = new MapPartitionMetaHandler(diskFileInfo, notifier)
    val pbPushDataHandShake =
      PbPushDataHandShake.newBuilder().setNumPartitions(10).setBufferSize(1024).build()
    mapMetaHandler.handleEvent(pbPushDataHandShake)

    assert(10 === mapMetaHandler.numSubpartitions)
    assert(10 === mapMetaHandler.numSubpartitionBytes.length)

    val pbRegionStart = PbRegionStart.newBuilder()
      .setCurrentRegionIndex(0)
      .setIsBroadcast(true)
      .build()

    mapMetaHandler.handleEvent(pbRegionStart)

    assert(mapMetaHandler.currentSubpartition === 0)
    assert(mapMetaHandler.isBroadcastRegion === true)
    assert(mapMetaHandler.currentDataRegionIndex === 0)

    for (i <- 0 until 10) {
      val dataBuf: ByteBuf = generateFlinkFormatData(byteBufAllocator, i)
      mapMetaHandler.beforeWrite(dataBuf)
    }

    val pbRegionFinish = PbRegionFinish.newBuilder()
      .build()

    mapMetaHandler.handleEvent(pbRegionFinish)

    assert(mapMetaHandler.indexBuffer.position() === 160)
    val start = mapMetaHandler.indexBuffer.getLong(0)
    val len = mapMetaHandler.indexBuffer.getLong(8)
    assert(0 === start)
    assert(1024 === len)

    assert(mapMetaHandler.indexChannel !== null)
    mapMetaHandler.afterClose()

    val file = new File(diskFileInfo.getIndexPath)
    assert(file.length() == 160)
    val inputStream = new FileInputStream(file)
    val array = new Array[Byte](160)
    inputStream.read(array)

    val readIndex = ByteBuffer.wrap(array)
    val start2 = readIndex.getLong()
    val len2 = readIndex.getLong()

    assert(0 === start2)
    assert(1024 === len2)

  }

  test("test reduce partition meta handler") {
    val byteBufAllocator = UnpooledByteBufAllocator.DEFAULT

    val tmpFilePath = Files.createTempFile("abc", "def")
    val fileMeta = new ReduceFileMeta(8192)
    val diskFileInfo = new DiskFileInfo(
      UserIdentifier.apply("`a`.`b`"),
      fileMeta,
      tmpFilePath.toString,
      StorageInfo.Type.HDD)

    val handler1 = new ReducePartitionMetaHandler(true, diskFileInfo)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 0))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 1))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 2))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 3))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 4))
    handler1.afterFlush(1024)

    assert(handler1.mapIdBitMap.get.getCardinality === 5)

    assert(diskFileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getNumChunks === 0)

    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 5))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 6))
    handler1.afterFlush(1024)
    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 7))
    handler1.afterFlush(1024)

    assert(diskFileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getNumChunks === 1)

    handler1.beforeWrite(generateSparkFormatData(byteBufAllocator, 8))
    handler1.afterFlush(1024)
    handler1.afterClose()

    assert(diskFileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getNumChunks == 2)
  }

  test("test map segment partition meta handler") {
    val byteBufAllocator = UnpooledByteBufAllocator.DEFAULT

    val tmpFilePath = Files.createTempFile("abc", "def")
    val fileMeta = new MapFileMeta()
    val notifier = new FlushNotifier()
    val diskFileInfo = new DiskFileInfo(
      UserIdentifier.apply("`a`.`b`"),
      fileMeta,
      tmpFilePath.toString,
      StorageInfo.Type.HDD)

    val mapMetaHandler = new SegmentMapPartitionMetaHandler(diskFileInfo, notifier)
    val pbPushDataHandShake =
      PbPushDataHandShake.newBuilder().setNumPartitions(10).setBufferSize(1024).build()
    mapMetaHandler.handleEvent(pbPushDataHandShake)

    assert(10 === mapMetaHandler.numSubpartitions)
    assert(10 === mapMetaHandler.numSubpartitionBytes.length)

    val pbSegmentStart = PbSegmentStart.newBuilder()
      .setSubPartitionId(0)
      .setSegmentId(0)
      .build()

    mapMetaHandler.handleEvent(pbSegmentStart)

    val pbRegionStart = PbRegionStart.newBuilder()
      .setCurrentRegionIndex(0)
      .setIsBroadcast(true)
      .build()

    mapMetaHandler.handleEvent(pbRegionStart)

    assert(mapMetaHandler.currentSubpartition === 0)
    assert(mapMetaHandler.isBroadcastRegion === true)
    assert(mapMetaHandler.currentDataRegionIndex === 0)

    for (i <- 0 until 10) {
      val dataBuf: ByteBuf = generateFlinkFormatData(byteBufAllocator, 0)
      mapMetaHandler.beforeWrite(dataBuf)
      mapMetaHandler.afterWrite(1024)
    }

    assert(fileMeta.getSegmentIdByFirstBufferIndex(0, 0) === 0)

    val pbRegionFinish = PbRegionFinish.newBuilder()
      .build()

    mapMetaHandler.handleEvent(pbRegionFinish)

    assert(mapMetaHandler.indexBuffer.position() === 160)
    val start = mapMetaHandler.indexBuffer.getLong(0)
    val len = mapMetaHandler.indexBuffer.getLong(8)
    assert(0 === start)
    assert(10240 === len)

    assert(mapMetaHandler.indexChannel !== null)
    mapMetaHandler.afterClose()

    val file = new File(diskFileInfo.getIndexPath)
    assert(file.length() == 160)
    val inputStream = new FileInputStream(file)
    val array = new Array[Byte](160)
    inputStream.read(array)

    val readIndex = ByteBuffer.wrap(array)
    val start2 = readIndex.getLong()
    val len2 = readIndex.getLong()

    assert(0 === start2)
    assert(10240 === len2)

  }
}
