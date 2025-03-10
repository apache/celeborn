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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util

import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.GeneratedMessageV3
import io.netty.buffer.ByteBuf
import org.apache.hadoop.fs.FileSystem
import org.roaringbitmap.RoaringBitmap
import org.slf4j.{Logger, LoggerFactory}

import org.apache.celeborn.common.meta.{DiskFileInfo, FileInfo, MapFileMeta, MemoryFileInfo, ReduceFileMeta}
import org.apache.celeborn.common.protocol.{PbPushDataHandShake, PbRegionFinish, PbRegionStart, PbSegmentStart}
import org.apache.celeborn.common.unsafe.Platform
import org.apache.celeborn.common.util.FileChannelUtils

/**
 * It's the specific logic for reduce partition writer, map partition writer and map segment partition writer
 */
trait PartitionMetaHandler {

  /**
   * For reduce partition meta handler, this method won't be invoked.
   * For map partition meta handler, this method accept 1,2,4 messages.
   * For map segment partition meta handler, this method accept 1,2,3,4 messages.
   * @param message This method accept protobuf messages only
   * There are only four message types can be accepted.
   * 1. PbPushDataHandShake
   * 2. PbRegionStart
   * 3. PbSegmentStart
   * 4. PbRegionFinish
   */
  def handleEvent(message: GeneratedMessageV3): Unit

  /**
   * For reduce partition meta handler, this method will add map id into roaringbitmap if rangeReadFilter is on
   * For map partition meta handler, this method will get partition id from bytebuf and update the length of this partition
   * For map segment partition meta handler, this method will check the segment contains this partition id
   * @param bytes The bytes that contains shuffle data
   */
  def beforeWrite(bytes: ByteBuf): Unit

  /**
   * For reduce partition meta handler, this method will do nothing
   * For map partition meta handler, this method will ensure that this region is not finished
   * For map segment partition meta handler, this method will update segment index
   * @param size processed shuffle data size
   */
  def afterWrite(size: Int): Unit

  /**
   * Update file meta about file written bytes changed
   * @param size the size of data that put into the data flusher
   */
  def afterFlush(size: Int): Unit

  /**
   * For reduce partition meta handler, this method will do nothing
   * For map partition meta handler, this method will clear its index buffer
   */
  def beforeDestroy(): Unit

  /**
   * For reduce partition meta handler, this method will update file meta's chunk offsets
   * For map partition meta handler, this method will flush index buffer to disk
   * For map segment partition meta handler, this method will flush index buffer to disk and clear
   * segment index
   */
  def afterClose(): Unit

  def getMapIdBitmap(): Option[RoaringBitmap]
}

class MapPartitionMetaHandler(
    diskFileInfo: DiskFileInfo,
    notifier: FlushNotifier) extends PartitionMetaHandler {
  lazy val hadoopFs: FileSystem = StorageManager.hadoopFs.get()
  val logger: Logger = LoggerFactory.getLogger(classOf[MapPartitionMetaHandler])
  val fileMeta: MapFileMeta = diskFileInfo.getFileMeta.asInstanceOf[MapFileMeta]
  var numSubpartitions = 0
  var currentDataRegionIndex = 0
  var isBroadcastRegion = false
  var numSubpartitionBytes: Array[Long] = _
  var indexBuffer: ByteBuffer = _
  var currentSubpartition = 0
  private var totalBytes = 0L
  private var regionStartingOffset = 0L
  var indexChannel: FileChannel =
    FileChannelUtils.createWritableFileChannel(diskFileInfo.getIndexPath)
  @volatile private var isRegionFinished = true

  override def handleEvent(message: GeneratedMessageV3): Unit = {
    // only accept protobuf messages
    message match {
      case pb: PbPushDataHandShake =>
        pushDataHandShake(pb.getNumPartitions, pb.getBufferSize)
      case pb: PbRegionStart =>
        regionStart(
          pb.getCurrentRegionIndex,
          pb.getIsBroadcast)
      case _: PbRegionFinish =>
        regionFinish()
      case _ =>
      // do not handle
    }

  }

  def pushDataHandShake(numSubpartitions: Int, bufferSize: Int): Unit = {
    logger.debug(
      s"FileWriter:${diskFileInfo.getFilePath} " +
        s"pushDataHandShake numReducePartitions:$numSubpartitions " +
        s"bufferSize:$bufferSize")
    this.numSubpartitions = numSubpartitions
    numSubpartitionBytes = new Array[Long](numSubpartitions)
    fileMeta.setBufferSize(bufferSize)
    fileMeta.setNumSubPartitions(numSubpartitions)
  }

  private def regionStart(currentDataRegionIndex: Int, isBroadcastRegion: Boolean): Unit = {
    logger.debug(
      s"FileWriter:${diskFileInfo.getFilePath} " +
        s"regionStart currentDataRegionIndex:$currentDataRegionIndex " +
        s"isBroadcastRegion:$isBroadcastRegion")
    this.currentSubpartition = 0
    this.currentDataRegionIndex = currentDataRegionIndex
    this.isBroadcastRegion = isBroadcastRegion
  }

  @throws[IOException]
  private def regionFinish(): Unit = {
    // TODO: When region is finished, flush the data to be ready for the reading, in scenarios that
    // the upstream task writes and the downstream task reads simultaneously, such as flink hybrid
    // shuffle
    logger.debug("FileWriter:{} regionFinish", diskFileInfo.getFilePath)
    if (regionStartingOffset == totalBytes) return
    var fileOffset = regionStartingOffset
    if (indexBuffer == null) indexBuffer = allocateIndexBuffer(numSubpartitions)
    // write the index information of the current data region
    for (partitionIndex <- 0 until numSubpartitions) {
      indexBuffer.putLong(fileOffset)
      if (!isBroadcastRegion) {
        logger.debug(
          s"flush index filename:${diskFileInfo.getFilePath} " +
            s"region:$currentDataRegionIndex " +
            s"partitionId:$partitionIndex " +
            s"flush index fileOffset:$fileOffset, " +
            s"size:${numSubpartitionBytes(partitionIndex)} ")
        indexBuffer.putLong(numSubpartitionBytes(partitionIndex))
        fileOffset += numSubpartitionBytes(partitionIndex)
      } else {
        logger.debug(
          s"flush index broadcast filename:${diskFileInfo.getFilePath} " +
            s"region:$currentDataRegionIndex " +
            s"partitionId:$partitionIndex " +
            s"fileOffset:$fileOffset, " +
            s"size:${numSubpartitionBytes(0)} ")
        indexBuffer.putLong(numSubpartitionBytes(0))
      }
    }
    if (!indexBuffer.hasRemaining) flushIndex()
    regionStartingOffset = totalBytes
    util.Arrays.fill(numSubpartitionBytes, 0)
    isRegionFinished = true
  }

  private def allocateIndexBuffer(numSubpartitions: Int): ByteBuffer = {
    // the returned buffer size is no smaller than 4096 bytes to improve disk IO performance
    val minBufferSize = 4096
    val indexRegionSize = numSubpartitions * (8 + 8)
    if (indexRegionSize >= minBufferSize) {
      val buffer = ByteBuffer.allocateDirect(indexRegionSize)
      buffer.order(ByteOrder.BIG_ENDIAN)
      return buffer
    }
    var numRegions = minBufferSize / indexRegionSize
    if (minBufferSize % indexRegionSize != 0) numRegions += 1
    val buffer = ByteBuffer.allocateDirect(numRegions * indexRegionSize)
    buffer.order(ByteOrder.BIG_ENDIAN)
    buffer
  }

  @SuppressWarnings(Array("ByteBufferBackingArray"))
  @throws[IOException]
  private def flushIndex(): Unit = {
    // TODO: force flush the index file channel in scenarios which the upstream task writes and
    // downstream task reads simultaneously, such as flink hybrid shuffle
    if (indexBuffer != null) {
      logger.debug("flushIndex start:{}", diskFileInfo.getIndexPath)
      val startTime = System.currentTimeMillis
      indexBuffer.flip
      notifier.checkException()
      try {
        if (indexBuffer.hasRemaining) {
          // map partition synchronously writes file index
          if (indexChannel != null) while (indexBuffer.hasRemaining) indexChannel.write(indexBuffer)
          else if (diskFileInfo.isDFS) {
            val dfsStream = hadoopFs.append(diskFileInfo.getDfsIndexPath)
            dfsStream.write(indexBuffer.array)
            dfsStream.close()
          }
        }
        indexBuffer.clear
      } finally logger.debug(
        s"flushIndex end:${diskFileInfo.getIndexPath}, " +
          s"cost:${System.currentTimeMillis - startTime}")
    }
  }

  @throws[InterruptedException]
  def checkPartitionRegionFinished(timeout: Long): Boolean = {
    val delta = 100
    var times = 0
    while (delta * times < timeout) {
      if (this.isRegionFinished) return true
      Thread.sleep(delta)
      times += 1
    }
    false
  }

  def getCurrentSubpartition: Int = currentSubpartition

  def setCurrentSubpartition(currentSubpartition: Int): Unit = {
    this.currentSubpartition = currentSubpartition
  }

  def getNumSubpartitionBytes: Array[Long] = numSubpartitionBytes

  def getTotalBytes: Long = totalBytes

  def setTotalBytes(totalBytes: Long): Unit = {
    this.totalBytes = totalBytes
  }

  def setRegionFinished(regionFinished: Boolean): Unit = {
    isRegionFinished = regionFinished
  }

  override def afterFlush(size: Int): Unit = {
    diskFileInfo.updateBytesFlushed(size)
  }

  override def afterClose(): Unit = {
    // TODO: force flush the index file channel in scenarios which the upstream task writes and
    // downstream task reads simultaneously, such as flink hybrid shuffle
    if (indexBuffer != null) {
      logger.debug(s"flushIndex start:${diskFileInfo.getIndexPath}")
      val startTime = System.currentTimeMillis
      indexBuffer.flip
      notifier.checkException()
      try {
        if (indexBuffer.hasRemaining) {
          // map partition synchronously writes file index
          if (indexChannel != null) while (indexBuffer.hasRemaining) indexChannel.write(indexBuffer)
          else if (diskFileInfo.isDFS) {
            val dfsStream = hadoopFs.append(diskFileInfo.getDfsIndexPath)
            dfsStream.write(indexBuffer.array)
            dfsStream.close()
          }
        }
        indexBuffer.clear
      } finally logger.debug(
        s"flushIndex end:${diskFileInfo.getIndexPath}, " +
          s"cost:${System.currentTimeMillis - startTime}")
    }
  }

  override def beforeWrite(bytes: ByteBuf): Unit = {
    bytes.markReaderIndex()
    val partitionId = bytes.readInt
    val attemptId = bytes.readInt
    val batchId = bytes.readInt
    val size = bytes.readInt
    bytes.resetReaderIndex()
    logger.debug(
      s"map partition filename:${diskFileInfo.getFilePath} " +
        s"write partition:$partitionId " +
        s"attemptId:$attemptId " +
        s"batchId:$batchId " +
        s"size:$size")

    if (partitionId < currentSubpartition) throw new IOException(
      s"Must writing data in reduce partition index order, " +
        s"but now partitionId is $partitionId " +
        s"and pre partitionId is $currentSubpartition")

    if (partitionId > currentSubpartition) currentSubpartition = partitionId
    val length = bytes.readableBytes
    totalBytes += length
    numSubpartitionBytes(partitionId) += length
  }

  override def afterWrite(size: Int): Unit = {
    isRegionFinished = false
  }

  override def beforeDestroy(): Unit = {
    try if (indexChannel != null) indexChannel.close()
    catch {
      case e: IOException =>
        logger.warn(
          s"Close channel failed for file ${diskFileInfo.getIndexPath} caused by {}.",
          e.getMessage)
    }
  }

  override def getMapIdBitmap(): Option[RoaringBitmap] = Option.empty
}

class ReducePartitionMetaHandler(val rangeReadFilter: Boolean, val fileInfo: FileInfo)
  extends PartitionMetaHandler {
  val logger: Logger = LoggerFactory.getLogger(classOf[MapPartitionMetaHandler])
  lazy val mapIdBitMap: Option[RoaringBitmap] =
    if (rangeReadFilter) Some(new RoaringBitmap()) else Option.empty

  override def afterFlush(size: Int): Unit = {
    fileInfo.updateBytesFlushed(size)
  }

  override def afterClose(): Unit = {
    // update offset if it is not matched
    if (!isChunkOffsetValid()) {
      fileInfo.getReduceFileMeta.updateChunkOffset(fileInfo.getFileLength, true)
    }
  }

  private def isChunkOffsetValid(): Boolean = {
    // Consider a scenario where some bytes have been flushed
    // but the chunk offset boundary has not yet been updated.
    // we should check if the chunk offset boundary equals
    // bytesFlush or not. For example:
    // The last record is a giant record and it has been flushed
    // but its size is smaller than the nextBoundary, then the
    // chunk offset will not be set after flushing. we should
    // set it during FileWriter close.
    fileInfo.getFileMeta.asInstanceOf[ReduceFileMeta].getLastChunkOffset == fileInfo.getFileLength
  }

  override def beforeWrite(bytes: ByteBuf): Unit = {
    if (rangeReadFilter) {
      val header = new Array[Byte](4)
      bytes.markReaderIndex
      bytes.readBytes(header)
      bytes.resetReaderIndex
      val mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET)
      mapIdBitMap.get.add(mapId)
    }
  }

  override def afterWrite(size: Int): Unit = {}

  def getMapIdBitmap(): Option[RoaringBitmap] = {
    mapIdBitMap
  }

  override def beforeDestroy(): Unit = {}

  override def handleEvent(message: GeneratedMessageV3): Unit = {
    // reduce partition have no message to handle
  }
}

class SegmentMapPartitionMetaHandler(diskFileInfo: DiskFileInfo, notifier: FlushNotifier)
  extends MapPartitionMetaHandler(diskFileInfo, notifier) {

  @VisibleForTesting
  private val subPartitionHasStartSegment: util.Map[Integer, Boolean] =
    new util.HashMap[Integer, Boolean]
  @VisibleForTesting
  private var subPartitionBufferIndex: Array[Int] = _
  private var dataHeaders: List[Int] = _

  override def handleEvent(message: GeneratedMessageV3): Unit = {
    super.handleEvent(message)
    message match {
      case pb: PbSegmentStart =>
        segmentStart(pb.getSubPartitionId, pb.getSegmentId)
      case _ =>
      // do not handle
    }
  }

  private def segmentStart(subPartitionId: Int, segmentId: Int): Unit = {
    fileMeta.addPartitionSegmentId(
      subPartitionId,
      segmentId)
    subPartitionHasStartSegment.put(subPartitionId, true)
  }

  override def pushDataHandShake(numSubpartitions: Int, bufferSize: Int): Unit = {
    super.pushDataHandShake(numSubpartitions, bufferSize)
    subPartitionBufferIndex = new Array[Int](numSubpartitions)
    util.Arrays.fill(subPartitionBufferIndex, 0)
    fileMeta.setIsWriterClosed(false)
    fileMeta.setSegmentGranularityVisible(true)
  }

  override def afterFlush(size: Int): Unit = {
    diskFileInfo.updateBytesFlushed(size)
  }

  override def afterClose(): Unit = {
    subPartitionHasStartSegment.clear()
    super.afterClose()
    logger.debug(s"Close $this for file ${diskFileInfo.getFile}")
    fileMeta.setIsWriterClosed(true)
  }

  override def beforeWrite(bytes: ByteBuf): Unit = {
    bytes.markReaderIndex
    val subPartitionId = bytes.readInt
    val attemptId = bytes.readInt
    val batchId = bytes.readInt
    val size = bytes.readInt
    dataHeaders = List(subPartitionId, attemptId, batchId, size)

    if (!subPartitionHasStartSegment.containsKey(subPartitionId))
      throw new IllegalStateException(String.format(
        s"This partition may not start a segment: subPartitionId:$subPartitionId attemptId:$attemptId batchId:$batchId size:$size"))
    val currentSubpartition = getCurrentSubpartition
    // the subPartitionId must be ordered in a region// the subPartitionId must be ordered in a region
    if (subPartitionId < currentSubpartition) throw new IOException(String.format(
      s"Must writing data in reduce partition index order, but now supPartitionId is $subPartitionId and the previous supPartitionId is $currentSubpartition, attemptId is $attemptId, batchId is $batchId, size is $size"))
    bytes.resetReaderIndex
    logger.debug(
      s"map partition filename:${diskFileInfo.getFilePath} write partition:$subPartitionId currentSubPartition:$currentSubpartition attemptId:$attemptId batchId:$batchId size:$size")
    if (subPartitionId > currentSubpartition) setCurrentSubpartition(subPartitionId)
    val length = bytes.readableBytes
    setTotalBytes(getTotalBytes + length)
    getNumSubpartitionBytes(subPartitionId) += length

  }

  override def afterWrite(size: Int): Unit = {
    super.afterWrite(size)
    setRegionFinished(false)
    val subPartitionId = dataHeaders(0)
    val attemptId = dataHeaders(1)
    if (subPartitionHasStartSegment.get(subPartitionId)) {
      fileMeta.addSegmentIdAndFirstBufferIndex(
        subPartitionId,
        subPartitionBufferIndex(subPartitionId),
        fileMeta.getPartitionWritingSegmentId(subPartitionId))
      logger.debug(
        s"Add a segment id, partitionId:$subPartitionId, " +
          s"bufferIndex:${subPartitionBufferIndex(subPartitionId)}, " +
          s"segmentId:${fileMeta.getPartitionWritingSegmentId(subPartitionId)}, " +
          s"filename:${diskFileInfo.getFilePath}, " +
          s"attemptId:$attemptId.")
      // After the first buffer index of the segment is added, the following buffers in the segment
      // should not be added anymore, so the subPartitionHasStartSegment is updated to false.
      subPartitionHasStartSegment.put(subPartitionId, false)
    }
    subPartitionBufferIndex(subPartitionId) += 1
  }

  override def beforeDestroy(): Unit = super.beforeDestroy()
}
