/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage.segment;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.service.deploy.worker.storage.DeviceMonitor;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataWriter;
import org.apache.celeborn.service.deploy.worker.storage.PartitionDataWriterContext;
import org.apache.celeborn.service.deploy.worker.storage.StorageManager;

public class SegmentMapPartitionFileWriter extends MapPartitionDataWriter {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionFileWriter.class);

  private final Map<Integer, Boolean> subPartitionHasStartSegment;

  private int[] subPartitionBufferIndex;

  public SegmentMapPartitionFileWriter(
      StorageManager storageManager,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext)
      throws IOException {
    super(storageManager, workerSource, conf, deviceMonitor, writerContext);
    this.subPartitionHasStartSegment = new HashMap<>();
  }

  @Override
  public void pushDataHandShake(int numSubpartitions, int bufferSize) {
    super.pushDataHandShake(numSubpartitions, bufferSize);
    subPartitionBufferIndex = new int[numSubpartitions];
    Arrays.fill(subPartitionBufferIndex, 0);
    getFileMeta().setHasWriteFinished(false);
    getFileMeta().setHasSegments(true);
  }

  @Override
  public void write(ByteBuf data) throws IOException {
    data.markReaderIndex();
    int subPartitionId = data.readInt();
    int attemptId = data.readInt();
    int batchId = data.readInt();
    int size = data.readInt();

    data.resetReaderIndex();
    logger.debug(
        "mappartition filename:{} write partition:{} currentSubPartition:{} attemptId:{} batchId:{} size:{}",
        diskFileInfo.getFilePath(),
        subPartitionId,
        currentSubpartition,
        attemptId,
        batchId,
        size);

    if (subPartitionId < currentSubpartition) {
      throw new IOException(
          "Must writing data in reduce partition index order, but now supPartitionId is "
              + subPartitionId
              + " and the previous supPartitionId is "
              + currentSubpartition);
    }

    if (subPartitionId > currentSubpartition) {
      currentSubpartition = subPartitionId;
    }
    long length = data.readableBytes();
    totalBytes += length;
    numSubpartitionBytes[subPartitionId] += length;
    if (flushBuffer == null) {
      takeBuffer();
    }
    writeDataToFile(data);
    isRegionFinished = false;

    MapFileMeta mapFileMeta = getFileMeta();
    if (!subPartitionHasStartSegment.containsKey(subPartitionId)) {
      throw new IllegalStateException("This partition may not start a segment: " + subPartitionId);
    }
    // Only when the sub partition has stated the segment, the buffer index(this is the first buffer
    // of this segment) will be added.
    if (subPartitionHasStartSegment.get(subPartitionId)) {
      mapFileMeta.addSegmentIdAndFirstBufferIndex(
          subPartitionId,
          subPartitionBufferIndex[subPartitionId],
          mapFileMeta.getPartitionWritingSegmentId(subPartitionId));
      logger.debug(
          "Add a segment id, partitionId:{}, bufferIndex:{} segmentId: {}, filename:{} attemptId:{}",
          subPartitionId,
          subPartitionBufferIndex[subPartitionId],
          mapFileMeta.getPartitionWritingSegmentId(subPartitionId),
          diskFileInfo.getFilePath(),
          attemptId);
      // After the first buffer index of the segment is added, the following buffers in the segment
      // should not be added anymore, so the subPartitionHasStartSegment is updated to false.
      subPartitionHasStartSegment.put(subPartitionId, false);
    }
    subPartitionBufferIndex[subPartitionId]++;
  }

  @Override
  public void regionFinish() throws IOException {
    logger.debug("FileWriter:{} regionFinish", diskFileInfo.getFilePath());
    if (regionStartingOffset == totalBytes) {
      return;
    }

    // When region is finished, flush the data to be ready for the reading
    super.flush(true, true, false);
    long fileOffset = regionStartingOffset;
    if (indexBuffer == null) {
      indexBuffer = allocateIndexBuffer(numSubpartitions);
    }

    // write the index information of the current data region
    for (int partitionIndex = 0; partitionIndex < numSubpartitions; ++partitionIndex) {
      indexBuffer.putLong(fileOffset);
      if (!isBroadcastRegion) {
        logger.debug(
            "flush index filename:{} region:{} partitionid:{} flush index fileOffset:{}, size:{} ",
            diskFileInfo.getFilePath(),
            currentDataRegionIndex,
            partitionIndex,
            fileOffset,
            numSubpartitionBytes[partitionIndex]);

        indexBuffer.putLong(numSubpartitionBytes[partitionIndex]);
        fileOffset += numSubpartitionBytes[partitionIndex];
      } else {
        logger.debug(
            "flush index broadcast filename:{} region:{} partitionid:{}  fileOffset:{}, size:{} ",
            diskFileInfo.getFilePath(),
            currentDataRegionIndex,
            partitionIndex,
            fileOffset,
            numSubpartitionBytes[0]);

        indexBuffer.putLong(numSubpartitionBytes[0]);
      }
    }

    if (!indexBuffer.hasRemaining()) {
      flushIndex();
    }

    regionStartingOffset = totalBytes;
    Arrays.fill(numSubpartitionBytes, 0);
    isRegionFinished = true;
  }

  @Override
  protected void flushIndex() throws IOException {
    boolean forceFlush = indexBuffer != null && indexBuffer.hasRemaining() && indexChannel != null;
    super.flushIndex();
    if (forceFlush) {
      indexChannel.force(true);
    }
  }

  @Override
  public synchronized long close() throws IOException {
    long fileLength = super.close();
    logger.debug("Close {} for stream id {}", this, getFile());
    getFileMeta().setHasWriteFinished(true);
    return fileLength;
  }

  @Override
  public String toString() {
    return String.format("SegmentMapPartitionFileWriter{filePath=%s}", diskFileInfo.getFilePath());
  }

  public void segmentStart(int partitionId, int segmentId) {
    getFileMeta().addPartitionSegmentId(partitionId, segmentId);
    subPartitionHasStartSegment.put(partitionId, true);
  }
}
