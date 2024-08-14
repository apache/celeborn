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

package org.apache.celeborn.service.deploy.worker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.common.util.Utils;

/*
 * map partition file writer, it will create index for each partition
 */
public class MapPartitionDataWriter extends PartitionDataWriter {
  private static final Logger logger = LoggerFactory.getLogger(MapPartitionDataWriter.class);

  protected int numSubpartitions;
  protected int currentDataRegionIndex;
  protected boolean isBroadcastRegion;
  protected long[] numSubpartitionBytes;
  protected ByteBuffer indexBuffer;
  protected int currentSubpartition;
  protected long totalBytes;
  protected long regionStartingOffset;
  protected FileChannel indexChannel;
  protected volatile boolean isRegionFinished = true;

  public MapPartitionDataWriter(
      StorageManager storageManager,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext)
      throws IOException {
    super(storageManager, workerSource, conf, deviceMonitor, writerContext, false);

    Preconditions.checkState(diskFileInfo != null);
    if (!diskFileInfo.isDFS()) {
      indexChannel = FileChannelUtils.createWritableFileChannel(diskFileInfo.getIndexPath());
    } else {
      StorageInfo.Type storageType =
          diskFileInfo.isS3() ? StorageInfo.Type.S3 : StorageInfo.Type.HDFS;
      this.hadoopFs = StorageManager.hadoopFs().get(storageType);
      try {
        hadoopFs.create(diskFileInfo.getDfsIndexPath(), true).close();
      } catch (IOException e) {
        try {
          // If create file failed, wait 10 ms and retry
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
        hadoopFs.create(diskFileInfo.getDfsIndexPath(), true).close();
      }
    }
  }

  @Override
  public void write(ByteBuf data) throws IOException {
    data.markReaderIndex();
    int partitionId = data.readInt();
    int attemptId = data.readInt();
    int batchId = data.readInt();
    int size = data.readInt();

    data.resetReaderIndex();
    logger.debug(
        "mappartition filename:{} write partition:{} attemptId:{} batchId:{} size:{}",
        diskFileInfo.getFilePath(),
        partitionId,
        attemptId,
        batchId,
        size);

    if (partitionId < currentSubpartition) {
      throw new IOException(
          "Must writing data in reduce partition index order, but now partitionId is "
              + partitionId
              + " and pre partitionId is "
              + currentSubpartition);
    }

    if (partitionId > currentSubpartition) {
      currentSubpartition = partitionId;
    }
    long length = data.readableBytes();
    totalBytes += length;
    numSubpartitionBytes[partitionId] += length;
    writeDataToFile(data);
    isRegionFinished = false;
  }

  @Override
  public void setHasWriteFinished() {
    getFileMeta().setHasWriteFinished(true);
  }

  @Override
  public synchronized long close() throws IOException {
    return super.close(
        this::flushIndex,
        () -> {
          if (diskFileInfo.isDFS()) {
            if (hadoopFs.exists(diskFileInfo.getDfsPeerWriterSuccessPath())) {
              hadoopFs.delete(diskFileInfo.getDfsPath(), false);
              deleted = true;
            } else {
              hadoopFs.create(diskFileInfo.getDfsWriterSuccessPath()).close();
            }
          }
        },
        () -> {
          if (indexChannel != null) {
            indexChannel.close();
          }
          if (diskFileInfo.isDFS()) {
            if (hadoopFs.exists(
                new Path(
                    Utils.getWriteSuccessFilePath(
                        Utils.getPeerPath(diskFileInfo.getIndexPath()))))) {
              hadoopFs.delete(diskFileInfo.getDfsIndexPath(), false);
              deleted = true;
            } else {
              hadoopFs
                  .create(new Path(Utils.getWriteSuccessFilePath(diskFileInfo.getIndexPath())))
                  .close();
            }
          }
        });
  }

  @Override
  public synchronized void destroy(IOException ioException) {
    destroyIndex();
    super.destroy(ioException);
  }

  public void pushDataHandShake(int numSubpartitions, int bufferSize) {
    logger.debug(
        "FileWriter:{} pushDataHandShake numReducePartitions:{} bufferSize:{}",
        diskFileInfo.getFilePath(),
        numSubpartitions,
        bufferSize);

    this.numSubpartitions = numSubpartitions;
    numSubpartitionBytes = new long[numSubpartitions];
    MapFileMeta mapFileMeta = (MapFileMeta) diskFileInfo.getFileMeta();
    mapFileMeta.setBufferSize(bufferSize);
    mapFileMeta.setNumSubPartitions(numSubpartitions);
  }

  public void regionStart(int currentDataRegionIndex, boolean isBroadcastRegion)
      throws IOException {
    logger.debug(
        "FileWriter:{} regionStart currentDataRegionIndex:{} isBroadcastRegion:{}",
        diskFileInfo.getFilePath(),
        currentDataRegionIndex,
        isBroadcastRegion);

    this.currentSubpartition = 0;
    this.currentDataRegionIndex = currentDataRegionIndex;
    this.isBroadcastRegion = isBroadcastRegion;
  }

  public void regionFinish() throws IOException {
    logger.debug("FileWriter:{} regionFinish", diskFileInfo.getFilePath());
    if (regionStartingOffset == totalBytes) {
      return;
    }

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

  protected void writeDataToFile(ByteBuf data) throws IOException {
    super.write(data);
  }

  private synchronized void destroyIndex() {
    try {
      if (indexChannel != null) {
        indexChannel.close();
      }
    } catch (IOException e) {
      logger.warn(
          "Close channel failed for file {} caused by {}.",
          diskFileInfo.getIndexPath(),
          e.getMessage());
    }
  }

  @SuppressWarnings("ByteBufferBackingArray")
  protected void flushIndex() throws IOException {
    if (indexBuffer != null) {
      logger.debug("flushIndex start:{}", diskFileInfo.getIndexPath());
      long startTime = System.currentTimeMillis();
      indexBuffer.flip();
      notifier.checkException();
      try {
        if (indexBuffer.hasRemaining()) {
          // mappartition synchronously writes file index
          if (indexChannel != null) {
            while (indexBuffer.hasRemaining()) {
              indexChannel.write(indexBuffer);
            }
          } else if (diskFileInfo.isDFS()) {
            FSDataOutputStream dfsStream = hadoopFs.append(diskFileInfo.getDfsIndexPath());
            dfsStream.write(indexBuffer.array());
            dfsStream.close();
          }
        }
        indexBuffer.clear();
      } finally {
        logger.debug(
            "flushIndex end:{}, cost:{}",
            diskFileInfo.getIndexPath(),
            System.currentTimeMillis() - startTime);
      }
    }
  }

  protected MapFileMeta getFileMeta() {
    return (MapFileMeta) diskFileInfo.getFileMeta();
  }

  protected ByteBuffer allocateIndexBuffer(int numSubpartitions) {

    // the returned buffer size is no smaller than 4096 bytes to improve disk IO performance
    int minBufferSize = 4096;

    int indexRegionSize = numSubpartitions * (8 + 8);
    if (indexRegionSize >= minBufferSize) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(indexRegionSize);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    }

    int numRegions = minBufferSize / indexRegionSize;
    if (minBufferSize % indexRegionSize != 0) {
      ++numRegions;
    }
    ByteBuffer buffer = ByteBuffer.allocateDirect(numRegions * indexRegionSize);
    buffer.order(ByteOrder.BIG_ENDIAN);

    return buffer;
  }

  public boolean isRegionFinished() {
    return isRegionFinished;
  }

  public boolean checkPartitionRegionFinished(long timeout) throws InterruptedException {
    long delta = 100;
    int times = 0;
    while (delta * times < timeout) {
      if (this.isRegionFinished) {
        return true;
      }
      Thread.sleep(delta);
      times += 1;
    }
    return false;
  }
}
