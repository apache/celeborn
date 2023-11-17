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

import io.netty.buffer.ByteBuf;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.NonMemoryFileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.common.util.Utils;

/*
 * map partition file writer, it will create index for each partition
 */
public final class MapPartitionPartitionDataWriter extends PartitionDataWriter {
  private static final Logger logger =
      LoggerFactory.getLogger(MapPartitionPartitionDataWriter.class);

  private int numSubpartitions;
  private int currentDataRegionIndex;
  private boolean isBroadcastRegion;
  private long[] numSubpartitionBytes;
  private ByteBuffer indexBuffer;
  private int currentSubpartition;
  private long totalBytes;
  private long regionStartingOffset;
  private FileChannel indexChannel;
  private volatile boolean isRegionFinished = true;

  public MapPartitionPartitionDataWriter(
      StorageManager storageManager,
      CreateFileContext createFileContext,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode,
      boolean rangeReadFilter)
      throws IOException {
    super(
        storageManager,
        createFileContext,
        workerSource,
        conf,
        deviceMonitor,
        splitThreshold,
        splitMode,
        PartitionType.MAP,
        rangeReadFilter);
    if (!((NonMemoryFileInfo) fileInfo).isHdfs()) {
      indexChannel =
          FileChannelUtils.createWritableFileChannel(((NonMemoryFileInfo) fileInfo).getIndexPath());
    } else {
      try {
        StorageManager.hadoopFs()
            .create(((NonMemoryFileInfo) fileInfo).getHdfsIndexPath(), true)
            .close();
      } catch (IOException e) {
        try {
          // If create file failed, wait 10 ms and retry
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
        StorageManager.hadoopFs()
            .create(((NonMemoryFileInfo) fileInfo).getHdfsIndexPath(), true)
            .close();
      }
    }
  }

  public void write(ByteBuf data) throws IOException {
    data.markReaderIndex();
    int partitionId = data.readInt();
    int attemptId = data.readInt();
    int batchId = data.readInt();
    int size = data.readInt();

    data.resetReaderIndex();
    logger.debug(
        "mappartition filename:{} write partition:{} attemptId:{} batchId:{} size:{}",
        ((NonMemoryFileInfo) fileInfo).getFilePath(),
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
    super.write(data);
    isRegionFinished = false;
  }

  @Override
  public synchronized long close() throws IOException {
    return super.close(
        () -> {
          flushIndex();
        },
        () -> {
          if (((NonMemoryFileInfo) fileInfo).isHdfs()) {
            if (StorageManager.hadoopFs()
                .exists(((NonMemoryFileInfo) fileInfo).getHdfsPeerWriterSuccessPath())) {
              StorageManager.hadoopFs().delete(((NonMemoryFileInfo) fileInfo).getHdfsPath(), false);
              deleted = true;
            } else {
              StorageManager.hadoopFs()
                  .create(((NonMemoryFileInfo) fileInfo).getHdfsWriterSuccessPath())
                  .close();
            }
          }
        },
        () -> {
          if (indexChannel != null) {
            indexChannel.close();
          }
          if (((NonMemoryFileInfo) fileInfo).isHdfs()) {
            if (StorageManager.hadoopFs()
                .exists(
                    new Path(
                        Utils.getWriteSuccessFilePath(
                            Utils.getPeerPath(((NonMemoryFileInfo) fileInfo).getIndexPath()))))) {
              StorageManager.hadoopFs()
                  .delete(((NonMemoryFileInfo) fileInfo).getHdfsIndexPath(), false);
              deleted = true;
            } else {
              StorageManager.hadoopFs()
                  .create(
                      new Path(
                          Utils.getWriteSuccessFilePath(
                              (((NonMemoryFileInfo) fileInfo).getIndexPath()))))
                  .close();
            }
          }
        });
  }

  public synchronized void destroy(IOException ioException) {
    destroyIndex();
    super.destroy(ioException);
  }

  public void pushDataHandShake(int numSubpartitions, int bufferSize) {
    logger.debug(
        "FileWriter:{} pushDataHandShake numReducePartitions:{} bufferSize:{}",
        ((NonMemoryFileInfo) fileInfo).getFilePath(),
        numSubpartitions,
        bufferSize);

    this.numSubpartitions = numSubpartitions;
    numSubpartitionBytes = new long[numSubpartitions];
    fileInfo.getFileMeta().setBufferSize(bufferSize);
    fileInfo.getFileMeta().setNumSubpartitions(numSubpartitions);
  }

  public void regionStart(int currentDataRegionIndex, boolean isBroadcastRegion) {
    logger.debug(
        "FileWriter:{} regionStart currentDataRegionIndex:{} isBroadcastRegion:{}",
        ((NonMemoryFileInfo) fileInfo).getFilePath(),
        currentDataRegionIndex,
        isBroadcastRegion);

    this.currentSubpartition = 0;
    this.currentDataRegionIndex = currentDataRegionIndex;
    this.isBroadcastRegion = isBroadcastRegion;
  }

  public void regionFinish() throws IOException {
    logger.debug("FileWriter:{} regionFinish", ((NonMemoryFileInfo) fileInfo).getFilePath());
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
            ((NonMemoryFileInfo) fileInfo).getFilePath(),
            currentDataRegionIndex,
            partitionIndex,
            fileOffset,
            numSubpartitionBytes[partitionIndex]);

        indexBuffer.putLong(numSubpartitionBytes[partitionIndex]);
        fileOffset += numSubpartitionBytes[partitionIndex];
      } else {
        logger.debug(
            "flush index broadcast filename:{} region:{} partitionid:{}  fileOffset:{}, size:{} ",
            ((NonMemoryFileInfo) fileInfo).getFilePath(),
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

  private synchronized void destroyIndex() {
    try {
      if (indexChannel != null) {
        indexChannel.close();
      }
    } catch (IOException e) {
      logger.warn(
          "Close channel failed for file {} caused by {}.",
          ((NonMemoryFileInfo) fileInfo).getIndexPath(),
          e.getMessage());
    }
  }

  private void flushIndex() throws IOException {
    if (indexBuffer != null) {
      logger.debug("flushIndex start:{}", ((NonMemoryFileInfo) fileInfo).getIndexPath());
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
          } else if (((NonMemoryFileInfo) fileInfo).isHdfs()) {
            FSDataOutputStream hdfsStream =
                StorageManager.hadoopFs().append(((NonMemoryFileInfo) fileInfo).getHdfsIndexPath());
            hdfsStream.write(indexBuffer.array());
            hdfsStream.close();
          }
        }
        indexBuffer.clear();
      } finally {
        logger.debug(
            "flushIndex end:{}, cost:{}",
            ((NonMemoryFileInfo) fileInfo).getIndexPath(),
            System.currentTimeMillis() - startTime);
      }
    }
  }

  private ByteBuffer allocateIndexBuffer(int numSubpartitions) {

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
}
