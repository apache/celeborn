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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;

/*
 * map partition file writer, it will create index for each partition
 */
public final class MapPartitionFileWriter extends FileWriter {
  private static final Logger logger = LoggerFactory.getLogger(MapPartitionFileWriter.class);

  private int numReducePartitions;
  private int currentDataRegionIndex;
  private boolean isBroadcastRegion;
  private long[] numReducePartitionBytes;
  private ByteBuffer indexBuffer;
  private int currentReducePartition;
  private long totalBytes;
  private long regionStartingOffset;
  private long numDataRegions;
  private FileChannel channelIndex;
  private FSDataOutputStream streamIndex;
  private CompositeByteBuf flushBufferIndex;

  public MapPartitionFileWriter(
      FileInfo fileInfo,
      Flusher flusher,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode,
      boolean rangeReadFilter)
      throws IOException {
    super(
        fileInfo,
        flusher,
        workerSource,
        conf,
        deviceMonitor,
        splitThreshold,
        splitMode,
        PartitionType.MAP,
        rangeReadFilter);
    if (!fileInfo.isHdfs()) {
      channelIndex = new FileOutputStream(fileInfo.getIndexPath()).getChannel();
    } else {
      streamIndex = StorageManager.hadoopFs().create(fileInfo.getHdfsIndexPath(), true);
    }
    takeBufferIndex();
  }

  private void takeBufferIndex() {
    // metrics start
    String metricsName = null;
    String fileAbsPath = null;
    if (source.metricsCollectCriticalEnabled()) {
      metricsName = WorkerSource.TakeBufferTimeIndex();
      fileAbsPath = fileInfo.getIndexPath();
      source.startTimer(metricsName, fileAbsPath);
    }

    // real action
    flushBufferIndex = flusher.takeBuffer();

    // metrics end
    if (source.metricsCollectCriticalEnabled()) {
      source.stopTimer(metricsName, fileAbsPath);
    }

    if (flushBufferIndex == null) {
      IOException e =
          new IOException(
              "Take buffer index encounter error from Flusher: " + flusher.bufferQueueInfo());
      notifier.setException(e);
    }
  }

  public void write(ByteBuf data) throws IOException {
    byte[] header = new byte[16];
    data.markReaderIndex();
    data.readBytes(header);
    data.resetReaderIndex();
    int partitionId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
    collectPartitionDataLength(partitionId, data);

    super.write(data);
  }

  private void collectPartitionDataLength(int partitionId, ByteBuf data) {
    if (numReducePartitionBytes == null) {
      numReducePartitionBytes = new long[numReducePartitions];
    }
    currentReducePartition = partitionId;
    long length = data.readableBytes();
    totalBytes += length;
    numReducePartitionBytes[partitionId] += length;
  }

  @Override
  public synchronized long close() throws IOException {
    return super.close(
        () -> {
          if (flushBufferIndex.readableBytes() > 0) {
            flushIndex();
          }
        },
        () -> {
          if (StorageManager.hdfsFs().exists(fileInfo.getHdfsPeerWriterSuccessPath())) {
            StorageManager.hdfsFs().delete(fileInfo.getHdfsPath(), false);
            deleted = true;
          } else {
            StorageManager.hdfsFs().create(fileInfo.getHdfsWriterSuccessPath()).close();
          }
        },
        () -> {
          returnBufferIndex();
          if (channelIndex != null) {
            channelIndex.close();
          }
          if (streamIndex != null) {
            streamIndex.close();
            if (StorageManager.hdfsFs()
                .exists(
                    new Path(
                        Utils.getWriteSuccessFilePath(
                            Utils.getPeerPath(fileInfo.getIndexPath()))))) {
              StorageManager.hdfsFs().delete(fileInfo.getHdfsIndexPath(), false);
              deleted = true;
            } else {
              StorageManager.hdfsFs()
                  .create(new Path(Utils.getWriteSuccessFilePath((fileInfo.getIndexPath()))))
                  .close();
            }
          }
        });
  }

  public synchronized void destroy(IOException ioException) {
    destroyIndex();
    super.destroy(ioException);
  }

  public void pushDataHandShake(int numReducePartitions, int bufferSize) {
    this.numReducePartitions = numReducePartitions;
    fileInfo.setBufferSize(bufferSize);
  }

  public void regionStart(int currentDataRegionIndex, boolean isBroadcastRegion) {
    this.currentDataRegionIndex = currentDataRegionIndex;
    this.isBroadcastRegion = isBroadcastRegion;
  }

  public void regionFinish() throws IOException {
    if (regionStartingOffset == totalBytes) {
      return;
    }

    long fileOffset = regionStartingOffset;
    if (indexBuffer == null) {
      indexBuffer = allocateIndexBuffer(numReducePartitions);
    }

    // write the index information of the current data region
    for (int partitionIndex = 0; partitionIndex < numReducePartitions; ++partitionIndex) {
      indexBuffer.putLong(fileOffset);
      if (!isBroadcastRegion) {
        indexBuffer.putLong(numReducePartitionBytes[partitionIndex]);
        fileOffset += numReducePartitionBytes[partitionIndex];
      } else {
        indexBuffer.putLong(numReducePartitionBytes[0]);
      }
    }

    if (!indexBuffer.hasRemaining()) {
      flushIndex();
      takeBufferIndex();
    }

    ++numDataRegions;
    regionStartingOffset = totalBytes;
    Arrays.fill(numReducePartitionBytes, 0);
  }

  private synchronized void returnBufferIndex() {
    if (flushBufferIndex != null) {
      flusher.returnBuffer(flushBufferIndex);
      flushBufferIndex = null;
    }
  }

  private synchronized void destroyIndex() {
    returnBufferIndex();
    try {
      if (channelIndex != null) {
        channelIndex.close();
      }
      if (streamIndex != null) {
        streamIndex.close();
      }
    } catch (IOException e) {
      logger.warn(
          "Close channel failed for file {} caused by {}.",
          fileInfo.getIndexPath(),
          e.getMessage());
    }
  }

  private void flushIndex() throws IOException {
    indexBuffer.flip();
    notifier.checkException();
    notifier.numPendingFlushes.incrementAndGet();
    if (indexBuffer.hasRemaining()) {
      FlushTask task = null;
      if (channelIndex != null) {
        Unpooled.wrappedBuffer(indexBuffer);
        task = new LocalFlushTask(flushBufferIndex, channelIndex, notifier);
      } else if (streamIndex != null) {
        task = new HdfsFlushTask(flushBufferIndex, streamIndex, notifier);
      }
      addTask(task);
      flushBufferIndex = null;
    }
    indexBuffer.clear();
  }

  private ByteBuffer allocateIndexBuffer(int numPartitions) {

    // the returned buffer size is no smaller than 4096 bytes to improve disk IO performance
    int minBufferSize = 4096;

    int indexRegionSize = numPartitions * (8 + 8);
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
}
