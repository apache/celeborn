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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.ReduceFileMeta;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;

/*
 * reduce partition file writer, it will create chunk index
 */
public final class ReducePartitionDataWriter extends PartitionDataWriter {
  private static final Logger logger = LoggerFactory.getLogger(ReducePartitionDataWriter.class);

  private long nextBoundary;
  private final long shuffleChunkSize;

  public ReducePartitionDataWriter(
      StorageManager storageManager,
      DiskFileInfo diskFileInfo,
      Flusher flusher,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      long splitThreshold,
      PartitionSplitMode splitMode,
      boolean rangeReadFilter,
      String shuffleKey)
      throws IOException {
    super(
        storageManager,
        diskFileInfo,
        flusher,
        workerSource,
        conf,
        deviceMonitor,
        splitThreshold,
        splitMode,
        PartitionType.REDUCE,
        rangeReadFilter,
        shuffleKey);
    this.shuffleChunkSize = conf.shuffleChunkSize();
    this.nextBoundary = this.shuffleChunkSize;
  }

  @Override
  protected void flush(boolean finalFlush) throws IOException {
    super.flush(finalFlush);
    maybeSetChunkOffsets(finalFlush);
  }

  private void maybeSetChunkOffsets(boolean forceSet) {
    long bytesFlushed = diskFileInfo.getFileLength();
    if (bytesFlushed >= nextBoundary || forceSet) {
      ((ReduceFileMeta) diskFileInfo.getFileMeta()).addChunkOffset(bytesFlushed);
      nextBoundary = bytesFlushed + shuffleChunkSize;
    }
  }

  private boolean isChunkOffsetValid() {
    // Consider a scenario where some bytes have been flushed
    // but the chunk offset boundary has not yet been updated.
    // we should check if the chunk offset boundary equals
    // bytesFlush or not. For example:
    // The last record is a giant record and it has been flushed
    // but its size is smaller than the nextBoundary, then the
    // chunk offset will not be set after flushing. we should
    // set it during FileWriter close.
    return ((ReduceFileMeta) diskFileInfo.getFileMeta()).getLastChunkOffset()
        == diskFileInfo.getFileLength();
  }

  @Override
  public synchronized long close() throws IOException {
    return super.close(
        () -> {
          if (!isChunkOffsetValid()) {
            maybeSetChunkOffsets(true);
          }
        },
        () -> {
          if (diskFileInfo.isHdfs()) {
            if (StorageManager.hadoopFs().exists(diskFileInfo.getHdfsPeerWriterSuccessPath())) {
              StorageManager.hadoopFs().delete(diskFileInfo.getHdfsPath(), false);
              deleted = true;
            } else {
              StorageManager.hadoopFs().create(diskFileInfo.getHdfsWriterSuccessPath()).close();
              FSDataOutputStream indexOutputStream =
                  StorageManager.hadoopFs().create(diskFileInfo.getHdfsIndexPath());
              indexOutputStream.writeInt(
                  ((ReduceFileMeta) diskFileInfo.getFileMeta()).getChunkOffsets().size());
              for (Long offset : ((ReduceFileMeta) diskFileInfo.getFileMeta()).getChunkOffsets()) {
                indexOutputStream.writeLong(offset);
              }
              indexOutputStream.close();
            }
          }
        },
        () -> {});
  }
}
