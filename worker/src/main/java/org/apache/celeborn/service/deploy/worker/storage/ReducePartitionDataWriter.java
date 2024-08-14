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
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;

/*
 * reduce partition file writer, it will create chunk index
 */
public final class ReducePartitionDataWriter extends PartitionDataWriter {
  private static final Logger logger = LoggerFactory.getLogger(ReducePartitionDataWriter.class);

  public ReducePartitionDataWriter(
      StorageManager storageManager,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext)
      throws IOException {
    super(storageManager, workerSource, conf, deviceMonitor, writerContext, true);
  }

  private void updateLastChunkOffset() {
    FileInfo fileInfo = getCurrentFileInfo();
    fileInfo.getReduceFileMeta().updateChunkOffset(fileInfo.getFileLength(), true);
  }

  @Override
  public void flush(boolean forceFlush, boolean finalFlush, boolean fromEvict) throws IOException {
    super.flush(forceFlush, finalFlush, fromEvict);
    getCurrentFileInfo()
        .getReduceFileMeta()
        .updateChunkOffset(getCurrentFileInfo().getFileLength(), finalFlush);
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
    if (diskFileInfo != null) {
      return (diskFileInfo.getReduceFileMeta()).getLastChunkOffset()
          == diskFileInfo.getFileLength();
    }
    if (memoryFileInfo != null) {
      return (memoryFileInfo.getReduceFileMeta()).getLastChunkOffset()
          == memoryFileInfo.getFileLength();
    }
    // this should not happen
    return false;
  }

  @Override
  public synchronized long close() throws IOException {
    long streamId =
        super.close(
            () -> {
              if (!isChunkOffsetValid()) {
                updateLastChunkOffset();
              }
            },
            () -> {
              if (diskFileInfo != null) {
                if (diskFileInfo.isDFS()) {
                  if (hadoopFs.exists(diskFileInfo.getDfsPeerWriterSuccessPath())) {
                    hadoopFs.delete(diskFileInfo.getDfsPath(), false);
                    deleted = true;
                  } else {
                    hadoopFs.create(diskFileInfo.getDfsWriterSuccessPath()).close();
                    FSDataOutputStream indexOutputStream =
                        hadoopFs.create(diskFileInfo.getDfsIndexPath());
                    indexOutputStream.writeInt(
                        (diskFileInfo.getReduceFileMeta()).getChunkOffsets().size());
                    for (Long offset : (diskFileInfo.getReduceFileMeta()).getChunkOffsets()) {
                      indexOutputStream.writeLong(offset);
                    }
                    indexOutputStream.close();
                  }
                }
              } else {
                synchronized (flushLock) {
                  // merge and free small components
                  flushBuffer.consolidate();
                  memoryFileInfo.setBuffer(flushBuffer);
                }
              }
            },
            () -> {});
    return streamId;
  }
}
