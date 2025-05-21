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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Option;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.*;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.PartitionType;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionController;
import org.apache.celeborn.service.deploy.worker.congestcontrol.UserCongestionControlContext;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

/*
 * Note: Once FlushNotifier.exception is set, the whole file is not available.
 *       That's fine some of the internal state(e.g. bytesFlushed) may be inaccurate.
 */
public class PartitionDataWriter implements DeviceObserver {
  private static final Logger logger = LoggerFactory.getLogger(PartitionDataWriter.class);

  private final long splitThreshold;
  private final PartitionSplitMode splitMode;
  private final long memoryFileStorageMaxFileSize;
  private final AtomicInteger numPendingWrites = new AtomicInteger(0);
  private final PartitionDataWriterContext writerContext;
  private final PartitionType partitionType;
  private final String writerString;
  private final StorageManager storageManager;
  private final FlushNotifier notifier = new FlushNotifier();

  private UserCongestionControlContext userCongestionControlContext = null;
  private volatile TierWriterBase currentTierWriter;

  public PartitionDataWriter(
      StorageManager storageManager,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext,
      PartitionType partitionType) {
    memoryFileStorageMaxFileSize = conf.workerMemoryFileStorageMaxFileSize();
    this.writerContext = writerContext;

    this.storageManager = storageManager;
    this.splitThreshold = writerContext.getSplitThreshold();
    this.splitMode = writerContext.getPartitionSplitMode();
    String shuffleKey = writerContext.getShuffleKey();
    String filename = writerContext.getPartitionLocation().getFileName();

    writerString = shuffleKey + "-" + filename + "-partition-writer";

    logger.debug("FileWriter {} split threshold {} mode {}", this, splitThreshold, splitMode);
    if (CongestionController.instance() != null) {
      userCongestionControlContext =
          CongestionController.instance()
              .getUserCongestionContext(writerContext.getUserIdentifier());
    }

    writerContext.setPartitionDataWriter(this);
    writerContext.setDeviceMonitor(deviceMonitor);
    this.partitionType = partitionType;
    currentTierWriter =
        storageManager
            .storagePolicy()
            .createFileWriter(writerContext, partitionType, numPendingWrites, notifier);
  }

  public DiskFileInfo getDiskFileInfo() {
    // keep compatible with current logic
    FileInfo currentFileInfo = currentTierWriter.fileInfo();
    if (currentFileInfo instanceof DiskFileInfo) {
      return (DiskFileInfo) currentFileInfo;
    }
    return null;
  }

  public String getFilePath() {
    return getDiskFileInfo().getFilePath();
  }

  public void incrementPendingWrites() {
    numPendingWrites.incrementAndGet();
  }

  public void decrementPendingWrites() {
    numPendingWrites.decrementAndGet();
  }

  // this will only happen if a worker is under memory pressure
  @VisibleForTesting
  public synchronized void flush() {
    currentTierWriter.flush(false, false);
  }

  public synchronized boolean needHardSplitForMemoryShuffleStorage() {
    if (!(currentTierWriter instanceof MemoryTierWriter)) {
      return false;
    }
    return !storageManager.localOrDfsStorageAvailable()
        && (currentTierWriter.fileInfo().getFileLength() > memoryFileStorageMaxFileSize
            || !MemoryManager.instance().memoryFileStorageAvailable());
  }

  public synchronized void write(ByteBuf data) throws IOException {
    if (currentTierWriter.needEvict()) {
      evict(false);
    }
    currentTierWriter.write(data);
  }

  public RoaringBitmap getMapIdBitMap() {
    Option<RoaringBitmap> bitmapOpt = currentTierWriter.metaHandler().getMapIdBitmap();
    // to keep compatible with scala 2.11
    if (bitmapOpt.isDefined()) {
      return bitmapOpt.get();
    }
    return null;
  }

  public StorageInfo getStorageInfo() {
    StorageInfo storageInfo = null;
    FileInfo fileInfo = currentTierWriter.fileInfo();
    if (fileInfo instanceof DiskFileInfo) {
      DiskFileInfo diskFileInfo = (DiskFileInfo) fileInfo;
      if (diskFileInfo.isDFS()) {
        if (((DfsTierWriter) currentTierWriter).deleted()) {
          return null;
        } else if (diskFileInfo.isS3()) {
          storageInfo = new StorageInfo(StorageInfo.Type.S3, true, diskFileInfo.getFilePath());
        } else if (diskFileInfo.isOSS()) {
          storageInfo = new StorageInfo(StorageInfo.Type.OSS, true, diskFileInfo.getFilePath());
        } else {
          storageInfo = new StorageInfo(StorageInfo.Type.HDFS, true, diskFileInfo.getFilePath());
        }
      } else {
        LocalFlusher flusher = (LocalFlusher) currentTierWriter.getFlusher();
        storageInfo = new StorageInfo(flusher.diskType(), true, "");
      }
    } else if (fileInfo instanceof MemoryFileInfo) {
      storageInfo = new StorageInfo(StorageInfo.Type.MEMORY, true, "");
    }
    if (storageInfo != null
        && currentTierWriter.fileInfo().getFileMeta() instanceof ReduceFileMeta) {
      storageInfo.setFileSize(currentTierWriter.fileInfo().getFileLength());
      storageInfo.setChunkOffsets(
          ((ReduceFileMeta) currentTierWriter.fileInfo().getFileMeta()).getChunkOffsets());
    }
    return storageInfo;
  }

  public boolean isClosed() {
    return currentTierWriter.closed();
  }

  // evict and flush method need to be in a same synchronized block
  // because memory manager may want to evict a file under memory pressure
  public synchronized void evict(boolean checkClose) {
    // close and evict might be invoked concurrently
    // do not evict committed files from memory manager
    // evict memory file info if worker is shutdown gracefully
    if (checkClose) {
      if (currentTierWriter.closed()) {
        return;
      }
    }
    TierWriterBase newTierWriter =
        storageManager
            .storagePolicy()
            .getEvictedFileWriter(
                currentTierWriter, writerContext, partitionType, numPendingWrites, notifier);
    currentTierWriter.evict(newTierWriter);
    currentTierWriter = newTierWriter;
  }

  public synchronized void destroy(IOException ioException) {
    currentTierWriter.destroy(ioException);
  }

  public synchronized long close() {
    return currentTierWriter.close();
  }

  public FileInfo getCurrentFileInfo() {
    return currentTierWriter.fileInfo();
  }

  public IOException getException() {
    if (notifier.hasException()) {
      return notifier.exception.get();
    } else {
      return null;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    PartitionDataWriter that = (PartitionDataWriter) o;
    return Objects.equals(writerString, that.writerString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(writerString);
  }

  @Override
  public String toString() {
    return writerString;
  }

  public void flushOnMemoryPressure() throws IOException {
    // this won't happen if this writer is in memory
    flush();
  }

  public long getSplitThreshold() {
    return splitThreshold;
  }

  public PartitionSplitMode getSplitMode() {
    return splitMode;
  }

  @Override
  public void notifyError(String mountPoint, DiskStatus diskStatus) {
    destroy(
        new IOException(
            "Destroy FileWriter "
                + this
                + " by device ERROR."
                + " Disk: "
                + mountPoint
                + " Status: "
                + diskStatus));
  }

  // These empty methods are intended to match scala 2.11 restrictions that
  // trait can not be used as an interface with default implementation.
  @Override
  public void notifyHealthy(String mountPoint) {}

  @Override
  public void notifyHighDiskUsage(String mountPoint) {}

  @Override
  public void notifyNonCriticalError(String mountPoint, DiskStatus diskStatus) {}

  public MemoryFileInfo getMemoryFileInfo() {
    return ((MemoryFileInfo) currentTierWriter.fileInfo());
  }

  public UserCongestionControlContext getUserCongestionControlContext() {
    return userCongestionControlContext;
  }

  public void handleEvents(GeneratedMessageV3 message) {
    currentTierWriter.metaHandler().handleEvent(message);
  }

  public PartitionMetaHandler getMetaHandler() {
    return currentTierWriter.metaHandler();
  }

  public Flusher getFlusher() {
    return currentTierWriter.getFlusher();
  }
}
