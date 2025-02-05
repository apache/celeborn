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

/*
 * Note: Once FlushNotifier.exception is set, the whole file is not available.
 *       That's fine some of the internal state(e.g. bytesFlushed) may be inaccurate.
 */
public class PartitionDataWriter implements DeviceObserver {
  private static final Logger logger = LoggerFactory.getLogger(PartitionDataWriter.class);

  protected TierWriterProxy tierWriterProxy;

  protected final DeviceMonitor deviceMonitor;
  private final long splitThreshold;
  private final PartitionSplitMode splitMode;
  protected final FlushNotifier notifier = new FlushNotifier();
  // It's only needed when graceful shutdown is enabled
  protected final StorageManager storageManager;
  private UserCongestionControlContext userCongestionControlContext = null;
  public final String writerString;

  public PartitionDataWriter(
      StorageManager storageManager,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext,
      PartitionType partitionType)
      throws IOException {
    this.storageManager = storageManager;
    this.splitThreshold = writerContext.getSplitThreshold();
    this.deviceMonitor = deviceMonitor;
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
    tierWriterProxy = new TierWriterProxy(writerContext, storageManager, conf, partitionType);
    tierWriterProxy.registerToDeviceMonitor();
  }

  public DiskFileInfo getDiskFileInfo() {
    // keep compatible with current logic
    FileInfo currentFileInfo = tierWriterProxy.getCurrentFileInfo();
    if (currentFileInfo instanceof DiskFileInfo) {
      return (DiskFileInfo) currentFileInfo;
    }
    return null;
  }

  public String getFilePath() {
    return getDiskFileInfo().getFilePath();
  }

  public void incrementPendingWrites() {
    tierWriterProxy.incrementPendingWriters();
  }

  public void decrementPendingWrites() {
    tierWriterProxy.decrementPendingWriters();
  }

  // this will only happen if a worker is under memory pressure
  @VisibleForTesting
  public void flush() throws IOException {
    tierWriterProxy.flush(false);
  }

  public boolean needHardSplitForMemoryShuffleStorage() {
    return tierWriterProxy.needHardSplitForMemoryFile();
  }

  /** assume data size is less than chunk capacity */
  public void write(ByteBuf data) throws IOException {
    tierWriterProxy.write(data);
    tierWriterProxy.decrementPendingWriters();
  }

  public RoaringBitmap getMapIdBitMap() {
    scala.Option<RoaringBitmap> bitmapOpt =
        tierWriterProxy.currentTierWriter().metaHandler().getMapIdBitmap();
    // to keep compatible with scala 2.11
    if (bitmapOpt.isDefined()) {
      return bitmapOpt.get();
    }
    return null;
  }

  public StorageInfo getStorageInfo() {
    return tierWriterProxy.getCurrentStorageInfo();
  }

  public synchronized long close() {
    return tierWriterProxy.close();
  }

  public boolean isClosed() {
    return tierWriterProxy.isClosed();
  }

  public void evict(boolean checkClose) throws IOException {
    // this lock is used to make sure that
    // memory manager won't evict with writer thread concurrently
    tierWriterProxy.evict(checkClose);
  }

  public synchronized void destroy(IOException ioException) {
    tierWriterProxy.destroy(ioException);
  }

  public FileInfo getCurrentFileInfo() {
    return tierWriterProxy.getCurrentFileInfo();
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
    return ((MemoryFileInfo) tierWriterProxy.getCurrentFileInfo());
  }

  public UserCongestionControlContext getUserCongestionControlContext() {
    return userCongestionControlContext;
  }

  public void handleEvents(GeneratedMessageV3 message) {
    tierWriterProxy.handleEvents(message);
  }

  public PartitionMetaHandler getMetaHandler() {
    return tierWriterProxy.getMetaHandler();
  }

  public Flusher getFlusher() {
    return tierWriterProxy.getFlusher();
  }

  public TierWriterProxy getTierWriterProxy() {
    return tierWriterProxy;
  }
}
