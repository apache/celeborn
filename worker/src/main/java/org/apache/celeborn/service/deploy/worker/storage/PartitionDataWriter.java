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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.MemoryFileInfo;
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
  protected boolean deleted = false;
  protected final FlushNotifier notifier = new FlushNotifier();
  // It's only needed when graceful shutdown is enabled
  protected final StorageManager storageManager;
  private UserCongestionControlContext userCongestionControlContext = null;
  public static String writerString;

  public PartitionDataWriter(
      StorageManager storageManager,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext,
      PartitionType partitionType)
      throws IOException {
    tierWriterProxy = new TierWriterProxy(writerContext, storageManager, conf, partitionType);
    writerContext.setPartitionDataWriter(this);

    this.storageManager = storageManager;
    this.splitThreshold = writerContext.getSplitThreshold();
    this.deviceMonitor = deviceMonitor;
    this.splitMode = writerContext.getPartitionSplitMode();
    String shuffleKey = writerContext.getShuffleKey();
    String filename = writerContext.getPartitionLocation().getFileName();

    writerString = shuffleKey + "-" + filename + " partition-writer";

    logger.debug("FileWriter {} split threshold {} mode {}", this, splitThreshold, splitMode);
    if (CongestionController.instance() != null) {
      userCongestionControlContext =
          CongestionController.instance()
              .getUserCongestionContext(writerContext.getUserIdentifier());
    }
  }

  public DiskFileInfo getDiskFileInfo() {
    return ((DiskFileInfo) tierWriterProxy.getCurrentFileInfo());
  }

  public String getFilePath() {
    return getDiskFileInfo().getFilePath();
  }

  public void incrementPendingWrites() {
    tierWriterProxy.incrementPendingWriters();
  }

  public void decrementPendingWrites() {
    tierWriterProxy.descrmentPendingWriters();
  }

  @VisibleForTesting
  public void flush(boolean finalFlush, boolean fromEvict) throws IOException {
    tierWriterProxy.flush(finalFlush);
  }

  public boolean needHardSplitForMemoryShuffleStorage() {
    return tierWriterProxy.needHardSplitForMemoryFile();
  }

  /** assume data size is less than chunk capacity */
  public void write(ByteBuf data) throws IOException {
    tierWriterProxy.write(data);
    tierWriterProxy.descrmentPendingWriters();
  }

  public RoaringBitmap getMapIdBitMap() {
    return ((ReducePartitionMetaHandler) tierWriterProxy.currentTierWriter().metaHandler())
        .getMapIdBitmap()
        .get();
  }

  public StorageInfo getStorageInfo() {
    return tierWriterProxy.getCurrentStorageInfo();
  }

  public long close() {
    return tierWriterProxy.close();
  }

  @FunctionalInterface
  public interface RunnableWithIOException {
    void run() throws IOException;
  }

  public boolean isClosed() {
    return tierWriterProxy.isClosed();
  }

  protected synchronized long close(
      RunnableWithIOException tryClose,
      RunnableWithIOException streamClose,
      RunnableWithIOException finalClose)
      throws IOException {
    return tierWriterProxy.close();
  }

  public void evict(boolean checkClose) throws IOException {
    // this lock is used to make sure that
    // memory manager won't evict with writer thread concurrently
    tierWriterProxy.evict(checkClose);
  }

  public synchronized void destroy(IOException ioException) {
    tierWriterProxy.destroy(ioException);
    // If this is local files, it needs to unregister from device monitor
    if (tierWriterProxy.isLocalFile()) {
      deviceMonitor.unregisterFileWriter(this);
    }
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
  public int hashCode() {
    return tierWriterProxy.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof PartitionDataWriter)
        && tierWriterProxy.equals(((PartitionDataWriter) obj).tierWriterProxy);
  }

  @Override
  public String toString() {
    return writerString;
  }

  public void flushOnMemoryPressure() throws IOException {
    // this won't happen if this writer is in memory
    flush(false, false);
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
    return tierWriterProxy.flusher();
  }

  public TierWriterProxy getTierWriterProxy() {
    return tierWriterProxy;
  }
}
