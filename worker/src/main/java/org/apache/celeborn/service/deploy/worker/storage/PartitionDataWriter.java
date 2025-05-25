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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Tuple4;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.AlreadyClosedException;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.DiskStatus;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.MemoryFileInfo;
import org.apache.celeborn.common.metrics.source.AbstractSource;
import org.apache.celeborn.common.protocol.PartitionSplitMode;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.unsafe.Platform;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.congestcontrol.BufferStatusHub;
import org.apache.celeborn.service.deploy.worker.congestcontrol.CongestionController;
import org.apache.celeborn.service.deploy.worker.congestcontrol.UserBufferInfo;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

/*
 * Note: Once FlushNotifier.exception is set, the whole file is not available.
 *       That's fine some of the internal state(e.g. bytesFlushed) may be inaccurate.
 */
public abstract class PartitionDataWriter implements DeviceObserver {
  private static final Logger logger = LoggerFactory.getLogger(PartitionDataWriter.class);
  private static final long WAIT_INTERVAL_MS = 5;

  // After commit file, there will be only 1 fileinfo left.
  protected DiskFileInfo diskFileInfo = null;
  protected MemoryFileInfo memoryFileInfo = null;
  protected FileChannel channel;
  private volatile boolean closed;
  private volatile boolean destroyed;

  protected final AtomicInteger numPendingWrites = new AtomicInteger();

  public Flusher flusher;
  private int flushWorkerIndex;

  protected CompositeByteBuf flushBuffer;

  protected final Object flushLock = new Object();
  private final long writerCloseTimeoutMs;

  protected long flusherBufferSize;

  protected final DeviceMonitor deviceMonitor;
  protected final AbstractSource source; // metrics

  private final long splitThreshold;
  private final PartitionSplitMode splitMode;
  private final boolean rangeReadFilter;
  protected boolean deleted = false;
  private RoaringBitmap mapIdBitMap = null;
  protected final FlushNotifier notifier = new FlushNotifier();
  // It's only needed when graceful shutdown is enabled
  private final String shuffleKey;
  protected final StorageManager storageManager;
  private final boolean workerGracefulShutdown;
  protected final long memoryFileStorageMaxFileSize;
  protected AtomicBoolean isMemoryShuffleFile = new AtomicBoolean();
  protected final String filename;
  protected PooledByteBufAllocator pooledByteBufAllocator;
  private final PartitionDataWriterContext writerContext;
  private final long localFlusherBufferSize;
  private final long hdfsFlusherBufferSize;
  private Exception exception = null;
  private boolean metricsCollectCriticalEnabled;
  private long chunkSize;

  private UserBufferInfo userBufferInfo = null;

  public PartitionDataWriter(
      StorageManager storageManager,
      AbstractSource workerSource,
      CelebornConf conf,
      DeviceMonitor deviceMonitor,
      PartitionDataWriterContext writerContext,
      boolean supportInMemory)
      throws IOException {
    this.storageManager = storageManager;
    this.writerCloseTimeoutMs = conf.workerWriterCloseTimeoutMs();
    this.workerGracefulShutdown = conf.workerGracefulShutdown();
    this.splitThreshold = writerContext.getSplitThreshold();
    this.deviceMonitor = deviceMonitor;
    this.splitMode = writerContext.getPartitionSplitMode();
    this.rangeReadFilter = writerContext.isRangeReadFilter();
    this.shuffleKey = writerContext.getShuffleKey();
    this.memoryFileStorageMaxFileSize = conf.workerMemoryFileStorageMaxFileSize();
    this.filename = writerContext.getPartitionLocation().getFileName();
    this.writerContext = writerContext;
    this.localFlusherBufferSize = conf.workerFlusherBufferSize();
    this.hdfsFlusherBufferSize = conf.workerHdfsFlusherBufferSize();
    this.metricsCollectCriticalEnabled = conf.metricsCollectCriticalEnabled();
    this.chunkSize = conf.shuffleChunkSize();

    Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File> createFileResult =
        storageManager.createFile(writerContext, supportInMemory);

    // Reduce partition data writers support memory storage now
    if (supportInMemory && createFileResult._1() != null) {
      this.memoryFileInfo = createFileResult._1();
      this.pooledByteBufAllocator = storageManager.storageBufferAllocator();
      this.isMemoryShuffleFile.set(true);
      storageManager.registerMemoryPartitionWriter(this, createFileResult._1());
    } else if (createFileResult._2() != null) {
      this.diskFileInfo = createFileResult._3();
      this.flusher = createFileResult._2();
      this.flushWorkerIndex = this.flusher.getWorkerIndex();
      File workingDir = createFileResult._4();
      this.isMemoryShuffleFile.set(false);
      initFileChannelsForDiskFile();
      storageManager.registerDiskFilePartitionWriter(this, workingDir, diskFileInfo);
    } else {
      throw new CelebornIOException(
          "Create file failed for location:" + writerContext.getPartitionLocation().toString());
    }

    source = workerSource;
    logger.debug("FileWriter {} split threshold {} mode {}", this, splitThreshold, splitMode);
    if (rangeReadFilter) {
      this.mapIdBitMap = new RoaringBitmap();
    }
    takeBuffer();
    CongestionController congestionController = CongestionController.instance();
    if (!isMemoryShuffleFile.get() && congestionController != null) {
      userBufferInfo = congestionController.getUserBuffer(getDiskFileInfo().getUserIdentifier());
    }
  }

  public void initFileChannelsForDiskFile() throws IOException {
    if (!this.diskFileInfo.isHdfs()) {
      this.flusherBufferSize = localFlusherBufferSize;
      channel = FileChannelUtils.createWritableFileChannel(this.diskFileInfo.getFilePath());
    } else {
      this.flusherBufferSize = hdfsFlusherBufferSize;
      // We open the stream and close immediately because HDFS output stream will
      // create a DataStreamer that is a thread.
      // If we reuse HDFS output stream, we will exhaust the memory soon.
      try {
        StorageManager.hadoopFs().create(this.diskFileInfo.getHdfsPath(), true).close();
      } catch (IOException e) {
        try {
          // If create file failed, wait 10 ms and retry
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
        StorageManager.hadoopFs().create(this.diskFileInfo.getHdfsPath(), true).close();
      }
    }
  }

  public DiskFileInfo getDiskFileInfo() {
    return diskFileInfo;
  }

  public File getFile() {
    return diskFileInfo.getFile();
  }

  public void incrementPendingWrites() {
    numPendingWrites.incrementAndGet();
  }

  public void decrementPendingWrites() {
    numPendingWrites.decrementAndGet();
  }

  @VisibleForTesting
  public void flush(boolean finalFlush, boolean fromEvict) throws IOException {
    // flushBuffer == null here means this writer is already closed
    if (flushBuffer != null) {
      int numBytes = flushBuffer.readableBytes();
      if (numBytes != 0) {
        notifier.checkException();
        FlushTask task = null;
        if (fromEvict) {
          notifier.numPendingFlushes.incrementAndGet();
          // duplicate buffer before its released
          ByteBuf dupBuf = flushBuffer.retainedDuplicate();
          // flush task will release the buffer of memory shuffle file
          if (channel != null) {
            task = new LocalFlushTask(flushBuffer, channel, notifier, false);
          } else if (diskFileInfo.isHdfs()) {
            task = new HdfsFlushTask(flushBuffer, diskFileInfo.getHdfsPath(), notifier, false);
          }
          MemoryManager.instance().releaseMemoryFileStorage(numBytes);
          MemoryManager.instance().incrementDiskBuffer(numBytes);
          // read flush buffer to generate correct chunk offsets
          // data header layout (mapId, attemptId, nextBatchId, length)
          if (numBytes > chunkSize) {
            ByteBuffer headerBuf = ByteBuffer.allocate(16);
            while (dupBuf.isReadable()) {
              headerBuf.rewind();
              dupBuf.readBytes(headerBuf);
              byte[] batchHeader = headerBuf.array();
              int compressedSize = Platform.getInt(batchHeader, Platform.BYTE_ARRAY_OFFSET + 12);
              dupBuf.skipBytes(compressedSize);
              diskFileInfo.updateBytesFlushed(compressedSize + 16);
            }
            dupBuf.release();
          } else {
            diskFileInfo.updateBytesFlushed(numBytes);
          }
        } else {
          if (!isMemoryShuffleFile.get()) {
            notifier.numPendingFlushes.incrementAndGet();
            if (channel != null) {
              task = new LocalFlushTask(flushBuffer, channel, notifier, true);
            } else if (diskFileInfo.isHdfs()) {
              task = new HdfsFlushTask(flushBuffer, diskFileInfo.getHdfsPath(), notifier, true);
            }
          }
        }
        // task won't be null in real workloads
        // task will be null in UT to check chunk size and offset
        if (task != null) {
          addTask(task);
          flushBuffer = null;
          if (!fromEvict) {
            diskFileInfo.updateBytesFlushed(numBytes);
          }
          if (!finalFlush) {
            takeBuffer();
          }
        }
      }
    }
  }

  public boolean needHardSplitForMemoryShuffleStorage() {
    if (!isMemoryShuffleFile.get()) {
      return false;
    } else {
      return !storageManager.localOrHdfsStorageAvailable()
          && (memoryFileInfo.getFileLength() > memoryFileStorageMaxFileSize
              || !MemoryManager.instance().memoryFileStorageAvailable());
    }
  }

  /** assume data size is less than chunk capacity */
  public void write(ByteBuf data) throws IOException {
    if (closed) {
      String msg = getFileAlreadyClosedMsg();
      logger.warn(msg);
      throw new AlreadyClosedException(msg);
    }

    if (notifier.hasException()) {
      return;
    }

    int mapId = 0;
    if (rangeReadFilter) {
      byte[] header = new byte[4];
      data.markReaderIndex();
      data.readBytes(header);
      data.resetReaderIndex();
      mapId = Platform.getInt(header, Platform.BYTE_ARRAY_OFFSET);
    }

    final int numBytes = data.readableBytes();

    synchronized (flushLock) {
      if (closed) {
        String msg = getFileAlreadyClosedMsg();
        logger.warn(msg);
        throw new AlreadyClosedException(msg);
      }
      if (rangeReadFilter) {
        mapIdBitMap.add(mapId);
      }
      int flushBufferReadableBytes = flushBuffer.readableBytes();
      if (!isMemoryShuffleFile.get()) {
        if (flushBufferReadableBytes != 0
            && flushBufferReadableBytes + numBytes >= flusherBufferSize) {
          flush(false, false);
        }
      } else {
        if (flushBufferReadableBytes > memoryFileStorageMaxFileSize
            && storageManager.localOrHdfsStorageAvailable()) {
          logger.debug(
              "{} Evict, memory buffer is  {}",
              writerContext.getPartitionLocation().getFileName(),
              flushBufferReadableBytes);
          evict(false);
        }
      }

      // update the disk buffer or memory file storage after evict
      if (isMemoryShuffleFile.get()) {
        MemoryManager.instance().incrementMemoryFileStorage(numBytes);
      } else {
        MemoryManager.instance().incrementDiskBuffer(numBytes);
        if (userBufferInfo != null) {
          userBufferInfo.updateInfo(
              System.currentTimeMillis(), new BufferStatusHub.BufferStatusNode(numBytes));
        }
      }

      data.retain();
      try {
        flushBuffer.addComponent(true, data);
      } catch (OutOfMemoryError oom) {
        data.release();
        if (isMemoryShuffleFile.get()) {
          MemoryManager.instance().releaseMemoryFileStorage(numBytes);
        } else {
          MemoryManager.instance().releaseDiskBuffer(numBytes);
        }
        throw oom;
      }
      if (isMemoryShuffleFile.get()) {
        memoryFileInfo.updateBytesFlushed(numBytes);
      }
    }

    numPendingWrites.decrementAndGet();
  }

  public void evictInternal() throws IOException {
    if (exception != null) {
      return;
    }
    Tuple4<MemoryFileInfo, Flusher, DiskFileInfo, File> createFileResult =
        storageManager.createFile(writerContext, false);
    if (createFileResult._4() != null) {
      this.diskFileInfo = createFileResult._3();
      this.flusher = createFileResult._2();
      this.flushWorkerIndex = this.flusher.getWorkerIndex();

      isMemoryShuffleFile.set(false);
      initFileChannelsForDiskFile();
      flush(closed, true);

      logger.debug("evict {} {}", shuffleKey, filename);
      storageManager.unregisterMemoryPartitionWriterAndFileInfo(
          memoryFileInfo, shuffleKey, filename);
      storageManager.evictedFileCount().incrementAndGet();
      memoryFileInfo = null;
    } else {
      exception = new CelebornIOException("PartitionDataWriter create disk-related file failed");
      throw (CelebornIOException) exception;
    }
  }

  public RoaringBitmap getMapIdBitMap() {
    return mapIdBitMap;
  }

  public StorageInfo getStorageInfo() {
    if (diskFileInfo != null) {
      if (diskFileInfo.isHdfs()) {
        if (deleted) {
          return null;
        } else {
          return new StorageInfo(StorageInfo.Type.HDFS, true, diskFileInfo.getFilePath());
        }
      } else {
        return new StorageInfo(((LocalFlusher) flusher).diskType(), true, "");
      }
    } else {
      Preconditions.checkArgument(memoryFileInfo != null);
      return new StorageInfo(StorageInfo.Type.MEMORY, true, "");
    }
  }

  public abstract long close() throws IOException;

  @FunctionalInterface
  public interface RunnableWithIOException {
    void run() throws IOException;
  }

  public boolean isClosed() {
    return closed;
  }

  protected synchronized long close(
      RunnableWithIOException tryClose,
      RunnableWithIOException streamClose,
      RunnableWithIOException finalClose)
      throws IOException {
    if (closed) {
      String msg = getFileAlreadyClosedMsg();
      logger.error(msg);
      throw new AlreadyClosedException(msg);
    }

    try {
      waitOnNoPending(numPendingWrites, false);
      closed = true;

      synchronized (flushLock) {
        if (!isMemoryShuffleFile.get()) {
          // memory shuffle file doesn't need final flush
          if (flushBuffer != null && flushBuffer.readableBytes() > 0) {
            flush(true, false);
          }
        }
      }

      tryClose.run();
      waitOnNoPending(notifier.numPendingFlushes, true);
    } finally {
      returnBuffer(false);
      try {
        if (channel != null) {
          channel.close();
        }
        streamClose.run();
      } catch (IOException e) {
        logger.warn("close file writer {} failed", this, e);
      }

      finalClose.run();

      // unregister from DeviceMonitor
      if (diskFileInfo != null && !diskFileInfo.isHdfs()) {
        logger.debug("file info {} unregister from device monitor", diskFileInfo);
        deviceMonitor.unregisterFileWriter(this);
      }
    }
    if (workerGracefulShutdown) {
      if (diskFileInfo != null) {
        storageManager.notifyFileInfoCommitted(shuffleKey, getFile().getName(), diskFileInfo);
      }
    }
    if (diskFileInfo != null) {
      return diskFileInfo.getFileLength();
    } else {
      return memoryFileInfo.getFileLength();
    }
  }

  private String getFileAlreadyClosedMsg() {
    String msg = "PartitionDataWriter has already closed! ";
    if (isMemoryShuffleFile.get()) {
      msg += "In memory file name:" + filename;
    } else {
      msg += "Disk file name:" + diskFileInfo.getFilePath();
    }
    return msg;
  }

  public void evict(boolean checkClose) throws IOException {
    // this lock is used to make sure that
    // memory manager won't evict with writer thread concurrently
    synchronized (flushLock) {
      if (checkClose) {
        // close and evict might be invoked concurrently
        // do not evict committed files from memory manager
        // evict memory file info if worker is shutdown gracefully
        if (isClosed()) {
          return;
        }
      }
      if (memoryFileInfo != null) {
        evictInternal();
        if (isClosed()) {
          waitOnNoPending(notifier.numPendingFlushes, true);
          storageManager.notifyFileInfoCommitted(shuffleKey, getFile().getName(), diskFileInfo);
        }
      }
    }
  }

  public synchronized void destroy(IOException ioException) {
    if (!closed) {
      closed = true;
      if (!notifier.hasException()) {
        notifier.setException(ioException);
      }
      returnBuffer(true);
      try {
        if (channel != null) {
          channel.close();
        }
      } catch (IOException e) {
        logger.warn(
            "Close channel failed for file {} caused by {}.",
            diskFileInfo.getFilePath(),
            e.getMessage());
      }
    }

    if (!destroyed) {
      destroyed = true;
      if (diskFileInfo != null) {
        diskFileInfo.deleteAllFiles(StorageManager.hadoopFs());

        // unregister from DeviceMonitor
        if (!diskFileInfo.isHdfs()) {
          deviceMonitor.unregisterFileWriter(this);
        }
      }
    }
  }

  public FileInfo getCurrentFileInfo() {
    if (!isMemoryShuffleFile.get()) {
      return diskFileInfo;
    } else {
      return memoryFileInfo;
    }
  }

  public IOException getException() {
    if (notifier.hasException()) {
      return notifier.exception.get();
    } else {
      return null;
    }
  }

  protected void waitOnNoPending(AtomicInteger counter, boolean failWhenTimeout)
      throws IOException {
    long waitTime = writerCloseTimeoutMs;
    while (counter.get() > 0 && waitTime > 0) {
      try {
        notifier.checkException();
        TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        IOException ioe = new IOException(e);
        notifier.setException(ioe);
        throw ioe;
      }
      waitTime -= WAIT_INTERVAL_MS;
    }
    if (counter.get() > 0 && failWhenTimeout) {
      IOException ioe = new IOException("Wait pending actions timeout, Counter: " + counter.get());
      notifier.setException(ioe);
      throw ioe;
    }
    notifier.checkException();
  }

  protected void takeBuffer() {
    String metricsName = null;
    String fileAbsPath = null;
    if (metricsCollectCriticalEnabled) {
      metricsName = WorkerSource.TAKE_BUFFER_TIME();
      fileAbsPath = diskFileInfo.getFilePath();
      source.startTimer(metricsName, fileAbsPath);
    }

    synchronized (flushLock) {
      if (diskFileInfo != null) {
        flushBuffer = flusher.takeBuffer();
      } else {
        if (flushBuffer == null) {
          flushBuffer = pooledByteBufAllocator.compositeBuffer(Integer.MAX_VALUE);
        }
      }
    }

    if (metricsCollectCriticalEnabled) {
      source.stopTimer(metricsName, fileAbsPath);
    }
  }

  protected void addTask(FlushTask task) throws IOException {
    if (!flusher.addTask(task, writerCloseTimeoutMs, flushWorkerIndex)) {
      IOException e = new IOException("Add flush task timeout.");
      notifier.setException(e);
      throw e;
    }
  }

  protected void returnBuffer(boolean destroy) {
    synchronized (flushLock) {
      if (flushBuffer != null) {
        if (flusher != null) {
          flusher.returnBuffer(flushBuffer, true);
          flushBuffer = null;
        } else {
          if (destroy) {
            flushBuffer.removeComponents(0, flushBuffer.numComponents());
            flushBuffer.release();
          }
        }
      }
    }
  }

  @Override
  public int hashCode() {
    return diskFileInfo.getFilePath().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof PartitionDataWriter)
        && diskFileInfo
            .getFilePath()
            .equals(((PartitionDataWriter) obj).diskFileInfo.getFilePath());
  }

  @Override
  public String toString() {
    return shuffleKey + "-" + filename + " partition-writer";
  }

  public void flushOnMemoryPressure() throws IOException {
    synchronized (flushLock) {
      // this won't happen if this writer is in memory
      flush(false, false);
    }
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
    return memoryFileInfo;
  }
}
