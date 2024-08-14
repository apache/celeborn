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
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.service.deploy.worker.memory.BufferQueue;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

// this means active data partition
public class MapPartitionData implements MemoryManager.ReadBufferTargetChangeListener {
  public static final Logger logger = LoggerFactory.getLogger(MapPartitionData.class);
  protected final DiskFileInfo diskFileInfo;
  protected final MapFileMeta mapFileMeta;
  protected final ScheduledExecutorService readExecutor;
  protected final ConcurrentHashMap<Long, MapPartitionDataReader> readers =
      JavaUtils.newConcurrentHashMap();
  protected FileChannel dataFileChanel;
  protected FileChannel indexChannel;
  protected long indexSize;
  protected volatile boolean isReleased = false;
  protected final BufferQueue bufferQueue = new BufferQueue();
  private AtomicBoolean bufferQueueInitialized = new AtomicBoolean(false);
  private MemoryManager memoryManager = MemoryManager.instance();
  protected Consumer<Long> recycleStream;
  private int minReadBuffers;
  private int maxReadBuffers;
  private int minBuffersToTriggerRead;
  protected AtomicBoolean hasReadingTask = new AtomicBoolean(false);

  public MapPartitionData(
      int minReadBuffers,
      int maxReadBuffers,
      HashMap<String, ScheduledExecutorService> storageFetcherPool,
      int threadsPerMountPoint,
      DiskFileInfo diskFileInfo,
      Consumer<Long> recycleStream,
      int minBuffersToTriggerRead)
      throws IOException {
    this.recycleStream = recycleStream;
    this.diskFileInfo = diskFileInfo;
    this.mapFileMeta = (MapFileMeta) diskFileInfo.getFileMeta();

    this.minReadBuffers = minReadBuffers;
    this.maxReadBuffers = maxReadBuffers;

    updateBuffersTarget((this.minReadBuffers + this.maxReadBuffers) / 2 + 1);
    logger.debug(
        "read map partition {} with {} {}",
        diskFileInfo.getFilePath(),
        bufferQueue.getLocalBuffersTarget(),
        mapFileMeta.getBufferSize());

    this.minBuffersToTriggerRead = minBuffersToTriggerRead;

    readExecutor =
        storageFetcherPool.computeIfAbsent(
            mapFileMeta.getMountPoint(),
            // From the java doc of {@link ScheduledThreadPoolExecutor}, it is a fixed-sized pool
            // using corePoolSize threads, so we can use it safely.
            k ->
                new ScheduledThreadPoolExecutor(
                    threadsPerMountPoint,
                    new ThreadFactoryBuilder()
                        .setNameFormat(mapFileMeta.getMountPoint() + "-reader-thread-%d")
                        .setUncaughtExceptionHandler(
                            (t1, t2) -> {
                              logger.warn("StorageFetcherPool thread:{}:{}", t1, t2);
                            })
                        .build()));
    openChannels(diskFileInfo);

    MemoryManager.instance().addReadBufferTargetChangeListener(this);
  }

  private synchronized void updateBuffersTarget(int buffersTarget) {
    int currentBuffersTarget = buffersTarget;
    if (currentBuffersTarget < minReadBuffers) {
      currentBuffersTarget = minReadBuffers;
    }
    if (currentBuffersTarget > maxReadBuffers) {
      currentBuffersTarget = maxReadBuffers;
    }
    bufferQueue.setLocalBuffersTarget(currentBuffersTarget);
  }

  public void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel) {
    MapPartitionDataReader mapPartitionDataReader =
        new MapPartitionDataReader(
            startSubIndex,
            endSubIndex,
            diskFileInfo,
            streamId,
            channel,
            readExecutor,
            () -> recycleStream.accept(streamId));
    readers.put(streamId, mapPartitionDataReader);
  }

  public void tryRequestBufferOrRead() {
    boolean needTriggerRead = true;
    if (bufferQueueInitialized.compareAndSet(false, true)) {
      // In flink hybrid shuffle, the applyNewBuffers may fail to request buffers if the file buffer
      // size has not been initialized.
      // In such case, the next read operation should be scheduled to avoid job hang.
      needTriggerRead = !applyNewBuffers();
    }

    if (needTriggerRead) {
      triggerRead();
    }
  }

  protected boolean applyNewBuffers() {
    return bufferQueue.tryApplyNewBuffers(
        readers.size(),
        mapFileMeta.getBufferSize(),
        (allocatedBuffers, throwable) -> onBuffer(allocatedBuffers));
  }

  // Read logic is executed on another thread.
  public void onBuffer(List<ByteBuf> buffers) {
    if (isReleased) {
      buffers.forEach(memoryManager::recycleReadBuffer);
      return;
    }

    bufferQueue.add(buffers);

    if (bufferQueue.size()
        >= Math.min(bufferQueue.getLocalBuffersTarget() / 2 + 1, minBuffersToTriggerRead)) {
      triggerRead();
    }
  }

  public void recycle(ByteBuf buffer) {
    if (isReleased) {
      // this means bufferQueue is already release
      memoryManager.recycleReadBuffer(buffer);
      return;
    }

    bufferQueue.recycle(buffer);

    if (bufferQueue.size()
        >= Math.min(bufferQueue.getLocalBuffersTarget() / 2 + 1, minBuffersToTriggerRead)) {
      triggerRead();
    }

    bufferQueue.tryApplyNewBuffers(
        readers.size(),
        mapFileMeta.getBufferSize(),
        (allocatedBuffers, throwable) -> onBuffer(allocatedBuffers));
  }

  public synchronized int readBuffers() {
    hasReadingTask.set(false);
    if (isReleased) {
      // some read executor task may already be submitted to the thread pool
      return 0;
    }

    try {
      PriorityQueue<MapPartitionDataReader> sortedReaders =
          new PriorityQueue<>(
              readers.values().stream()
                  .filter(MapPartitionDataReader::shouldReadData)
                  .collect(Collectors.toList()));
      for (MapPartitionDataReader reader : sortedReaders) {
        reader.open(dataFileChanel, indexChannel, true, indexSize);
      }
      while (bufferQueue.bufferAvailable() && !sortedReaders.isEmpty()) {
        BufferRecycler bufferRecycler = new BufferRecycler(MapPartitionData.this::recycle);
        MapPartitionDataReader reader = sortedReaders.poll();
        try {
          reader.readData(bufferQueue, bufferRecycler);
        } catch (Throwable e) {
          logger.error("reader exception, reader: {}, message: {}", reader, e.getMessage(), e);
          reader.recycleOnError(e);
        }
      }
    } catch (Throwable e) {
      logger.error("Fatal: failed to read partition data. {}", e.getMessage(), e);
      for (MapPartitionDataReader reader : readers.values()) {
        reader.recycleOnError(e);
      }
    }
    return 0;
  }

  protected void openChannels(DiskFileInfo fileInfo) throws IOException {
    this.dataFileChanel = FileChannelUtils.openReadableFileChannel(fileInfo.getFilePath());
    this.indexChannel = FileChannelUtils.openReadableFileChannel(fileInfo.getIndexPath());
    this.indexSize = indexChannel.size();
  }

  protected void releaseExtraBuffers() {
    bufferQueue.recycleExtraBuffersToGlobalPool();
  }

  // for one reader only the associated channel can access
  public void addReaderCredit(int numCredit, long streamId) {
    MapPartitionDataReader streamReader = getStreamReader(streamId);
    if (streamReader != null) {
      streamReader.addCredit(numCredit);
      readExecutor.submit(() -> streamReader.sendData());
    }
  }

  public void triggerRead() {
    // Key for IO schedule.
    if (hasReadingTask.compareAndSet(false, true)) {
      readExecutor.submit(() -> readBuffers());
    }
  }

  public MapPartitionDataReader getStreamReader(long streamId) {
    return readers.get(streamId);
  }

  public boolean releaseReader(Long streamId) {
    MapPartitionDataReader mapPartitionDataReader = readers.get(streamId);
    mapPartitionDataReader.release();
    if (mapPartitionDataReader.isFinished()) {
      logger.debug("release all for stream: {}", streamId);
      readers.remove(streamId);
      return true;
    }

    return false;
  }

  public void close() {
    logger.debug("release map data partition {}", diskFileInfo);
    bufferQueue.release();
    isReleased = true;

    IOUtils.closeQuietly(dataFileChanel);
    IOUtils.closeQuietly(indexChannel);

    MemoryManager.instance().removeReadBufferTargetChangeListener(this);
  }

  @Override
  public String toString() {
    return "MapDataPartition{" + "fileInfo=" + diskFileInfo.getFilePath() + '}';
  }

  public ConcurrentHashMap<Long, MapPartitionDataReader> getReaders() {
    return readers;
  }

  public DiskFileInfo getDiskFileInfo() {
    return diskFileInfo;
  }

  @Override
  public void onChange(long newMemoryTarget) {
    updateBuffersTarget((int) Math.ceil(newMemoryTarget * 1.0 / mapFileMeta.getBufferSize()));
    bufferQueue.trim();
  }
}
