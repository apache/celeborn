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

package org.apache.celeborn.common.network.server;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.server.memory.BufferQueue;
import org.apache.celeborn.common.network.server.memory.BufferRecycler;
import org.apache.celeborn.common.network.server.memory.MemoryManager;
import org.apache.celeborn.common.network.server.memory.ReadBufferRequest;
import org.apache.celeborn.common.util.JavaUtils;

// this means active data partition
class MapDataPartition implements MemoryManager.ReadBufferTargetChangeListener {
  public static final Logger logger = LoggerFactory.getLogger(MapDataPartition.class);
  private final List<Long> activeStreamIds = new ArrayList<>();
  private final FileInfo fileInfo;
  private final ExecutorService readExecutor;
  private final ConcurrentHashMap<Long, MapDataPartitionReader> readers =
      JavaUtils.newConcurrentHashMap();
  private FileChannel dataFileChanel;
  private FileChannel indexChannel;
  private volatile boolean isReleased = false;
  private final BufferQueue bufferQueue = new BufferQueue();
  private boolean bufferQueueInitialized = false;
  private MemoryManager memoryManager = MemoryManager.instance();
  private Consumer<Long> recycleStream;
  private volatile int localBuffersTarget = 0;
  private volatile int pendingRequestBuffers = 0;
  private Object applyBufferLock = new Object();
  private long minReadAheadMemory;
  private long maxReadAheadMemory;
  private int minBuffersToTriggerRead;

  public MapDataPartition(
      int minReadBuffers,
      int maxReadBuffers,
      HashMap<String, ExecutorService> storageFetcherPool,
      int threadsPerMountPoint,
      FileInfo fileInfo,
      Consumer<Long> recycleStream,
      int minBuffersToTriggerRead)
      throws FileNotFoundException {
    this.recycleStream = recycleStream;
    this.fileInfo = fileInfo;
    int bufferSize = fileInfo.getBufferSize();

    minReadAheadMemory = minReadBuffers * bufferSize;
    maxReadAheadMemory = maxReadBuffers * bufferSize;

    updateLocalTarget(0, fileInfo.getFileSize(), minReadAheadMemory, maxReadAheadMemory);

    logger.debug(
        "read map partition {} with {} {} {}",
        fileInfo.getFilePath(),
        localBuffersTarget,
        fileInfo.getBufferSize());

    this.minBuffersToTriggerRead = minBuffersToTriggerRead;

    readExecutor =
        storageFetcherPool.computeIfAbsent(
            fileInfo.getMountPoint(),
            k ->
                Executors.newFixedThreadPool(
                    threadsPerMountPoint,
                    new ThreadFactoryBuilder()
                        .setNameFormat(fileInfo.getMountPoint() + "-reader-thread-%d")
                        .setUncaughtExceptionHandler(
                            (t1, t2) -> {
                              logger.warn("StorageFetcherPool thread:{}:{}", t1, t2);
                            })
                        .build()));
    this.dataFileChanel = new FileInputStream(fileInfo.getFile()).getChannel();
    this.indexChannel = new FileInputStream(fileInfo.getIndexPath()).getChannel();
  }

  private synchronized void updateLocalTarget(
      long nTarget, long fileSize, long definedMinReadAheadMemory, long definedMaxReadAheadMemory) {
    long target = nTarget;
    if (target < definedMinReadAheadMemory) {
      target = definedMinReadAheadMemory;
    }
    if (target > definedMaxReadAheadMemory) {
      target = definedMaxReadAheadMemory;
    }
    if (target > fileSize) {
      target = fileSize;
    }
    localBuffersTarget = (int) Math.ceil(target * 1.0 / fileInfo.getBufferSize());
  }

  public synchronized void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel) {
    MapDataPartitionReader mapDataPartitionReader =
        new MapDataPartitionReader(
            startSubIndex,
            endSubIndex,
            fileInfo,
            streamId,
            channel,
            () -> recycleStream.accept(streamId));
    readers.put(streamId, mapDataPartitionReader);

    // allocate resources when the first reader is registered
    if (!bufferQueueInitialized) {
      memoryManager.requestReadBuffers(
          new ReadBufferRequest(
              localBuffersTarget,
              fileInfo.getBufferSize(),
              (allocatedBuffers, throwable) -> onBuffer(allocatedBuffers)));
      bufferQueueInitialized = true;
    } else {
      triggerRead();
    }
  }

  // Read logic is executed on another thread.
  public void onBuffer(List<ByteBuf> buffers) {
    if (isReleased || bufferQueue.isReleased()) {
      buffers.forEach(memoryManager::recycleReadBuffer);
      return;
    }

    try {
      bufferQueue.add(buffers);
      pendingRequestBuffers -= buffers.size();
    } catch (Exception e) {
      // this branch means that this bufferQueue is closed
      buffers.forEach(memoryManager::recycleReadBuffer);
      return;
    }

    if (bufferQueue.size() >= Math.min(localBuffersTarget / 2 + 1, minBuffersToTriggerRead)) {
      triggerRead();
    }
  }

  public void recycle(ByteBuf buffer) {
    if (bufferQueue.numBuffersOccupied() > localBuffersTarget) {
      bufferQueue.recycle(buffer);
    } else {
      buffer.clear();
      bufferQueue.add(buffer);
    }
    if (isReleased || readers.isEmpty() || bufferQueue.isReleased()) {
      return;
    }

    if (bufferQueue.size() >= Math.min(localBuffersTarget / 2 + 1, minBuffersToTriggerRead)) {
      triggerRead();
    }

    applyNewBuffers();
  }

  private void applyNewBuffers() {
    logger.debug(
        "try to apply new buffers  {} {} {} {}",
        bufferQueue.numBuffersOccupied(),
        bufferQueue.size(),
        readers.size(),
        localBuffersTarget);
    synchronized (applyBufferLock) {
      if (!readers.isEmpty()
          && bufferQueue.numBuffersOccupied() + pendingRequestBuffers < localBuffersTarget) {
        int newBuffersCount =
            (localBuffersTarget - bufferQueue.numBuffersOccupied() - pendingRequestBuffers);
        logger.debug(
            "apply new buffers {} while current buffer queue size {} with read count {} for "
                + "map data partition {} with active stream id count {}",
            newBuffersCount,
            bufferQueue.numBuffersOccupied(),
            readers.size(),
            this,
            activeStreamIds.size());

        pendingRequestBuffers += newBuffersCount;
        memoryManager.requestReadBuffers(
            new ReadBufferRequest(
                newBuffersCount,
                fileInfo.getBufferSize(),
                (allocatedBuffers, throwable) -> onBuffer(allocatedBuffers)));
      }
    }
  }

  public synchronized void readBuffers() {
    if (isReleased) {
      // some read executor task may already be submitted to the thread pool
      return;
    }

    try {
      // make sure that all reader are open
      PriorityQueue<MapDataPartitionReader> sortedReaders = new PriorityQueue<>(readers.values());
      for (MapDataPartitionReader reader : readers.values()) {
        reader.open(dataFileChanel, indexChannel);
      }
      while (bufferQueue.size() > 0 && !sortedReaders.isEmpty()) {
        BufferRecycler bufferRecycler = new BufferRecycler(MapDataPartition.this::recycle);
        MapDataPartitionReader reader = sortedReaders.poll();
        try {
          reader.readData(bufferQueue, bufferRecycler);
        } catch (Throwable e) {
          logger.error("reader exception, reader: {}, message: {}", reader, e.getMessage(), e);
          // this reader failed , recycle stream id
          reader.recycleOnError(e);
        }
      }
    } catch (Throwable e) {
      logger.error("Fatal: failed to read partition data. {}", e.getMessage(), e);
      for (MapDataPartitionReader reader : readers.values()) {
        reader.recycleOnError(e);
      }
    }
  }

  // for one reader only the associated channel can access
  public void addReaderCredit(int numCredit, long streamId) {
    MapDataPartitionReader streamReader = getStreamReader(streamId);
    if (streamReader != null) {
      streamReader.addCredit(numCredit);
      readExecutor.submit(() -> streamReader.sendData());
    }
  }

  public void triggerRead() {
    // Key for IO schedule.
    readExecutor.submit(() -> readBuffers());
  }

  public void addStream(Long streamId) {
    synchronized (activeStreamIds) {
      activeStreamIds.add(streamId);
    }
  }

  public void removeStream(Long streamId) {
    synchronized (activeStreamIds) {
      activeStreamIds.remove(streamId);
      readers.remove(streamId);
    }
  }

  public MapDataPartitionReader getStreamReader(long streamId) {
    return readers.get(streamId);
  }

  public boolean releaseStream(Long streamId) {
    MapDataPartitionReader mapDataPartitionReader = readers.get(streamId);
    mapDataPartitionReader.release();
    if (mapDataPartitionReader.isFinished()) {
      logger.debug("release all for stream: {}", streamId);
      removeStream(streamId);
      return true;
    }

    return false;
  }

  public void close() {
    logger.debug("release map data partition {}", fileInfo);

    IOUtils.closeQuietly(dataFileChanel);
    IOUtils.closeQuietly(indexChannel);
    bufferQueue.release();

    isReleased = true;
  }

  @Override
  public String toString() {
    return "MapDataPartition{" + "fileInfo=" + fileInfo.getFilePath() + '}';
  }

  public List<Long> getActiveStreamIds() {
    return activeStreamIds;
  }

  public ConcurrentHashMap<Long, MapDataPartitionReader> getReaders() {
    return readers;
  }

  public FileInfo getFileInfo() {
    return fileInfo;
  }

  @Override
  public void onChange(long nVal) {
    updateLocalTarget(nVal, fileInfo.getFileSize(), minReadAheadMemory, maxReadAheadMemory);
    logger.debug("local buffers target {}", localBuffersTarget);
    while (bufferQueue.numBuffersOccupied() > localBuffersTarget) {
      recycle(bufferQueue.poll());
    }
  }
}
