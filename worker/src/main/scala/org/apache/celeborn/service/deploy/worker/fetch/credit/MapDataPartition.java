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

package org.apache.celeborn.service.deploy.worker.fetch.credit;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.celeborn.common.memory.BufferRecycler;
import org.apache.celeborn.common.memory.MemoryManager;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.util.JavaUtils;

public class MapDataPartition {
  public static final Logger logger = LoggerFactory.getLogger(MapDataPartition.class);
  private final List<Long> activeStreamIds = new ArrayList<>();
  private final FileInfo fileInfo;
  private final Set<MapDataPartitionReader> readers = new HashSet<>();
  private final ExecutorService readExecutor;
  private final ConcurrentHashMap<Long, MapDataPartitionReader> streamReaders =
      JavaUtils.newConcurrentHashMap();

  /** All available buffers can be used by the partition readers for reading. */
  private Queue<ByteBuf> buffers;

  private FileChannel dataFileChanel;
  private FileChannel indexChannel;

  private boolean isReleased;
  private MemoryManager memoryManager = MemoryManager.instance();
  private int maxReadBuffers;
  private int minReadBuffers;

  public MapDataPartition(
      FileInfo fileInfo, ExecutorService executorService, int maxReadBuffers, int minReadBuffers)
      throws FileNotFoundException {
    this.fileInfo = fileInfo;
    readExecutor = executorService;
    dataFileChanel = new FileInputStream(fileInfo.getFile()).getChannel();
    indexChannel = new FileInputStream(fileInfo.getIndexPath()).getChannel();
    this.maxReadBuffers = maxReadBuffers;
    this.minReadBuffers = minReadBuffers;
  }

  public synchronized void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel, Runnable recycleStream) {
    MapDataPartitionReader mapDataPartitionReader =
        new MapDataPartitionReader(
            startSubIndex, endSubIndex, fileInfo, streamId, channel, recycleStream);
    // allocate resources when the first reader is registered
    boolean allocateResources = readers.isEmpty();
    readers.add(mapDataPartitionReader);
    streamReaders.put(streamId, mapDataPartitionReader);

    // create initial buffers for read
    if (allocateResources && buffers == null) {
      memoryManager.requestReadBuffers(
          minReadBuffers,
          maxReadBuffers,
          fileInfo.getBufferSize(),
          (allocatedBuffers, throwable) -> onBuffer(new LinkedBlockingDeque<>(allocatedBuffers)));
    } else {
      triggerRead();
    }
  }

  // Read logic is executed on another thread.
  public void onBuffer(Queue<ByteBuf> buffers) {
    this.buffers = buffers;
    triggerRead();
  }

  public void recycle(ByteBuf buffer, Queue<ByteBuf> bufferQueue) {
    buffer.clear();
    bufferQueue.add(buffer);
    triggerRead();
  }

  public synchronized void readBuffers() {
    if (isReleased) {
      // some read executor task may already be submitted to the threadpool
      return;
    }

    try {
      PriorityQueue<MapDataPartitionReader> sortedReaders = new PriorityQueue<>(readers);
      for (MapDataPartitionReader reader : readers) {
        reader.open(dataFileChanel, indexChannel);
      }
      while (buffers != null && buffers.size() > 0 && !sortedReaders.isEmpty()) {
        BufferRecycler bufferRecycler =
            new BufferRecycler(memoryManager, (buffer) -> recycle(buffer, buffers));
        MapDataPartitionReader reader = sortedReaders.poll();
        try {
          if (!reader.readAndSend(buffers, bufferRecycler)) {
            readers.remove(reader);
          }
        } catch (Throwable e) {
          logger.error("reader exception, reader: {}, message: {}", reader, e.getMessage(), e);
          readers.remove(reader);
          reader.recycleOnError(e);
        }
      }
    } catch (Throwable e) {
      logger.error("Fatal: failed to read partition data. {}", e.getMessage(), e);
      for (MapDataPartitionReader reader : readers) {
        reader.recycleOnError(e);
      }

      readers.clear();
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

  public MapDataPartitionReader getStreamReader(long streamId) {
    return streamReaders.get(streamId);
  }

  public boolean releaseStream(Long streamId) {
    MapDataPartitionReader mapDataPartitionReader = streamReaders.get(streamId);
    mapDataPartitionReader.release();
    if (mapDataPartitionReader.isFinished()) {
      logger.debug("release all for stream: {}", streamId);
      synchronized (activeStreamIds) {
        activeStreamIds.remove(streamId);
        streamReaders.remove(streamId);
      }
      return true;
    }

    return false;
  }

  public void close() {
    logger.info("release map data partition {}", fileInfo);

    IOUtils.closeQuietly(dataFileChanel);
    IOUtils.closeQuietly(indexChannel);

    if (buffers != null) {
      for (ByteBuf buffer : buffers) {
        memoryManager.recycleReadBuffer(buffer);
      }
    }

    buffers = null;

    isReleased = true;
  }

  public boolean hasActiveStreamIds() {
    return !activeStreamIds.isEmpty();
  }

  public FileInfo getFileInfo() {
    return fileInfo;
  }
}
