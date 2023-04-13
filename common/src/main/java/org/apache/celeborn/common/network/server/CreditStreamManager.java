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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.network.server.memory.MemoryManager;
import org.apache.celeborn.common.util.JavaUtils;

public class CreditStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(CreditStreamManager.class);
  private final AtomicLong nextStreamId;
  private final ConcurrentHashMap<Long, StreamState> streams;
  private final ConcurrentHashMap<FileInfo, MapDataPartition> activeMapPartitions;
  private final HashMap<String, ExecutorService> storageFetcherPool = new HashMap<>();
  private int minReadBuffers;
  private int maxReadBuffers;
  private int threadsPerMountPoint;
  private int minBuffersToTriggerRead;
  private final BlockingQueue<Long> recycleStreamIds = new LinkedBlockingQueue<>();

  @GuardedBy("lock")
  private volatile Thread recycleThread;

  private final Object lock = new Object();

  public CreditStreamManager(
      int minReadBuffers,
      int maxReadBuffers,
      int threadsPerMountpoint,
      int minBuffersToTriggerRead) {
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = JavaUtils.newConcurrentHashMap();
    activeMapPartitions = JavaUtils.newConcurrentHashMap();
    this.minReadBuffers = minReadBuffers;
    this.maxReadBuffers = maxReadBuffers;
    threadsPerMountPoint = threadsPerMountpoint;
    this.minBuffersToTriggerRead = minBuffersToTriggerRead;
    MemoryManager.instance().setCreditStreamManager(this);
    logger.debug(
        "Initialize buffer stream manager with {} {} {}",
        this.minReadBuffers,
        this.maxReadBuffers,
        threadsPerMountpoint);
  }

  public long registerStream(
      Consumer<Long> callback,
      Channel channel,
      int initialCredit,
      int startSubIndex,
      int endSubIndex,
      FileInfo fileInfo)
      throws IOException {
    long streamId = nextStreamId.getAndIncrement();
    logger.debug(
        "Register stream start from {}, streamId: {}, fileInfo: {}",
        channel.remoteAddress(),
        streamId,
        fileInfo);
    synchronized (activeMapPartitions) {
      MapDataPartition mapDataPartition = activeMapPartitions.get(fileInfo);
      if (mapDataPartition == null) {
        mapDataPartition =
            new MapDataPartition(
                minReadBuffers,
                maxReadBuffers,
                storageFetcherPool,
                threadsPerMountPoint,
                fileInfo,
                id -> recycleStream(id),
                minBuffersToTriggerRead);
        activeMapPartitions.put(fileInfo, mapDataPartition);
      }
      StreamState streamState =
          new StreamState(channel, fileInfo.getBufferSize(), mapDataPartition);
      streams.put(streamId, streamState);
      mapDataPartition.setupDataPartitionReader(startSubIndex, endSubIndex, streamId, channel);
    }

    callback.accept(streamId);
    addCredit(initialCredit, streamId);

    logger.debug("Register stream streamId: {}, fileInfo: {}", streamId, fileInfo);

    return streamId;
  }

  private void addCredit(MapDataPartition mapDataPartition, int numCredit, long streamId) {
    logger.debug("streamId: {}, add credit: {}", streamId, numCredit);
    try {
      if (mapDataPartition != null && numCredit > 0) {
        mapDataPartition.addReaderCredit(numCredit, streamId);
      }
    } catch (Throwable e) {
      logger.error("streamId: {}, add credit end: {}", streamId, numCredit);
    }
  }

  public void addCredit(int numCredit, long streamId) {
    MapDataPartition mapDataPartition = streams.get(streamId).getMapDataPartition();
    addCredit(mapDataPartition, numCredit, streamId);
  }

  public void connectionTerminated(Channel channel) {
    for (Map.Entry<Long, StreamState> entry : streams.entrySet()) {
      if (entry.getValue().getAssociatedChannel() == channel) {
        logger.info("connection closed, clean streamId: {}", entry.getKey());
        recycleStream(entry.getKey());
      }
    }
  }

  public void notifyStreamEndByClient(long streamId) {
    recycleStream(streamId);
  }

  public void recycleStream(long streamId) {
    recycleStreamIds.add(streamId);
    startRecycleThread(); // lazy start thread
  }

  @VisibleForTesting
  public int numStreamStates() {
    return streams.size();
  }

  @VisibleForTesting
  public int numRecycleStreams() {
    return recycleStreamIds.size();
  }

  @VisibleForTesting
  public ConcurrentHashMap<Long, StreamState> getStreams() {
    return streams;
  }

  private void startRecycleThread() {
    if (recycleThread == null) {
      synchronized (lock) {
        if (recycleThread == null) {
          recycleThread =
              new Thread(
                  () -> {
                    while (true) {
                      List<Long> retryList = new ArrayList<>();
                      try {
                        Long streamId = recycleStreamIds.poll();
                        if (streamId != null) {
                          if (!cleanResource(streamId)) {
                            retryList.add(streamId);
                          }
                        } else {
                          recycleStreamIds.addAll(retryList);
                          Thread.sleep(100);
                        }
                      } catch (Throwable e) {
                        logger.warn(e.getMessage(), e);
                      }
                    }
                  },
                  "recycle-thread");
          recycleThread.setDaemon(true);
          recycleThread.start();

          logger.info("start stream recycle thread");
        }
      }
    }
  }

  public boolean cleanResource(Long streamId) {
    logger.debug("received clean stream: {}", streamId);
    if (streams.containsKey(streamId)) {
      MapDataPartition mapDataPartition = streams.get(streamId).getMapDataPartition();
      if (mapDataPartition != null) {
        if (mapDataPartition.releaseReader(streamId)) {
          streams.remove(streamId);
          if (mapDataPartition.getReaders().isEmpty()) {
            synchronized (activeMapPartitions) {
              if (mapDataPartition.getReaders().isEmpty()) {
                mapDataPartition.close();
                FileInfo fileInfo = mapDataPartition.getFileInfo();
                activeMapPartitions.remove(fileInfo);
              }
            }
          }
        } else {
          logger.debug("retry clean stream: {}", streamId);
          return false;
        }
      }
    }
    return true;
  }

  public long getStreamsCount() {
    return streams.size();
  }

  public int getActiveMapPartitionCount() {
    return activeMapPartitions.size();
  }

  protected class StreamState {
    private Channel associatedChannel;
    private int bufferSize;
    private MapDataPartition mapDataPartition;

    public StreamState(
        Channel associatedChannel, int bufferSize, MapDataPartition mapDataPartition) {
      this.associatedChannel = associatedChannel;
      this.bufferSize = bufferSize;
      this.mapDataPartition = mapDataPartition;
    }

    public Channel getAssociatedChannel() {
      return associatedChannel;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public MapDataPartition getMapDataPartition() {
      return mapDataPartition;
    }
  }
}
