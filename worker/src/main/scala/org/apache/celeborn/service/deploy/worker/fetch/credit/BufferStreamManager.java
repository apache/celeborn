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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.util.JavaUtils;

public class BufferStreamManager {
  private static final Logger logger = LoggerFactory.getLogger(BufferStreamManager.class);
  private final AtomicLong nextStreamId;
  private final ConcurrentHashMap<Long, StreamState> streams;
  private final ConcurrentHashMap<FileInfo, MapDataPartition> activeMapPartitions;
  private int minReadBuffers;
  private int maxReadBuffers;
  private int threadsPerMountPoint;
  private final BlockingQueue<DelayedStreamId> recycleStreamIds = new DelayQueue<>();

  @GuardedBy("lock")
  private volatile Thread recycleThread;

  private final Object lock = new Object();
  private final HashMap<String, ExecutorService> executorPools = new HashMap<>();

  public BufferStreamManager(int minReadBuffers, int maxReadBuffers, int threadsPerMountpoint) {
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = JavaUtils.newConcurrentHashMap();
    activeMapPartitions = JavaUtils.newConcurrentHashMap();
    this.minReadBuffers = minReadBuffers;
    this.maxReadBuffers = maxReadBuffers;
    this.threadsPerMountPoint = threadsPerMountpoint;
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
    StreamState streamState = new StreamState(channel, fileInfo.getBufferSize());
    streams.put(streamId, streamState);
    logger.debug(
        "Register stream start from {}, streamId: {}, fileInfo: {}",
        channel.remoteAddress(),
        streamId,
        fileInfo);
    MapDataPartition mapDataPartition;
    synchronized (activeMapPartitions) {
      mapDataPartition = activeMapPartitions.get(fileInfo);
      if (mapDataPartition == null) {
        mapDataPartition =
            new MapDataPartition(
                fileInfo,
                executorPools.computeIfAbsent(
                    fileInfo.getMountPoint(),
                    k ->
                        Executors.newFixedThreadPool(
                            threadsPerMountPoint,
                            new ThreadFactoryBuilder()
                                .setNameFormat("reader-thread-%d")
                                .setUncaughtExceptionHandler(
                                    (t1, t2) -> {
                                      logger.warn("StorageFetcherPool thread:{}:{}", t1, t2);
                                    })
                                .build())),
                maxReadBuffers,
                minReadBuffers);
        activeMapPartitions.put(fileInfo, mapDataPartition);
      }
      mapDataPartition.addStream(streamId);
    }

    addCredit(initialCredit, streamId);
    streamState.setMapDataPartition(mapDataPartition);
    // response streamId to channel first
    callback.accept(streamId);
    mapDataPartition.setupDataPartitionReader(
        startSubIndex, endSubIndex, streamId, channel, () -> recycleStream(streamId));

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
    recycleStreamIds.add(new DelayedStreamId(streamId));
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
                      try {
                        DelayedStreamId delayedStreamId = recycleStreamIds.take();
                        cleanResource(delayedStreamId.streamId);
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

  public void cleanResource(Long streamId) {
    logger.debug("received clean stream: {}", streamId);
    if (streams.containsKey(streamId)) {
      MapDataPartition mapDataPartition = streams.get(streamId).getMapDataPartition();
      if (mapDataPartition != null) {
        if (mapDataPartition.releaseStream(streamId)) {
          streams.remove(streamId);
          if (!mapDataPartition.hasActiveStreamIds()) {
            synchronized (activeMapPartitions) {
              if (!mapDataPartition.hasActiveStreamIds()) {
                mapDataPartition.close();
                FileInfo fileInfo = mapDataPartition.getFileInfo();
                activeMapPartitions.remove(fileInfo);
              }
            }
          }
        } else {
          logger.debug("retry clean stream: {}", streamId);
          recycleStreamIds.add(new DelayedStreamId(streamId));
        }
      }
    }
  }

  public static class DelayedStreamId implements Delayed {
    private static final long delayTime = 100; // 100ms
    private long createMillis = System.currentTimeMillis();

    private long streamId;

    public DelayedStreamId(long streamId) {
      createMillis = createMillis + delayTime;
      this.streamId = streamId;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long diff = createMillis - System.currentTimeMillis();
      return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    public long getCreateMillis() {
      return createMillis;
    }

    @Override
    public int compareTo(Delayed o) {
      long otherCreateMillis = ((DelayedStreamId) o).getCreateMillis();
      if (createMillis < otherCreateMillis) {
        return -1;
      } else if (createMillis > otherCreateMillis) {
        return 1;
      }

      return 0;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("DelayedStreamId{");
      sb.append("createMillis=").append(createMillis);
      sb.append(", streamId=").append(streamId);
      sb.append('}');
      return sb.toString();
    }
  }

  @VisibleForTesting
  protected class StreamState {
    private Channel associatedChannel;
    private int bufferSize;
    private MapDataPartition mapDataPartition;

    public StreamState(Channel associatedChannel, int bufferSize) {
      this.associatedChannel = associatedChannel;
      this.bufferSize = bufferSize;
    }

    public MapDataPartition getMapDataPartition() {
      return mapDataPartition;
    }

    public void setMapDataPartition(MapDataPartition mapDataPartition) {
      this.mapDataPartition = mapDataPartition;
    }

    public Channel getAssociatedChannel() {
      return associatedChannel;
    }

    public int getBufferSize() {
      return bufferSize;
    }
  }
}
