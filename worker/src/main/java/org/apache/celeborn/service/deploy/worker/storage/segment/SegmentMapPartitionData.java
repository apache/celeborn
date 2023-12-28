/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.service.deploy.worker.storage.segment;

import static org.apache.commons.crypto.utils.Utils.checkArgument;
import static org.apache.commons.crypto.utils.Utils.checkState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.util.FileChannelUtils;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionData;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataReader;

public class SegmentMapPartitionData extends MapPartitionData {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionData.class);

  private static final int SCHEDULE_READ_INIT_INTERVAL_MS = 10;

  private static final int SCHEDULE_READ_MAT_INTERVAL_MS = 200;

  private int nextScheduleReadInterval = SCHEDULE_READ_INIT_INTERVAL_MS;

  private final boolean requireSubpartitionId;

  private final Map<Long, Channel> emptyStreamChannels = new HashMap<>();

  public SegmentMapPartitionData(
      int minReadBuffers,
      int maxReadBuffers,
      HashMap<String, ScheduledExecutorService> storageFetcherPool,
      int threadsPerMountPoint,
      DiskFileInfo fileInfo,
      Consumer<Long> recycleStream,
      int minBuffersToTriggerRead,
      boolean requireSubpartitionId)
      throws IOException {
    super(
        minReadBuffers,
        maxReadBuffers,
        storageFetcherPool,
        threadsPerMountPoint,
        fileInfo,
        recycleStream,
        minBuffersToTriggerRead);
    this.requireSubpartitionId = requireSubpartitionId;
  }

  @Override
  public void setupDataPartitionReader(
      int startSubIndex, int endSubIndex, long streamId, Channel channel) {
    if (mapFileMeta.getBufferSize() == 0) {
      // If the producer client has revived, the fileMeta.bufferSize has not been initialized
      // (default is 0)
      // In this case, the stream should be recycled immediately, and a BufferStreamEnd should be
      // sent to
      // consumer client to allow it to proceed to the next shuffle data file.
      emptyStreamChannels.put(streamId, channel);
      recycleStream.accept(streamId);
    } else {
      SegmentMapPartitionDataReader mapDataPartitionReader =
          new SegmentMapPartitionDataReader(
              startSubIndex,
              endSubIndex,
              getDiskFileInfo(),
              streamId,
              requireSubpartitionId,
              channel,
              readExecutor,
              () -> recycleStream.accept(streamId));
      logger.debug("[{}] add reader, streamId: {}", this, streamId);
      readers.put(streamId, mapDataPartitionReader);
    }
  }

  @Override
  public synchronized int readBuffers() {
    hasReadingTask.set(false);

    if (isReleased) {
      // some read executor task may already be submitted to the thread pool
      return 0;
    }

    if (!bufferQueue.bufferAvailable()) {
      applyNewBuffers();
      return 0;
    }
    if (!lazyOpenChannels()) {
      return 0;
    }

    int numDataBuffers = 0;
    try {
      PriorityQueue<MapPartitionDataReader> sortedReaders =
          new PriorityQueue<>(
              readers.values().stream()
                  .filter(MapPartitionDataReader::shouldReadData)
                  .collect(Collectors.toList()));
      for (MapPartitionDataReader reader : sortedReaders) {
        if (indexChannel == null) {
          continue;
        }
        boolean hasWriteFinished = getFileMeta().hasWriteFinished();
        indexSize = indexChannel.size();
        reader.open(dataFileChanel, indexChannel, hasWriteFinished, indexSize);
        if (!reader.isOpened()) {
          continue;
        }
        checkState(reader instanceof SegmentMapPartitionDataReader);
        ((SegmentMapPartitionDataReader) reader).updateSegmentId();
      }
      while (bufferQueue.bufferAvailable() && !sortedReaders.isEmpty()) {
        BufferRecycler bufferRecycler = new BufferRecycler(this::recycle);
        MapPartitionDataReader reader = sortedReaders.poll();
        try {
          if (!reader.isOpened() || !reader.canBeRead()) {
            continue;
          }
          numDataBuffers += reader.readData(bufferQueue, bufferRecycler);
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

    releaseExtraBuffers();
    logger.debug("This loop read {} buffers", numDataBuffers);
    return numDataBuffers;
  }

  @Override
  public void triggerRead() {
    // Key for IO schedule.
    if (!isReleased && hasReadingTask.compareAndSet(false, true)) {
      try {
        readExecutor.submit(
            () -> {
              try {
                readAndScheduleNextRead();
              } catch (Throwable throwable) {
                logger.error("Failed to read data.", throwable);
              }
            });
      } catch (RejectedExecutionException e) {
        ignoreRejectedExecutionOnShutdown(e);
      }
    }
  }

  @Override
  public boolean releaseReader(Long streamId) {
    MapPartitionDataReader mapPartitionDataReader = readers.get(streamId);
    if (mapPartitionDataReader == null) {
      // It only occurs when the producer client has revived.
      logger.warn("Release an empty stream {}", streamId);
      checkArgument(
          emptyStreamChannels.containsKey(streamId),
          String.format("Fail to release stream %d, the stream not exist", streamId));
      emptyStreamChannels
          .get(streamId)
          .writeAndFlush(
              new RpcRequest(
                  TransportClient.requestId(),
                  new NioManagedBuffer(
                      new TransportMessage(
                              MessageType.BUFFER_STREAM_END,
                              PbBufferStreamEnd.newBuilder()
                                  .setStreamType(StreamType.CreditStream)
                                  .setStreamId(streamId)
                                  .build()
                                  .toByteArray())
                          .toByteBuffer())));
      return true;
    }

    return super.releaseReader(streamId);
  }

  @Override
  public String toString() {
    return String.format("SegmentMapDataPartition{filePath=%s}", diskFileInfo.getFilePath());
  }

  public void notifyRequiredSegmentId(int segmentId, long streamId, int subPartitionId) {
    MapPartitionDataReader streamReader = getStreamReader(streamId);
    if (streamReader == null) {
      return;
    }
    checkState(streamReader instanceof SegmentMapPartitionDataReader);
    ((SegmentMapPartitionDataReader) streamReader)
        .notifyRequiredSegmentId(segmentId, subPartitionId);
    // After notifying the required segment id, we need to try to send data again.
    readExecutor.submit(
        () -> {
          try {
            streamReader.sendData();
          } catch (Throwable throwable) {
            logger.error("Failed to read data.", throwable);
          }
        });
  }

  private boolean lazyOpenChannels() {
    if (dataFileChanel == null) {
      try {
        dataFileChanel = FileChannelUtils.openReadableFileChannel(diskFileInfo.getFilePath());
      } catch (IOException e) {
        logger.error("Failed to open data file channel: {}", diskFileInfo.getFilePath());
        return false;
      }
    }
    if (indexChannel == null) {
      try {
        indexChannel = FileChannelUtils.openReadableFileChannel(diskFileInfo.getIndexPath());
        indexSize = indexChannel.size();
      } catch (IOException e) {
        logger.error("Failed to open index file channel: {}", diskFileInfo.getIndexPath());
        return false;
      }
    }
    return true;
  }

  private synchronized void readAndScheduleNextRead() {
    if (isReleased) {
      return;
    }
    int numBuffersRead = readBuffers();
    if (numBuffersRead == 0) {
      try {
        nextScheduleReadInterval =
            Math.min(
                nextScheduleReadInterval + SCHEDULE_READ_INIT_INTERVAL_MS,
                SCHEDULE_READ_MAT_INTERVAL_MS);
        readExecutor.schedule(this::triggerRead, nextScheduleReadInterval, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        ignoreRejectedExecutionOnShutdown(e);
      }
    } else {
      nextScheduleReadInterval = SCHEDULE_READ_INIT_INTERVAL_MS;
      triggerRead();
    }
  }

  private void ignoreRejectedExecutionOnShutdown(RejectedExecutionException e) {
    logger.debug(
        "Attempting to submit a task to a shutdown thread pool. No more tasks should be accepted.",
        e);
  }

  private MapFileMeta getFileMeta() {
    return (MapFileMeta) diskFileInfo.getFileMeta();
  }
}
