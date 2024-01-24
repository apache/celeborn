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

import io.netty.channel.Channel;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.storage.MapDataPartition;
import org.apache.celeborn.service.deploy.worker.storage.MapDataPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.commons.crypto.utils.Utils.checkState;

public class SegmentMapDataPartition extends MapDataPartition {

    public static final Logger logger = LoggerFactory.getLogger(SegmentMapDataPartition.class);

    public SegmentMapDataPartition(
            int minReadBuffers,
            int maxReadBuffers,
            HashMap<String, ExecutorService> storageFetcherPool,
            int threadsPerMountPoint,
            FileInfo fileInfo,
            Consumer<Long> recycleStream,
            int minBuffersToTriggerRead)
            throws IOException {
        super(
                minReadBuffers,
                maxReadBuffers,
                storageFetcherPool,
                threadsPerMountPoint,
                fileInfo,
                recycleStream,
                minBuffersToTriggerRead);
    }

    @Override
    public ExecutorService getReadExecutor(
            HashMap<String, ExecutorService> storageFetcherPool,
            int threadsPerMountPoint,
            FileInfo fileInfo) {
       return ThreadUtils.newDaemonSingleThreadScheduledExecutor(
                fileInfo.getMountPoint() + "-reader-thread");
    }

    @Override
    public void setupDataPartitionReader(
            int startSubIndex, int endSubIndex, long streamId, Channel channel) {
        SegmentMapDataPartitionReader mapDataPartitionReader =
                new SegmentMapDataPartitionReader(
                        startSubIndex,
                        endSubIndex,
                        fileInfo,
                        streamId,
                        channel,
                        readExecutor,
                        () -> recycleStream.accept(streamId));
        readers.put(streamId, mapDataPartitionReader);
    }


    @Override
    public synchronized int readBuffers() {
        hasReadingTask.set(false);
        if (isReleased) {
            // some read executor task may already be submitted to the thread pool
            return 0;
        }

        int numBuffersAllocated;
        if (bufferQueue.size() == 0) {
            applyNewBuffers();
        }
        numBuffersAllocated = bufferQueue.size();
        if (numBuffersAllocated == 0) {
            return 0;
        }

        try {
            PriorityQueue<MapDataPartitionReader> sortedReaders =
                    new PriorityQueue<>(
                            readers.values().stream()
                                    .filter(MapDataPartitionReader::shouldReadData)
                                    .collect(Collectors.toList()));
            for (MapDataPartitionReader reader : sortedReaders) {
                indexSize = indexChannel.size();
                reader.open(dataFileChanel, indexChannel, indexSize);
                checkState(reader instanceof SegmentMapDataPartitionReader);
                ((SegmentMapDataPartitionReader) reader).updateSegmentId();
            }
            while (bufferQueue.bufferAvailable() && !sortedReaders.isEmpty()) {
                BufferRecycler bufferRecycler = new BufferRecycler(this::recycle);
                MapDataPartitionReader reader = sortedReaders.poll();
                try {
                    reader.readData(bufferQueue, bufferRecycler);
                } catch (Throwable e) {
                    logger.error("reader exception, reader: {}, message: {}", reader, e.getMessage(), e);
                    reader.recycleOnError(e);
                }
            }
        } catch (Throwable e) {
            logger.error("Fatal: failed to read partition data. {}", e.getMessage(), e);
            for (MapDataPartitionReader reader : readers.values()) {
                reader.recycleOnError(e);
            }
        }
        return getNumReadBuffers(numBuffersAllocated);
    }

    @Override
    public void triggerRead() {
        // Key for IO schedule.
        if (hasReadingTask.compareAndSet(false, true)) {
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
        return super.releaseReader(streamId);
    }

    @Override
    public void close() {
        super.close();
        readExecutor.shutdownNow();
    }

    public void notifyRequiredSegmentId(int segmentId, long streamId) {
        MapDataPartitionReader streamReader = getStreamReader(streamId);
        if (streamReader == null) {
            return;
        }
        checkState(streamReader instanceof SegmentMapDataPartitionReader);
        ((SegmentMapDataPartitionReader) streamReader).notifyRequiredSegmentId(segmentId);
        // After notifying the required segment id, we need to try to send data again.
        readExecutor.submit(streamReader::sendData);
    }

    private synchronized void readAndScheduleNextRead() {
        checkState(readExecutor instanceof ScheduledExecutorService);
        int numBuffersRead = readBuffers();
        if (numBuffersRead == 0) {
            try {
                ((ScheduledExecutorService) readExecutor).schedule(this::triggerRead, 5, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                ignoreRejectedExecutionOnShutdown(e);
            }
        } else {
            triggerRead();
        }
    }


    private int getNumReadBuffers(int numBuffersAllocated) {
        int numReadBuffers = numBuffersAllocated - bufferQueue.size();
        releaseBuffers();
        logger.debug("This loop read {} buffers", numReadBuffers);
        return numReadBuffers;
    }

    private void ignoreRejectedExecutionOnShutdown(RejectedExecutionException e) {
        logger.debug(
                "Attempting to submit a task to a shutdown thread pool. No more tasks should be accepted.",
                e);
    }
}
