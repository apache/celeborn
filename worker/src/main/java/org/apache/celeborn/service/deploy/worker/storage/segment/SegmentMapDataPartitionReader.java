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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.util.CheckUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableBuffer;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableSegmentIdBuffer;
import org.apache.celeborn.service.deploy.worker.storage.MapDataPartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.celeborn.common.util.CheckUtils.checkArgument;
import static org.apache.celeborn.common.util.CheckUtils.checkNotNull;
import static org.apache.celeborn.common.util.CheckUtils.checkState;

public class SegmentMapDataPartitionReader extends MapDataPartitionReader {

    public static final Logger logger = LoggerFactory.getLogger(SegmentMapDataPartitionReader.class);

    // This should be the same as the header size in ShuffleClientImpl
    private static final int HEADER_BUFFER_SIZE = 16;

    private static final int END_OF_SEGMENT_INDEX = 7;

    private final int startPartitionIndex;

    private final int endPartitionIndex;

    private final AtomicBoolean isSendingData = new AtomicBoolean(false);

    @GuardedBy("lock")
    private final Deque<Integer> backlogs = new LinkedList<>();

    @GuardedBy("lock")
    private int lastSegmentId = -1;

    @GuardedBy("lock")
    private int requiredSegmentId = -1;

    @GuardedBy("lock")
    private int nextBufferIndex;

    public SegmentMapDataPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            FileInfo fileInfo,
            long streamId,
            Channel associatedChannel,
            ExecutorService readExecutor,
            Runnable recycleStream) {
        super(
                startPartitionIndex,
                endPartitionIndex,
                fileInfo,
                streamId,
                associatedChannel,
                readExecutor,
                recycleStream);
        this.startPartitionIndex = startPartitionIndex;
        this.endPartitionIndex = endPartitionIndex;
    }

    @Override
    public void open(FileChannel dataFileChannel, FileChannel indexFileChannel, long indexSize)
            throws IOException {
        if (this.dataFileChannel == null) {
            this.dataFileChannel = dataFileChannel;
        }
        if (this.indexFileChannel == null) {
            this.indexFileChannel = indexFileChannel;
        }
        // index is (offset,length)
        long indexRegionSize = fileInfo.getNumSubpartitions() * (long) INDEX_ENTRY_SIZE;
        this.numRegions = Utils.checkedDownCast(indexSize / indexRegionSize);

        updateConsumingOffset();
        isOpen = true;
    }

    @Override
    protected void tryNotifyBacklog(int numDataBuffers) {
        // If the hybrid shuffle mode is enabled, the backlog is notified here
        // only when the backlog queue size is less than 1. For other situations,
        // notify the number of backlog when the credit becomes 0.
        if (numDataBuffers > 0 && getNumBacklogs() <= 1) {
            notifyBacklog(numDataBuffers);
        }
    }

    @Override
    protected void tryCloseReader(boolean hasRemaining) {
    }

    @Override
    public synchronized void sendData() {
        isSendingData.set(false);
        RecyclableBuffer buffer;
        while (true) {
            synchronized (lock) {
                if (requiredSegmentId == -1) {
                    logger.debug("The required segment id is not updated, skip sending data.");
                    return;
                }
                buffer = buffersToSend.peek();
                if (buffer instanceof RecyclableSegmentIdBuffer) {
                    RecyclableSegmentIdBuffer recyclableSegmentIdBuffer = (RecyclableSegmentIdBuffer) buffer;
                    int segmentId = recyclableSegmentIdBuffer.getSegmentId();
                    if (segmentId != requiredSegmentId) {
                        // If the queued head buffer is not the required segment id, we do not sent it.
                        logger.debug(
                                "The queued head buffer is not the required segment id, "
                                        + "do not sent it. current: {}, required segment id: {} {}",
                                segmentId,
                                requiredSegmentId,
                                this);
                        break;
                    } else {
                        buffersToSend.poll();
                    }
                }
                buffer = fetchBufferToSend();
            }
            if (buffer == null) {
                break;
            }
            sendDataInternal(buffer);
        }

        boolean shouldRecycle = false;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            if (readFinished && buffersToSend.isEmpty()) {
                shouldRecycle = true;
            }
        }

        if (shouldRecycle) {
            recycle();
        }
    }

    @Override
    protected void addBuffer(ByteBuf buffer, BufferRecycler bufferRecycler) {
        if (buffer == null) {
            return;
        }

        boolean isEndOfSegment = false;
        int subpartitionId = 0;
        checkState(buffer.readableBytes() > HEADER_BUFFER_SIZE, "Wrong buffer size.");
        buffer.markReaderIndex();
        subpartitionId = buffer.readInt();
        isEndOfSegment = isEndOfSegment(buffer, subpartitionId);

        boolean isInQueue;
        synchronized (lock) {
            if (!isReleased) {
                buffersToSend.add(new RecyclableBuffer(buffer, bufferRecycler));
                increaseBacklog();
            } else {
                bufferRecycler.recycle(buffer);
                numInUseBuffers.decrementAndGet();
                throw new RuntimeException("Partition reader has been failed or finished.", errorCause);
            }

            nextBufferIndex++;
            if (isEndOfSegment) {
                updateSegmentId(subpartitionId);
            }
            isInQueue = isSendingData.get();
        }
        if (!isInQueue) {
            triggerSendData();
        }
    }

    @Override
    protected RecyclableBuffer fetchBufferToSend() {
        synchronized (lock) {
            if (isReleased || buffersToSend.isEmpty()) {
                return null;
            }

            if (credits.get() > 0) {
                RecyclableBuffer buffer = buffersToSend.poll();
                checkNotNull(buffer);
                decreaseBacklog();
                return buffer;
            } else {
                int backlog = getBacklog();
                if (backlog > 0) {
                    notifyBacklog(backlog);
                }
                return null;
            }
        }
    }

    private void triggerSendData() {
        if (!isSendingData.get()) {
            readExecutor.submit(this::sendData);
            isSendingData.set(true);
        }
    }

    private boolean isEndOfSegment(ByteBuf buffer, int subpartitionId) {
        boolean isEndOfSegment = false;
        checkState(
                startPartitionIndex <= subpartitionId && endPartitionIndex >= subpartitionId,
                "Wrong subpartition id.");
        if (fileInfo.hasPartitionSegmentIds()) {
            // skip another 3 int fields, the write details are in
            // FlinkShuffleClientImpl#pushDataToLocation
            buffer.skipBytes(3 * 4);
            int dataTypeIndex = buffer.readByte();
            isEndOfSegment = END_OF_SEGMENT_INDEX == dataTypeIndex;
            checkState(dataTypeIndex >= 0 && dataTypeIndex < 8, "Wrong index.");
        }
        buffer.resetReaderIndex();
        return isEndOfSegment;
    }

    private int getBacklog() {
        synchronized (lock) {
            Integer backlog = backlogs.peekFirst();
            while (backlog != null && backlog == 0) {
                backlogs.pollFirst();
                backlog = backlogs.peekFirst();
            }
            return backlog == null ? 0 : backlog;
        }
    }

    private int getNumBacklogs() {
        synchronized (lock) {
            return backlogs.size();
        }
    }

    @GuardedBy("lock")
    private void addNewBacklog() {
        backlogs.addLast(0);
    }

    @GuardedBy("lock")
    private void increaseBacklog() {
        Integer backlog = backlogs.pollLast();
        if (backlog == null) {
            backlogs.addLast(1);
        } else {
            backlogs.addLast(backlog + 1);
        }
    }

    @GuardedBy("lock")
    private void decreaseBacklog() {
        int backlog = checkNotNull(backlogs.pollFirst());
        CheckUtils.checkState(backlog > 0, "Wrong number of backlogs.");
        if (backlog > 1) {
            backlogs.addFirst(backlog - 1);
        }
    }

    private void updateSegmentId(int subpartitionId) {
        checkArgument(
                startPartitionIndex <= subpartitionId && subpartitionId <= endPartitionIndex,
                "Wrong subpartition id.");
        synchronized (lock) {
            // Note that only when writing buffers, it has the segment info, when loading buffers from
            // disk,
            // we do not know when the segment is started, so we try to get the segment id from the buffer
            // index here.
            Integer segmentId = fileInfo.getSegmentIdByFirstBufferIndex(subpartitionId, nextBufferIndex);
            if (segmentId == null) {
                return;
            }
            if (segmentId != -1) {
                // For the continuous segments, we use the same backlog
                if (segmentId == 0 || segmentId != lastSegmentId && segmentId != (lastSegmentId + 1)) {
                    addNewBacklog();
                }
                lastSegmentId = segmentId;
            }
            logger.debug(
                    "Insert a buffer to indicate the current segment id "
                            + "subpartitionId={}, segmentId={} for {}.",
                    subpartitionId,
                    segmentId,
                    this);
            // Before adding buffers in this segment, add a new buffer to indicate the segment id. So this
            // buffer
            // is in the head of this segment.
            buffersToSend.add(new RecyclableSegmentIdBuffer(segmentId));
        }
    }

    public void notifyRequiredSegmentId(int segmentId) {
        synchronized (lock) {
            this.requiredSegmentId = segmentId;
            logger.debug("Update the required segment id to {}, {}", requiredSegmentId, this);
        }
    }

    protected void updateSegmentId() {
        synchronized (lock) {
            for (int i = startPartitionIndex; i <= endPartitionIndex; i++) {
                if (requiredSegmentId < 0) {
                    updateSegmentId(i);
                }
            }
        }
    }
}
