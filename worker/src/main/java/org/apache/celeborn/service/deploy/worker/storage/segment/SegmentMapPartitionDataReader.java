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

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.annotation.concurrent.GuardedBy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableBuffer;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableSegmentIdBuffer;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataReader;

public class SegmentMapPartitionDataReader extends MapPartitionDataReader {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionDataReader.class);

  // The Flink EndOfSegment buffer data type, which has been written
  // to the buffer header, is in the 16th bit(starting from 0).
  private static final int END_OF_SEGMENT_BUFFER_DATA_TYPE = 7;

  private final int startPartitionIndex;

  private final int endPartitionIndex;

  @GuardedBy("lock")
  private final Deque<Integer> backlogs = new LinkedList<>();

  @GuardedBy("lock")
  // subpartitionId -> segmentId, current required segmentId by client per subpartition
  private Map<Integer, Integer> subPartitionRequiredSegmentIds = new HashMap<>();

  @GuardedBy("lock")
  private boolean hasUpdateSegmentId = false;

  @GuardedBy("lock")
  // subpartitionId -> segmentId, segmentId of current reading buffer per subpartition
  private Map<Integer, Integer> subPartitionLastSegmentIds = new HashMap<>();

  @GuardedBy("lock")
  // subpartitionId -> buffer index, current reading buffer index per subpartition
  private Map<Integer, Integer> subPartitionNextBufferIndex = new HashMap<>();

  public SegmentMapPartitionDataReader(
      int startPartitionIndex,
      int endPartitionIndex,
      DiskFileInfo fileInfo,
      long streamId,
      Channel associatedChannel,
      Runnable recycleStream,
      boolean requireSubpartitionId) {
    super(
        startPartitionIndex,
        endPartitionIndex,
        fileInfo,
        streamId,
        associatedChannel,
        recycleStream,
        requireSubpartitionId);
    this.startPartitionIndex = startPartitionIndex;
    this.endPartitionIndex = endPartitionIndex;

    for (int i = startPartitionIndex; i <= endPartitionIndex; i++) {
      subPartitionLastSegmentIds.put(i, -1);
      subPartitionRequiredSegmentIds.put(i, -1);
      subPartitionNextBufferIndex.put(i, 0);
    }
  }

  @Override
  protected void tryNotifyBacklog(int numDataBuffers) {
    notifyBacklog(getBacklog());
  }

  @Override
  public synchronized void sendData() {
    while (true) {
      synchronized (lock) {
        if (!hasUpdateSegmentId) {
          logger.debug(
              "The required segment id is not updated for {}, skip sending data.", getStreamId());
          return;
        }

        RecyclableBuffer buffer;

        // Verify if the client requires the segmentId of the first buffer; if so, send it to the
        // worker; otherwise, wait until the client sends a new segmentId.
        boolean breakLoop = false;
        while ((buffer = buffersToSend.peek()) instanceof RecyclableSegmentIdBuffer) {
          RecyclableSegmentIdBuffer recyclableSegmentIdBuffer = (RecyclableSegmentIdBuffer) buffer;
          int subPartitionId = recyclableSegmentIdBuffer.getSubPartitionId();
          int segmentId = recyclableSegmentIdBuffer.getSegmentId();
          if (segmentId != subPartitionRequiredSegmentIds.get(subPartitionId)) {
            // If the queued head buffer is not the required segment id, we do not sent it.
            logger.info(
                "The queued head buffer is not the required segment id, "
                    + "do not sent it. details: streamId {}, subPartitionId: {}, current segment id: {}, required segment id: {}, reader: {}",
                streamId,
                subPartitionId,
                segmentId,
                subPartitionRequiredSegmentIds.get(subPartitionId),
                this);
            breakLoop = true;
            break;
          } else {
            buffersToSend.poll();
          }
        }
        if (breakLoop) {
          break;
        }

        // fetch first buffer and send
        buffer = fetchBufferToSend();
        if (buffer instanceof RecyclableSegmentIdBuffer) {
          logger.error("Wrong type of buffer. ");
          throw new RuntimeException("Wrong type of buffer.");
        }
        if (buffer == null) {
          break;
        }
        sendDataInternal(buffer);
      }
    }

    tryRecycleReader();
  }

  @Override
  protected void addBuffer(ByteBuf buffer, BufferRecycler bufferRecycler) {
    if (buffer == null) {
      return;
    }

    buffer.markReaderIndex();
    int subPartitionId = buffer.readInt();
    // check the buffer type
    boolean isEndOfSegment = isEndOfSegment(buffer, subPartitionId);
    buffer.resetReaderIndex();

    synchronized (lock) {
      if (!isReleased) {
        buffersToSend.add(new RecyclableBuffer(buffer, subPartitionId, bufferRecycler));
        increaseBacklog();
      } else {
        bufferRecycler.recycle(buffer);
        numInUseBuffers.decrementAndGet();
        throw new RuntimeException("Partition reader has been failed or finished.", errorCause);
      }

      subPartitionNextBufferIndex.compute(subPartitionId, (k, v) -> v + 1);
      if (isEndOfSegment) {
        updateSegmentId(subPartitionId);
      }
    }
  }

  @Override
  protected RecyclableBuffer fetchBufferToSend() {
    synchronized (lock) {
      if (isReleased || buffersToSend.isEmpty()) {
        return null;
      }

      RecyclableBuffer buffer = null;
      int numCredit = credits.get();
      if (numCredit > 0) {
        buffer = buffersToSend.poll();
      }
      if (numCredit <= 1) {
        notifyBacklog(getBacklog());
      }
      return buffer;
    }
  }

  @Override
  protected void notifyBacklog(int backlog) {
    if (backlog == 0) {
      return;
    }
    super.notifyBacklog(backlog);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SegmentMapDataPartitionReader{");
    sb.append("startPartitionIndex=").append(startPartitionIndex);
    sb.append(", endPartitionIndex=").append(endPartitionIndex);
    sb.append(", streamId=").append(streamId);
    sb.append('}');
    return sb.toString();
  }

  private boolean isEndOfSegment(ByteBuf buffer, int subPartitionId) {
    boolean isEndOfSegment = false;
    if (subPartitionId < startPartitionIndex || subPartitionId > endPartitionIndex) {
      logger.error("Wrong sub partition id.");
      throw new IllegalArgumentException("Wrong sub partition id.");
    }
    if (mapFileMeta.hasPartitionSegmentIds()) {
      // skip another 3 int fields, the write details are in
      // FlinkShuffleClientImpl#pushDataToLocation
      buffer.skipBytes(3 * 4);
      int dataType = buffer.readByte();
      isEndOfSegment = END_OF_SEGMENT_BUFFER_DATA_TYPE == dataType;
    }
    return isEndOfSegment;
  }

  private int getBacklog() {
    synchronized (lock) {
      Integer backlog = backlogs.peekFirst();
      while (backlog != null && backlog == 0) {
        backlogs.pollFirst();
        backlog = backlogs.peekFirst();
      }
      if (backlog != null) {
        backlogs.pollFirst();
      }
      return backlog == null ? 0 : backlog;
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

  private void updateSegmentId(int subPartitionId) {
    synchronized (lock) {
      // Note that only when writing buffers, it has the segment info, when loading buffers from
      // disk,
      // we do not know when the segment is started, so we try to get the segment id from the buffer
      // index here.
      Integer segmentId =
          mapFileMeta.getSegmentIdByFirstBufferIndex(
              subPartitionId, subPartitionNextBufferIndex.get(subPartitionId));
      if (segmentId == null) {
        return;
      }
      if (segmentId != -1) {
        // For the continuous segments, we use the same backlog
        if (segmentId == 0
            || !segmentId.equals(subPartitionLastSegmentIds.get(subPartitionId))
                && segmentId != (subPartitionLastSegmentIds.get(subPartitionId) + 1)) {
          addNewBacklog();
        }
        subPartitionLastSegmentIds.put(subPartitionId, segmentId);
      }
      logger.debug(
          "Insert a buffer to indicate the current segment id "
              + "subPartitionId={}, segmentId={} for {}.",
          subPartitionId,
          segmentId,
          this);
      // Before adding buffers in this segment, add a new buffer to indicate the segment id. So this
      // buffer
      // is in the head of this segment.
      buffersToSend.add(new RecyclableSegmentIdBuffer(subPartitionId, segmentId));
    }
  }

  public void notifyRequiredSegmentId(int segmentId, int subPartitionId) {
    synchronized (lock) {
      logger.debug(
          "Update the required segment id to {}, {}, subPartitionId: {}",
          segmentId,
          this,
          subPartitionId);
      this.subPartitionRequiredSegmentIds.put(subPartitionId, segmentId);
    }
  }

  protected void updateSegmentId() {
    synchronized (lock) {
      hasUpdateSegmentId = true;
      for (int i = startPartitionIndex; i <= endPartitionIndex; i++) {
        if (subPartitionLastSegmentIds.get(i) < 0) {
          updateSegmentId(i);
        }
      }
    }
  }
}
