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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;

import scala.Tuple2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.FileCorruptedException;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.network.protocol.BufferStreamEnd;
import org.apache.celeborn.common.network.protocol.SubPartitionReadData;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.BufferQueue;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableBuffer;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableSegmentIdBuffer;
import org.apache.celeborn.service.deploy.worker.storage.MapPartitionDataReader;

public class SegmentMapPartitionDataReader extends MapPartitionDataReader {

  public static final Logger logger = LoggerFactory.getLogger(SegmentMapPartitionDataReader.class);

  // This should be the same as the header size in ShuffleClientImpl
  // subpartitionid(4) + attemptId(4) + nextBatchId(4) + compressedsize
  private static final int HEADER_BUFFER_SIZE = 16;

  // dataType(1) + isCompress(1) + size(4)
  private static final int SMALL_HEADER_BUFFER_SIZE = 1 + 1 + 4;

  private static final int END_OF_SEGMENT_INDEX = 7;

  private final int startPartitionIndex;

  private final int endPartitionIndex;

  private final boolean requireSubpartitionId;

  private final AtomicBoolean isSendingData = new AtomicBoolean(false);

  @GuardedBy("lock")
  private final Deque<Integer> backlogs = new LinkedList<>();

  @GuardedBy("lock")
  private Map<Integer, Integer> subPartitionLastSegmentIds = new HashMap<>();

  @GuardedBy("lock")
  private Map<Integer, Integer> subPartitionRequiredSegmentIds = new HashMap<>();

  @GuardedBy("lock")
  private boolean hasUpdateSegmentId = false;

  @GuardedBy("lock")
  private Map<Integer, Integer> subPartitionNextBufferIndex = new HashMap<>();

  private boolean canBeRead = true;

  public SegmentMapPartitionDataReader(
      int startPartitionIndex,
      int endPartitionIndex,
      DiskFileInfo fileInfo,
      long streamId,
      boolean requireSubpartitionId,
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
    this.requireSubpartitionId = requireSubpartitionId;

    for (int i = startPartitionIndex; i <= endPartitionIndex; i++) {
      subPartitionLastSegmentIds.put(i, -1);
      subPartitionRequiredSegmentIds.put(i, -1);
      subPartitionNextBufferIndex.put(i, 0);
    }
  }

  @Override
  public void open(
      FileChannel dataFileChannel,
      FileChannel indexFileChannel,
      boolean hasWriteFinished,
      long indexSize)
      throws IOException {
    if (this.dataFileChannel == null) {
      this.dataFileChannel = dataFileChannel;
    }
    if (this.indexFileChannel == null) {
      this.indexFileChannel = indexFileChannel;
    }
    // index is (offset,length)
    long indexRegionSize = mapFileMeta.getNumSubpartitions() * (long) INDEX_ENTRY_SIZE;
    if (indexRegionSize == 0) {
      return;
    }
    this.numRegions = Utils.checkedDownCast(indexSize / indexRegionSize);

    updateConsumingOffset();
    isOpen = true;
  }

  @Override
  public int readData(BufferQueue bufferQueue, BufferRecycler bufferRecycler) throws IOException {
    if (!isOpen) {
      return 0;
    }
    return super.readData(bufferQueue, bufferRecycler);
  }

  @Override
  protected Tuple2<Integer, ByteBuf> readBuffer(
      String filename, FileChannel channel, ByteBuffer header, ByteBuf buffer, int headerSize)
      throws IOException {
    readHeaderOrIndexBuffer(channel, header, headerSize);
    // header is combined of mapId(4),attemptId(4),nextBatchId(4) and total Compressed Length(4)
    // we need size here,so we read length directly
    int bufferLength = header.getInt(12);
    // the actual data buffer size is 32KB, and the prefix header buffer size is 16 + 6
    if (bufferLength <= 0 || bufferLength > buffer.capacity() + SMALL_HEADER_BUFFER_SIZE) {
      logger.error("Incorrect buffer header, buffer length: {}.", bufferLength);
      throw new FileCorruptedException("File " + filename + " is corrupted");
    } else if (bufferLength <= buffer.capacity() - HEADER_BUFFER_SIZE) {
      buffer.writeBytes(header);
      readBufferIntoReadBuffer(channel, buffer, bufferLength);
      return Tuple2.apply(bufferLength + headerSize, buffer);
    } else {
      // When flink CelebornTierProducerClient receive an large buffer(e.g., 32KB), it will write a
      // buffer to celeborn worker, which large than 32KB (specifically, 32KB + 16B + 6B)
      // In this case, it should create an additional temporary buffer to store the extra bytes
      // beyond 32KB

      // We simplify this by creating an additional buffer to store BIG HEADER and SMALL HEADER,
      // while the original buffer is used to store the actual data.
      ByteBuffer additionalBuffer =
          ByteBuffer.allocate(HEADER_BUFFER_SIZE + SMALL_HEADER_BUFFER_SIZE);
      // put BIG HEADER and SMALL HEADER into additionalBuffer
      additionalBuffer.put(header);
      channel.read(additionalBuffer);
      additionalBuffer.flip();
      // read actual data buffer
      readBufferIntoReadBuffer(channel, buffer, bufferLength - SMALL_HEADER_BUFFER_SIZE);
      CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
      compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(additionalBuffer));
      compositeByteBuf.addComponent(true, buffer);
      compositeByteBuf.retain();
      return Tuple2.apply(bufferLength + headerSize, compositeByteBuf);
    }
  }

  @Override
  protected void tryNotifyBacklog(int numDataBuffers) {
    notifyBacklog(getBacklog());
    isSendingData.set(true);
    sendData();
  }

  @Override
  protected void tryCloseReader(boolean hasRemaining) throws IOException {
    synchronized (lock) {
      if (!mapFileMeta.hasWriteFinished()) {
        return;
      }
      if (indexFileChannel == null) {
        return;
      }
      long indexRegionSize = mapFileMeta.getNumSubpartitions() * (long) INDEX_ENTRY_SIZE;
      if (indexRegionSize == 0) {
        return;
      }
      updateConsumeOffsetIfNeeded(indexRegionSize);
      if (!hasRemaining() && canBeRead && buffersToSend.isEmpty()) {
        closeReader();
        readExecutor.submit(this::tryRecycleReader);
        notifyBufferEndStream();
      }
    }
  }

  @Override
  public synchronized void sendData() {
    RecyclableBuffer buffer;
    while (true) {
      synchronized (lock) {
        if (!hasUpdateSegmentId) {
          logger.debug(
              "The required segment id is not updated for {}, skip sending data.", getStreamId());
          isSendingData.set(false);
          return;
        }

        boolean breakLoop = false;
        while ((buffer = buffersToSend.peek()) instanceof RecyclableSegmentIdBuffer) {
          RecyclableSegmentIdBuffer recyclableSegmentIdBuffer = (RecyclableSegmentIdBuffer) buffer;
          int subPartitionId = recyclableSegmentIdBuffer.getSubPartitionId();
          int segmentId = recyclableSegmentIdBuffer.getSegmentId();
          if (segmentId != subPartitionRequiredSegmentIds.get(subPartitionId)) {
            // If the queued head buffer is not the required segment id, we do not sent it.
            logger.debug(
                "The queued head buffer is not the required segment id, "
                    + "do not sent it. subPartitionId: {}, current: {}, required segment id: {} {}",
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

    isSendingData.set(false);
  }

  public void tryRecycleReader() {
    boolean shouldRecycle = false;
    synchronized (lock) {
      if (isReleased) {
        return;
      }
      if (readFinished && buffersToSend.isEmpty()) {
        logger.debug("recycle the reader, {}", streamId);
        shouldRecycle = true;
      }
    }

    if (shouldRecycle) {
      recycle();
    }
  }

  @Override
  protected void sendDataInternal(RecyclableBuffer buffer) {
    if (!requireSubpartitionId) {
      super.sendDataInternal(buffer);
      return;
    }
    final RecyclableBuffer wrappedBuffer = buffer;
    int readableBytes = wrappedBuffer.byteBuf.readableBytes();
    if (logger.isDebugEnabled()) {
      logger.debug("send data start: {}, {}, {}", streamId, readableBytes, getNumBuffersToSend());
    }
    SubPartitionReadData readData =
        new SubPartitionReadData(streamId, wrappedBuffer.subPartitionId, wrappedBuffer.byteBuf);
    associatedChannel
        .writeAndFlush(readData)
        .addListener(
            (ChannelFutureListener)
                future -> {
                  try {
                    if (!future.isSuccess()) {
                      recycleOnError(future.cause());
                    }
                  } finally {
                    logger.debug("send data end: {}, {}", streamId, readableBytes);
                    wrappedBuffer.recycle();
                    numInUseBuffers.decrementAndGet();
                  }
                });

    int currentCredit = credits.decrementAndGet();
    logger.debug("stream {} credit {}", streamId, currentCredit);
  }

  @Override
  protected void addBuffer(ByteBuf buffer, BufferRecycler bufferRecycler) {
    if (buffer == null) {
      return;
    }

    if (buffer.readableBytes() <= HEADER_BUFFER_SIZE) {
      logger.error("Wrong buffer size {}", buffer.readableBytes());
      throw new RuntimeException("Wrong buffer size " + buffer.readableBytes());
    }
    buffer.markReaderIndex();
    int subPartitionId = buffer.readInt();
    boolean isEndOfSegment = isEndOfSegment(buffer, subPartitionId);

    boolean isInQueue;
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
      isInQueue = isSendingData.get();
    }
    if (!isInQueue) {
      triggerSendData();
    }
  }

  @Override
  public boolean canBeRead() {
    return canBeRead;
  }

  @Override
  protected void updateConsumingOffset() throws IOException {
    while (currentPartitionRemainingBytes == 0
        && (currentDataRegion < numRegions - 1 || numRemainingPartitions > 0)) {
      if (numRemainingPartitions <= 0) {
        ++currentDataRegion;
        numRemainingPartitions = endPartitionIndex - startPartitionIndex + 1;

        // read the target index entry to the target index buffer
        indexFileChannel.position(
            currentDataRegion * getIndexRegionSize()
                + (long) startPartitionIndex * INDEX_ENTRY_SIZE);
        readHeaderOrIndexBuffer(indexFileChannel, indexBuffer, indexBuffer.capacity());
      }

      // get the data file offset and the data size
      dataConsumingOffset = indexBuffer.getLong();
      currentPartitionRemainingBytes = indexBuffer.getLong();
      --numRemainingPartitions;

      logger.debug(
          "readBuffer updateConsumingOffset, {},  {}, {}, {}",
          streamId,
          dataFileChannel.size(),
          dataConsumingOffset,
          currentPartitionRemainingBytes);

      // if these checks fail, the partition file must be corrupted
      if (dataConsumingOffset < 0
          || dataConsumingOffset + currentPartitionRemainingBytes > dataFileChannel.size()
          || currentPartitionRemainingBytes < 0) {
        canBeRead = false;
        return;
      }
    }
    canBeRead =
        dataConsumingOffset >= 0
            && dataConsumingOffset + currentPartitionRemainingBytes <= dataFileChannel.size()
            && currentPartitionRemainingBytes >= 0;
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
  public boolean hasRemaining() {
    return currentPartitionRemainingBytes > 0
        || currentDataRegion < numRegions - 1
        || numRemainingPartitions > 0;
  }

  @Override
  protected void notifyBacklog(int backlog) {
    if (backlog == 0) {
      return;
    }
    super.notifyBacklog(backlog);
  }

  private void updateConsumeOffsetIfNeeded(long indexRegionSize) throws IOException {
    int currentNumRegions = Utils.checkedDownCast(indexFileChannel.size() / indexRegionSize);
    if (currentNumRegions != numRegions) {
      numRegions = currentNumRegions;
      updateConsumingOffset();
    }
  }

  private void triggerSendData() {
    if (!isSendingData.get()) {
      readExecutor.submit(
          () -> {
            try {
              sendData();
            } catch (Throwable throwable) {
              logger.error("Failed to send data", throwable);
            }
          });
      isSendingData.set(true);
    }
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
      int dataTypeIndex = buffer.readByte();
      isEndOfSegment = END_OF_SEGMENT_INDEX == dataTypeIndex;
    }
    buffer.resetReaderIndex();
    return isEndOfSegment;
  }

  private void notifyBufferEndStream() {
    logger.debug("stream manager notify stream id end {}", streamId);
    associatedChannel
        .writeAndFlush(new BufferStreamEnd(streamId))
        .addListener(
            future -> {
              if (!future.isSuccess()) {
                logger.error("notify stream end to stream {} failed", streamId);
              }
            });
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
      this.subPartitionRequiredSegmentIds.put(subPartitionId, segmentId);
      logger.debug(
          "Update the required segment id to {}, {}, subPartitionId: {}",
          segmentId,
          this,
          subPartitionId);
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
