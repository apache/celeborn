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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.exception.FileCorruptedException;
import org.apache.celeborn.common.meta.DiskFileInfo;
import org.apache.celeborn.common.meta.FileInfo;
import org.apache.celeborn.common.meta.MapFileMeta;
import org.apache.celeborn.common.network.buffer.NioManagedBuffer;
import org.apache.celeborn.common.network.client.TransportClient;
import org.apache.celeborn.common.network.protocol.BacklogAnnouncement;
import org.apache.celeborn.common.network.protocol.ReadData;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.RpcRequest;
import org.apache.celeborn.common.network.protocol.TransportMessage;
import org.apache.celeborn.common.network.protocol.TransportableError;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.protocol.MessageType;
import org.apache.celeborn.common.protocol.PbBufferStreamEnd;
import org.apache.celeborn.common.protocol.StreamType;
import org.apache.celeborn.common.util.ExceptionUtils;
import org.apache.celeborn.common.util.Utils;
import org.apache.celeborn.service.deploy.worker.memory.BufferQueue;
import org.apache.celeborn.service.deploy.worker.memory.BufferRecycler;
import org.apache.celeborn.service.deploy.worker.memory.RecyclableBuffer;

public class MapPartitionDataReader implements Comparable<MapPartitionDataReader> {
  private static final Logger logger = LoggerFactory.getLogger(MapPartitionDataReader.class);

  private final ByteBuffer indexBuffer;
  private final ByteBuffer headerBuffer;
  private final int startPartitionIndex;
  private final int endPartitionIndex;
  private int numRegions;
  private int numRemainingPartitions;
  private int currentDataRegion = -1;
  private long dataConsumingOffset;
  private volatile long currentPartitionRemainingBytes;
  private DiskFileInfo fileInfo;
  protected MapFileMeta mapFileMeta;
  private int INDEX_ENTRY_SIZE = 16;
  protected long streamId;
  protected final Object lock = new Object();

  protected final AtomicInteger credits = new AtomicInteger();

  @GuardedBy("lock")
  protected final Queue<RecyclableBuffer> buffersToSend = new ArrayDeque<>();

  /** Whether all the data has been successfully read or not. */
  @GuardedBy("lock")
  private volatile boolean readFinished;

  /** Whether this partition reader has been released or not. */
  @GuardedBy("lock")
  protected boolean isReleased;

  /** Exception causing the release of this partition reader. */
  @GuardedBy("lock")
  protected Throwable errorCause;

  /** Whether there is any error at the consumer side or not. */
  @GuardedBy("lock")
  protected boolean errorNotified;

  private Channel associatedChannel;

  private Runnable recycleStream;

  protected AtomicInteger numInUseBuffers = new AtomicInteger(0);
  private boolean isOpen = false;

  private PartitionDataReader partitionDataReader;

  public MapPartitionDataReader(
      int startPartitionIndex,
      int endPartitionIndex,
      DiskFileInfo fileInfo,
      long streamId,
      Channel associatedChannel,
      Runnable recycleStream) {
    this.startPartitionIndex = startPartitionIndex;
    this.endPartitionIndex = endPartitionIndex;

    int indexBufferSize = 16 * (endPartitionIndex - startPartitionIndex + 1);
    this.indexBuffer = ByteBuffer.allocateDirect(indexBufferSize);

    this.headerBuffer = ByteBuffer.allocateDirect(16);
    this.streamId = streamId;
    this.associatedChannel = associatedChannel;
    this.recycleStream = recycleStream;

    this.fileInfo = fileInfo;
    this.mapFileMeta = ((MapFileMeta) fileInfo.getFileMeta());
    this.readFinished = false;
  }

  public void open(
      FileChannel dataFileChannel,
      FileChannel indexFileChannel,
      FSDataInputStream dataInputStream,
      FSDataInputStream indexInputStream)
      throws IOException {
    if (!isOpen) {
      this.partitionDataReader =
          fileInfo.isDFS()
              ? new DfsPartitionDataReader(
                  fileInfo, dataInputStream, indexInputStream, headerBuffer, indexBuffer)
              : new LocalPartitionDataReader(
                  fileInfo, dataFileChannel, indexFileChannel, headerBuffer, indexBuffer);
      // index is (offset,length)
      long indexRegionSize = mapFileMeta.getNumSubpartitions() * (long) INDEX_ENTRY_SIZE;
      this.numRegions =
          Utils.checkedDownCast(partitionDataReader.getIndexFileSize() / indexRegionSize);

      updateConsumingOffset();
      isOpen = true;
    }
  }

  public void addCredit(int credit) {
    credits.getAndAdd(credit);
  }

  public void readData(BufferQueue bufferQueue, BufferRecycler bufferRecycler) throws IOException {
    boolean hasRemaining = hasRemaining();
    boolean continueReading = hasRemaining;
    int numDataBuffers = 0;
    while (continueReading) {

      ByteBuf buffer = bufferQueue.poll();
      if (buffer == null) {
        // if there are no buffers available, halt current read and waiting for next triggered read
        break;
      } else {
        buffer.retain();
        numInUseBuffers.incrementAndGet();
      }

      try {
        continueReading = readBuffer(buffer);
      } catch (Throwable throwable) {
        bufferRecycler.recycle(buffer);
        numInUseBuffers.decrementAndGet();
        throw throwable;
      }

      hasRemaining = hasRemaining();
      addBuffer(buffer, bufferRecycler);
      ++numDataBuffers;
    }

    if (!hasRemaining) {
      closeReader();
    }

    tryNotifyBacklog(numDataBuffers);
  }

  protected void tryNotifyBacklog(int numDataBuffers) {
    if (numDataBuffers > 0) {
      notifyBacklog(numDataBuffers);
    }
  }

  protected void addBuffer(ByteBuf buffer, BufferRecycler bufferRecycler) {
    if (buffer == null) {
      return;
    }
    synchronized (lock) {
      if (!isReleased) {
        buffersToSend.add(new RecyclableBuffer(buffer, -1, bufferRecycler));
      } else {
        bufferRecycler.recycle(buffer);
        numInUseBuffers.decrementAndGet();
        throw new RuntimeException("Partition reader has been failed or finished.", errorCause);
      }
    }
  }

  protected RecyclableBuffer fetchBufferToSend() {
    synchronized (lock) {
      if (!buffersToSend.isEmpty() && credits.get() > 0 && !isReleased) {
        return buffersToSend.poll();
      } else {
        return null;
      }
    }
  }

  protected int getNumBuffersToSend() {
    synchronized (lock) {
      return buffersToSend.size();
    }
  }

  public synchronized void sendData() {
    RecyclableBuffer buffer;
    while (null != (buffer = fetchBufferToSend())) {
      sendDataInternal(buffer);
    }

    tryRecycleReader();
  }

  public void tryRecycleReader() {
    boolean shouldRecycle = false;
    synchronized (lock) {
      if (isReleased) return;
      if (readFinished && buffersToSend.isEmpty()) {
        shouldRecycle = true;
      }
    }

    if (shouldRecycle) {
      recycle();
    }
  }

  public RequestMessage generateReadDataMessage(
      long streamId, int subPartitionId, ByteBuf byteBuf) {
    return new ReadData(streamId, byteBuf);
  }

  protected void sendDataInternal(RecyclableBuffer buffer) {
    final RecyclableBuffer wrappedBuffer = buffer;
    int readableBytes = wrappedBuffer.byteBuf.readableBytes();
    if (logger.isDebugEnabled()) {
      logger.debug("send data start: {}, {}", streamId, readableBytes);
    }

    RequestMessage readData =
        generateReadDataMessage(streamId, wrappedBuffer.subPartitionId, wrappedBuffer.byteBuf);
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
                    if (logger.isDebugEnabled()) {
                      logger.debug("send data end: {}, {}", streamId, readableBytes);
                    }
                    wrappedBuffer.recycle();
                    numInUseBuffers.decrementAndGet();
                  }
                });
    int currentCredit = credits.decrementAndGet();
    logger.debug("Current credit is {} after stream {}", currentCredit, streamId);
  }

  protected long getIndexRegionSize() {
    return mapFileMeta.getNumSubpartitions() * (long) INDEX_ENTRY_SIZE;
  }

  protected void updateConsumingOffset() throws IOException {
    while (currentPartitionRemainingBytes == 0
        && (currentDataRegion < numRegions - 1 || numRemainingPartitions > 0)) {
      if (numRemainingPartitions <= 0) {
        ++currentDataRegion;
        numRemainingPartitions = endPartitionIndex - startPartitionIndex + 1;

        // read the target index entry to the target index buffer
        long targetPosition =
            currentDataRegion * getIndexRegionSize()
                + (long) startPartitionIndex * INDEX_ENTRY_SIZE;
        partitionDataReader.readIndexBuffer(targetPosition);
      }

      // get the data file offset and the data size
      dataConsumingOffset = indexBuffer.getLong();
      currentPartitionRemainingBytes = indexBuffer.getLong();
      --numRemainingPartitions;

      logger.debug(
          "readBuffer updateConsumingOffset, {},  {}, {}, {}",
          streamId,
          partitionDataReader.getDataFileSize(),
          dataConsumingOffset,
          currentPartitionRemainingBytes);

      // if these checks fail, the partition file must be corrupted
      if (dataConsumingOffset < 0
          || dataConsumingOffset + currentPartitionRemainingBytes
              > partitionDataReader.getDataFileSize()
          || currentPartitionRemainingBytes < 0) {
        throw new FileCorruptedException("File " + fileInfo.getFilePath() + " is corrupted");
      }
    }
  }

  private synchronized boolean readBuffer(ByteBuf buffer) throws IOException {
    try {
      int readSize = partitionDataReader.readBuffer(buffer, dataConsumingOffset);
      currentPartitionRemainingBytes -= readSize;
      dataConsumingOffset = partitionDataReader.position();

      logger.debug(
          "readBuffer data: {}, {}, {}, {}, {}, {}",
          streamId,
          currentPartitionRemainingBytes,
          readSize,
          dataConsumingOffset,
          fileInfo.getFilePath(),
          System.identityHashCode(buffer));

      // if this check fails, the partition file must be corrupted
      if (currentPartitionRemainingBytes < 0) {
        throw new FileCorruptedException("File is corrupted");
      } else if (currentPartitionRemainingBytes == 0) {
        logger.debug(
            "readBuffer end, {},  {}, {}, {}",
            streamId,
            partitionDataReader.getDataFileSize(),
            dataConsumingOffset,
            currentPartitionRemainingBytes);
        int prevDataRegion = currentDataRegion;
        updateConsumingOffset();
        return prevDataRegion == currentDataRegion && currentPartitionRemainingBytes > 0;
      }

      logger.debug(
          "readBuffer run: {}, {}, {}, {}",
          streamId,
          partitionDataReader.getDataFileSize(),
          dataConsumingOffset,
          currentPartitionRemainingBytes);
      return true;
    } catch (Throwable throwable) {
      logger.error("Failed to read partition file.", throwable);
      synchronized (lock) {
        isReleased = true;
      }
      throw throwable;
    }
  }

  public boolean hasRemaining() {
    return currentPartitionRemainingBytes > 0;
  }

  protected void notifyBacklog(int backlog) {
    logger.debug("stream manager stream id {} backlog:{}", streamId, backlog);
    associatedChannel
        .writeAndFlush(new BacklogAnnouncement(streamId, backlog))
        .addListener(
            future -> {
              if (!future.isSuccess()) {
                logger.error("send backlog {} to stream {} failed", backlog, streamId);
              }
            });
  }

  private void notifyError(Throwable throwable) {
    logger.error(
        "Read file: {} error from {}, stream id {}",
        fileInfo.getFilePath(),
        NettyUtils.getRemoteAddress(this.associatedChannel),
        streamId,
        throwable);
    if (throwable instanceof ClosedChannelException) {
      return;
    }

    if (this.associatedChannel.isActive()) {
      // If a stream is failed, send exceptions with the best effort, do not expect response.
      // And do not close channel because multiple streams are using the very same channel.
      // wrapIOException to PartitionUnRetryAbleException, client may choose regenerate the data.
      this.associatedChannel.writeAndFlush(
          new TransportableError(streamId, ExceptionUtils.wrapIOExceptionToUnRetryable(throwable)));
    }
  }

  public long getPriority() {
    return dataConsumingOffset;
  }

  @Override
  public int compareTo(MapPartitionDataReader that) {
    return Long.compare(getPriority(), that.getPriority());
  }

  public FileInfo getFileInfo() {
    return fileInfo;
  }

  public void closeReader() {
    synchronized (lock) {
      readFinished = true;
    }

    logger.debug("Closed read for stream {}", this.streamId);
  }

  public void recycle() {
    synchronized (lock) {
      if (!isReleased) {
        release();
        recycleStream.run();
      }
    }
  }

  public void recycleOnError(Throwable throwable) {
    synchronized (lock) {
      if (!errorNotified) {
        errorNotified = true;
        errorCause = throwable;
        notifyError(throwable);
        recycle();
      }
    }
  }

  public void release() {
    // we can safely release if reader reaches error or (read/send finished)
    synchronized (lock) {
      if (!isReleased) {
        logger.debug("release reader for stream {}", streamId);
        // old client can't support BufferStreamEnd, so for new client it tells client that this
        // stream is finished.
        if (fileInfo.isPartitionSplitEnabled() && !errorNotified) {
          associatedChannel.writeAndFlush(
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
        }
        if (!buffersToSend.isEmpty()) {
          int dataBufferInUse = 0;
          RecyclableBuffer buffer;
          while ((buffer = buffersToSend.poll()) != null) {
            if (buffer.isDataBuffer()) {
              dataBufferInUse++;
            }
            buffer.recycle();
          }
          numInUseBuffers.addAndGet(-1 * dataBufferInUse);
        }
        isReleased = true;
      }
    }
  }

  public boolean isFinished() {
    synchronized (lock) {
      // ensure every buffer are return to bufferQueue or release in buffersRead
      return numInUseBuffers.get() == 0 && isReleased;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DataPartitionReader{");
    sb.append("startPartitionIndex=").append(startPartitionIndex);
    sb.append(", endPartitionIndex=").append(endPartitionIndex);
    sb.append(", streamId=").append(streamId);
    sb.append('}');
    return sb.toString();
  }

  public long getStreamId() {
    return streamId;
  }

  @VisibleForTesting
  public AtomicInteger getNumInUseBuffers() {
    return numInUseBuffers;
  }

  public boolean shouldReadData() {
    synchronized (lock) {
      return !isReleased && !readFinished;
    }
  }

  public void close() {
    partitionDataReader.close();
  }
}
