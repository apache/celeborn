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

package org.apache.celeborn.plugin.flink.tiered;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.network.protocol.BacklogAnnouncement;
import org.apache.celeborn.common.network.protocol.BufferStreamEnd;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.TransportableError;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.protocol.PbNotifyRequiredSegment;
import org.apache.celeborn.common.protocol.PbReadAddCredit;
import org.apache.celeborn.plugin.flink.ShuffleResourceDescriptor;
import org.apache.celeborn.plugin.flink.protocol.SubPartitionReadData;
import org.apache.celeborn.plugin.flink.readclient.CelebornBufferStream;
import org.apache.celeborn.plugin.flink.readclient.FlinkShuffleClientImpl;

/**
 * Wrap the {@link CelebornBufferStream}, utilize in flink hybrid shuffle integration strategy now.
 */
public class CelebornChannelBufferReader {
  private static final Logger LOG = LoggerFactory.getLogger(CelebornChannelBufferReader.class);

  private CelebornChannelBufferManager bufferManager;

  private final FlinkShuffleClientImpl client;

  private final int shuffleId;

  private final int partitionId;

  private final TieredStorageInputChannelId inputChannelId;

  private final int subPartitionIndexStart;

  private final int subPartitionIndexEnd;

  private final BiConsumer<ByteBuf, TieredStorageSubpartitionId> dataListener;

  private final BiConsumer<Throwable, TieredStorageSubpartitionId> failureListener;

  private final Consumer<RequestMessage> messageConsumer;

  private CelebornBufferStream bufferStream;

  private boolean isOpened;

  private volatile boolean closed = false;

  private volatile ConcurrentHashMap<Integer, Integer> subPartitionRequiredSegmentIds;

  /** Note this field is to record the number of backlog before the read is set up. */
  private int numBackLog = 0;

  public CelebornChannelBufferReader(
      FlinkShuffleClientImpl client,
      ShuffleResourceDescriptor shuffleDescriptor,
      TieredStorageInputChannelId inputChannelId,
      int startSubIdx,
      int endSubIdx,
      BiConsumer<ByteBuf, TieredStorageSubpartitionId> dataListener,
      BiConsumer<Throwable, TieredStorageSubpartitionId> failureListener) {
    this.client = client;
    this.shuffleId = shuffleDescriptor.getShuffleId();
    this.partitionId = shuffleDescriptor.getPartitionId();
    this.inputChannelId = inputChannelId;
    this.subPartitionIndexStart = startSubIdx;
    this.subPartitionIndexEnd = endSubIdx;
    this.dataListener = dataListener;
    this.failureListener = failureListener;
    this.subPartitionRequiredSegmentIds = new ConcurrentHashMap<>();
    for (int subPartitionId = subPartitionIndexStart;
        subPartitionId <= subPartitionIndexEnd;
        subPartitionId++) {
      subPartitionRequiredSegmentIds.put(subPartitionId, -1);
    }
    this.messageConsumer =
        requestMessage -> {
          // Note that we need to use SubPartitionReadData because the isSegmentGranularityVisible
          // is set
          // as true when opening stream
          if (requestMessage instanceof SubPartitionReadData) {
            dataReceived((SubPartitionReadData) requestMessage);
          } else if (requestMessage instanceof BacklogAnnouncement) {
            backlogReceived(((BacklogAnnouncement) requestMessage).getBacklog());
          } else if (requestMessage instanceof TransportableError) {
            errorReceived(((TransportableError) requestMessage).getErrorMessage());
          } else if (requestMessage instanceof BufferStreamEnd) {
            onStreamEnd((BufferStreamEnd) requestMessage);
          }
        };
  }

  public void setup(TieredStorageMemoryManager memoryManager) {
    this.bufferManager = new CelebornChannelBufferManager(memoryManager, this);
    if (numBackLog > 0) {
      notifyAvailableCredits(bufferManager.requestBuffers(numBackLog));
      numBackLog = 0;
    }
  }

  public void open(int initialCredit) {
    try {
      bufferStream =
          client.readBufferedPartition(
              shuffleId, partitionId, subPartitionIndexStart, subPartitionIndexEnd, true);
      bufferStream.open(this::requestBuffer, initialCredit, messageConsumer);
      this.isOpened = bufferStream.isOpened();
    } catch (Exception e) {
      messageConsumer.accept(new TransportableError(0L, e));
    }
  }

  public void close() {
    // It may be call multiple times because subPartitions can share the same reader, as a single
    // reader can consume multiple subPartitions
    if (closed) {
      return;
    }

    // need set closed first before remove Handler
    closed = true;
    if (!CelebornBufferStream.isEmptyStream(bufferStream)) {
      bufferStream.close();
      bufferStream = null;
    } else {
      LOG.warn(
          "bufferStream is null when closed, shuffleId: {}, partitionId: {}",
          shuffleId,
          partitionId);
    }

    try {
      if (bufferManager != null) {
        bufferManager.close();
      }
    } catch (Throwable throwable) {
      LOG.warn("Failed to close buffer manager.", throwable);
    }
  }

  public boolean isOpened() {
    return isOpened;
  }

  boolean isClosed() {
    return closed;
  }

  public void notifyAvailableCredits(int numCredits) {
    if (numCredits <= 0) {
      return;
    }
    if (!closed && !CelebornBufferStream.isEmptyStream(bufferStream)) {
      bufferStream.addCreditWithoutResponse(
          PbReadAddCredit.newBuilder()
              .setStreamId(bufferStream.getStreamId())
              .setCredit(numCredits)
              .build());
      bufferManager.decreaseRequiredCredits(numCredits);
      return;
    }
    LOG.warn(
        "The buffer stream is null or closed, ignore the credits for shuffleId: {}, partitionId: {}",
        shuffleId,
        partitionId);
  }

  public void notifyRequiredSegmentIfNeeded(int requiredSegmentId, int subPartitionId) {
    Integer lastRequiredSegmentId =
        subPartitionRequiredSegmentIds.computeIfAbsent(subPartitionId, id -> -1);
    if (requiredSegmentId >= 0 && requiredSegmentId != lastRequiredSegmentId) {
      LOG.debug(
          "Notify required segment id {} for {} {}, the last segment id is {}",
          requiredSegmentId,
          partitionId,
          subPartitionId,
          lastRequiredSegmentId);
      subPartitionRequiredSegmentIds.put(subPartitionId, requiredSegmentId);
      if (!this.notifyRequiredSegment(requiredSegmentId, subPartitionId)) {
        // if fail to notify reader segment, restore the last required segment id
        subPartitionRequiredSegmentIds.putIfAbsent(subPartitionId, lastRequiredSegmentId);
      }
    }
  }

  public boolean notifyRequiredSegment(int requiredSegmentId, int subPartitionId) {
    this.subPartitionRequiredSegmentIds.put(subPartitionId, requiredSegmentId);
    if (!closed && !CelebornBufferStream.isEmptyStream(bufferStream)) {
      LOG.debug(
          "notifyRequiredSegmentId, shuffleId: {}, partitionId: {}, requiredSegmentId: {}",
          shuffleId,
          partitionId,
          requiredSegmentId);
      PbNotifyRequiredSegment notifyRequiredSegment =
          PbNotifyRequiredSegment.newBuilder()
              .setStreamId(bufferStream.getStreamId())
              .setRequiredSegmentId(requiredSegmentId)
              .setSubPartitionId(subPartitionId)
              .build();
      bufferStream.notifyRequiredSegment(notifyRequiredSegment);
      return true;
    }
    return false;
  }

  public ByteBuf requestBuffer() {
    Buffer buffer = bufferManager.requestBuffer();
    return buffer == null ? null : buffer.asByteBuf();
  }

  public void backlogReceived(int backlog) {
    if (!closed) {
      if (bufferManager == null) {
        numBackLog += backlog;
        return;
      }
      int numRequestedBuffers = bufferManager.requestBuffers(backlog);
      if (numRequestedBuffers > 0) {
        notifyAvailableCredits(numRequestedBuffers);
      }
      numBackLog = 0;
      return;
    }
    LOG.warn(
        "The buffer stream is null or closed, ignore the backlog for shuffleId: {}, partitionId: {}",
        shuffleId,
        partitionId);
  }

  public void errorReceived(String errorMsg) {
    if (!closed) {
      closed = true;
      LOG.debug("Error received, " + errorMsg);
      if (!CelebornBufferStream.isEmptyStream(bufferStream) && bufferStream.getClient() != null) {
        LOG.error(
            "Received error from {} message {}",
            NettyUtils.getRemoteAddress(bufferStream.getClient().getChannel()),
            errorMsg);
      }
      for (int subPartitionId = subPartitionIndexStart;
          subPartitionId <= subPartitionIndexEnd;
          subPartitionId++) {
        failureListener.accept(
            new IOException(errorMsg), new TieredStorageSubpartitionId(subPartitionId));
      }
    }
  }

  public void dataReceived(SubPartitionReadData readData) {
    LOG.debug(
        "Remote buffer stream reader get stream id {} subPartitionId {} received readable bytes {}.",
        readData.getStreamId(),
        readData.getSubPartitionId(),
        readData.getFlinkBuffer().readableBytes());
    checkState(
        readData.getSubPartitionId() >= subPartitionIndexStart
            && readData.getSubPartitionId() <= subPartitionIndexEnd,
        "Wrong sub partition id");
    dataListener.accept(
        readData.getFlinkBuffer(), new TieredStorageSubpartitionId(readData.getSubPartitionId()));
    int numRequested = bufferManager.tryRequestBuffersIfNeeded();
    notifyAvailableCredits(numRequested);
  }

  public void onStreamEnd(BufferStreamEnd streamEnd) {
    long streamId = streamEnd.getStreamId();
    LOG.debug("Buffer stream reader get stream end for {}", streamId);
    if (!closed && !CelebornBufferStream.isEmptyStream(bufferStream)) {
      // TOOD: Update the partition locations here if support reading and writing shuffle data
      // simultaneously
      bufferStream.moveToNextPartitionIfPossible(streamId, this::sendRequireSegmentId);
    }
  }

  public TieredStorageInputChannelId getInputChannelId() {
    return inputChannelId;
  }

  private void sendRequireSegmentId(long streamId, int subPartitionId) {
    if (subPartitionRequiredSegmentIds.containsKey(subPartitionId)) {
      int currentSegmentId = subPartitionRequiredSegmentIds.get(subPartitionId);
      if (bufferStream.isOpened() && currentSegmentId >= 0) {
        LOG.debug(
            "Buffer stream {} is opened, notify required segment id {} ",
            streamId,
            currentSegmentId);
        notifyRequiredSegment(currentSegmentId, subPartitionId);
      }
    }
  }
}
