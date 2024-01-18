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

import org.apache.celeborn.common.exception.PartitionUnRetryAbleException;
import org.apache.celeborn.common.network.protocol.BacklogAnnouncement;
import org.apache.celeborn.common.network.protocol.BufferStreamEnd;
import org.apache.celeborn.common.network.protocol.RequestMessage;
import org.apache.celeborn.common.network.protocol.TransportableError;
import org.apache.celeborn.common.network.util.NettyUtils;
import org.apache.celeborn.common.protocol.PbNotifyRequiredSegment;
import org.apache.celeborn.common.protocol.PbReadAddCredit;
import org.apache.celeborn.plugin.flink.ShuffleResourceDescriptor;
import org.apache.celeborn.plugin.flink.protocol.ReadData;
import org.apache.celeborn.plugin.flink.readclient.CelebornBufferStream;
import org.apache.celeborn.plugin.flink.readclient.FlinkShuffleClientImpl;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public class CelebornChannelBufferReader {
    private static final Logger LOG = LoggerFactory.getLogger(CelebornChannelBufferReader.class);

    private CelebornChannelBufferManager bufferManager;

    private final FlinkShuffleClientImpl client;

    private final int shuffleId;

    private final int partitionId;

    private final int subPartitionIndexStart;

    private final int subPartitionIndexEnd;

    private final Consumer<ByteBuf> dataListener;

    private final Consumer<Throwable> failureListener;

    private final Consumer<RequestMessage> messageConsumer;

    private CelebornBufferStream bufferStream;

    private boolean isOpened;

    private volatile boolean closed = false;

    /** Note this field is to record the number of backlog before the read is set up. */
    private int numBackLog = 0;

    public CelebornChannelBufferReader(
            FlinkShuffleClientImpl client,
            ShuffleResourceDescriptor shuffleDescriptor,
            int startSubIdx,
            int endSubIdx,
            Consumer<ByteBuf> dataListener,
            Consumer<Throwable> failureListener) {
        this.client = client;
        this.shuffleId = shuffleDescriptor.getShuffleId();
        this.partitionId = shuffleDescriptor.getPartitionId();
        this.subPartitionIndexStart = startSubIdx;
        this.subPartitionIndexEnd = endSubIdx;
        this.dataListener = dataListener;
        this.failureListener = failureListener;
        this.messageConsumer =
                requestMessage -> {
                    if (requestMessage instanceof ReadData) {
                        dataReceived((ReadData) requestMessage);
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
        }
    }

    public void open(int initialCredit, CompletableFuture<Boolean> openReaderFuture) {
        try {
            bufferStream =
                    client.readBufferedPartition(
                            shuffleId, partitionId, subPartitionIndexStart, subPartitionIndexEnd);
            if (CelebornBufferStream.isEmptyStream(bufferStream)) {
                LOG.info("The buffer stream is empty, ignore the open process.");
                openReaderFuture.complete(false);
                return;
            }
            bufferStream.open(
                    this::requestBuffer, initialCredit, messageConsumer, openReaderFuture);
        } catch (Exception e) {
            if (e instanceof PartitionUnRetryAbleException) {
                // TODO remove this line after the exception is resolved correctly
                LOG.warn("Failed to open stream. ", e);
                openReaderFuture.complete(false);
                return;
            }
            LOG.warn("Failed to open stream and report to flink framework. ", e);
            openReaderFuture.complete(false);
            messageConsumer.accept(new TransportableError(0L, e));
        }
    }

    public void close() {
        // need set closed first before remove Handler
        closed = true;
        if (!CelebornBufferStream.isEmptyStream(bufferStream)) {
            bufferStream.close();
        } else {
            LOG.warn(
                    "bufferStream is null when closed, shuffleId: {}, partitionId: {}",
                    shuffleId,
                    partitionId);
        }

        try {
            bufferManager.close();
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
        if (!closed && !CelebornBufferStream.isEmptyStream(bufferStream)) {
            bufferStream.addCredit(
                    PbReadAddCredit.newBuilder()
                            .setStreamId(bufferStream.getStreamId())
                            .setCredit(numCredits)
                            .build());
        }
    }

    public void notifyRequiredSegment(int requiredSegmentId) {
        if (!closed && !CelebornBufferStream.isEmptyStream(bufferStream)) {
            PbNotifyRequiredSegment notifyRequiredSegment =
                    PbNotifyRequiredSegment.newBuilder()
                            .setStreamId(bufferStream.getStreamId())
                            .setRequiredSegmentId(requiredSegmentId)
                            .build();
            bufferStream.notifyRequiredSegment(notifyRequiredSegment);
        }
    }

    public ByteBuf requestBuffer() {
        Buffer buffer = bufferManager.requestBuffer();
        return buffer == null? null:buffer.asByteBuf();
    }

    public void backlogReceived(int backlog) {
        if (!closed) {
            if (bufferManager == null) {
                numBackLog = backlog;
                return;
            }
            int numRequestedBuffers = bufferManager.requestBuffers(backlog);
            if (numRequestedBuffers > 0) {
                notifyAvailableCredits(numRequestedBuffers);
            }
            numBackLog = 0;
        }
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
            failureListener.accept(new IOException(errorMsg));
        }
    }

    public void dataReceived(ReadData readData) {
        LOG.debug(
                "Remote buffer stream reader get stream id {} received readable bytes {}.",
                readData.getStreamId(),
                readData.getFlinkBuffer().readableBytes());
        dataListener.accept(readData.getFlinkBuffer());
    }

    public void onStreamEnd(BufferStreamEnd streamEnd) {
        long streamId = streamEnd.getStreamId();
        LOG.debug("Buffer stream reader get stream end for {}", streamId);
        // TODO, update locations when the stream is end.
    }

    public void setIsOpened(boolean isOpened) {
        this.isOpened = isOpened;
    }
}
