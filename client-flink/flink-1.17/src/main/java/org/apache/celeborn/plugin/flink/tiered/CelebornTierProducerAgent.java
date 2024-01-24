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

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.DriverChangedException;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.CheckUtils;
import org.apache.celeborn.plugin.flink.RemoteShuffleDescriptor;
import org.apache.celeborn.plugin.flink.buffer.BufferHeader;
import org.apache.celeborn.plugin.flink.buffer.BufferPacker;
import org.apache.celeborn.plugin.flink.readclient.FlinkShuffleClientImpl;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.util.ExceptionUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

public class CelebornTierProducerAgent implements TierProducerAgent {

    private final int numBuffersPerSegment;

    private final int bufferSizeBytes;

    private final int numPartitions;

    private final int numSubpartitions;

    private final CelebornConf celebornConf;

    private final TieredStorageMemoryManager memoryManager;

    private final String applicationId;

    private final int shuffleId;

    private final int mapId;

    private final int attemptId;

    private final int partitionId;

    private final String lifecycleManagerHost;

    private final int lifecycleManagerPort;

    private final long lifecycleManagerTimestamp;

    private final FlinkShuffleClientImpl flinkShuffleClient;

    private final BufferPacker bufferPacker;

    private final int[] subpartitionSegmentIds;

    private final int[] subpartitionSegmentBuffers;

    private PartitionLocation partitionLocation;

    private boolean hasRegisteredShuffle;

    private boolean hasWrittenData = false;

    private volatile boolean isReleased;

    CelebornTierProducerAgent(
            CelebornConf conf,
            TieredStoragePartitionId partitionId,
            int numPartitions,
            int numSubpartitions,
            int numBytesPerSegment,
            int bufferSizeBytes,
            TieredStorageMemoryManager memoryManager,
            TieredStorageResourceRegistry resourceRegistry,
            List<TierShuffleDescriptor> shuffleDescriptors) {
        checkArgument(
                numBytesPerSegment >= bufferSizeBytes,
                "One segment should contain at least one buffer.");
        checkArgument(
                shuffleDescriptors.size() == 1, "There should be only one shuffle descriptor.");
        TierShuffleDescriptor descriptor = shuffleDescriptors.get(0);
        checkArgument(descriptor instanceof RemoteShuffleDescriptor,
                "Wrong shuffle descriptor type " + descriptor.getClass());
        RemoteShuffleDescriptor shuffleDesc = (RemoteShuffleDescriptor) descriptor;

        this.numBuffersPerSegment = numBytesPerSegment / bufferSizeBytes;
        this.bufferSizeBytes = bufferSizeBytes;
        this.memoryManager = memoryManager;
        this.numPartitions = numPartitions;
        this.numSubpartitions = numSubpartitions;
        this.celebornConf = conf;
        this.subpartitionSegmentIds = new int[numSubpartitions];
        this.subpartitionSegmentBuffers = new int[numSubpartitions];

        this.applicationId = shuffleDesc.getCelebornAppId();
        this.shuffleId =
                shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getShuffleId();
        this.mapId = shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getMapId();
        this.attemptId =
                shuffleDesc.getShuffleResource().getMapPartitionShuffleDescriptor().getAttemptId();
        this.partitionId =
                shuffleDesc
                        .getShuffleResource()
                        .getMapPartitionShuffleDescriptor()
                        .getPartitionId();
        this.lifecycleManagerHost = shuffleDesc.getShuffleResource().getLifecycleManagerHost();
        this.lifecycleManagerPort = shuffleDesc.getShuffleResource().getLifecycleManagerPort();
        this.lifecycleManagerTimestamp =
                shuffleDesc.getShuffleResource().getLifecycleManagerTimestamp();
        this.flinkShuffleClient = getShuffleClient();

        Arrays.fill(subpartitionSegmentIds, -1);
        Arrays.fill(subpartitionSegmentBuffers, 0);

        this.bufferPacker = new BufferPacker(this::write);
        resourceRegistry.registerResource(partitionId, this::releaseResources);
        registerShuffle();
        try {
            handshake();
        } catch (IOException e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
    }

    @Override
    public boolean tryStartNewSegment(
            TieredStorageSubpartitionId tieredStorageSubpartitionId, int segmentId) {
        int subpartitionId = tieredStorageSubpartitionId.getSubpartitionId();
        checkState(segmentId >= subpartitionSegmentIds[subpartitionId]);
        subpartitionSegmentIds[subpartitionId] = segmentId;
        // If the start segment rpc is sent, the worker side will expect that
        // there must be at least one buffer will be written in the next moment.
        try {
            flinkShuffleClient.segmentStart(
                    shuffleId, mapId, attemptId, subpartitionId, segmentId, partitionLocation);
        } catch (IOException e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
        return true;
    }
    @Override
    public boolean tryWrite(
            TieredStorageSubpartitionId tieredStorageSubpartitionId,
            Buffer buffer,
            Object bufferOwner) {
        int subpartitionId = tieredStorageSubpartitionId.getSubpartitionId();
        hasWrittenData = true;

        if (subpartitionSegmentBuffers[subpartitionId] + 1 >= numBuffersPerSegment) {
            subpartitionSegmentBuffers[subpartitionId] = 0;
            bufferPacker.drain();
            appendEndOfSegmentBuffer(subpartitionId);
            return false;
        }

        if (buffer.isBuffer()) {
            memoryManager.transferBufferOwnership(bufferOwner, this, buffer);
        }

        try {
            bufferPacker.process(buffer, subpartitionId);
            bufferPacker.drain();
        } catch (InterruptedException e) {
            buffer.recycleBuffer();
            throw new RuntimeException(e);
        }
        subpartitionSegmentBuffers[subpartitionId]++;
        return true;
    }

    @Override
    public void close() {
        if (hasWrittenData) {
            regionFinish();
        }
        try {
            if (hasRegisteredShuffle && partitionLocation != null) {
                flinkShuffleClient.mapPartitionMapperEnd(
                        shuffleId, mapId, attemptId, numPartitions, partitionLocation.getId());
            }
        } catch (Exception e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
        bufferPacker.close();
        flinkShuffleClient.cleanup(shuffleId, mapId, attemptId);
    }

    void regionFinish() {
        try {
            bufferPacker.drain();
            flinkShuffleClient.regionFinish(shuffleId, mapId, attemptId, partitionLocation);
        } catch (Exception e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
    }

    private void handshake() throws IOException {
        try {
            flinkShuffleClient.pushDataHandShake(
                    shuffleId, mapId, attemptId, numSubpartitions, bufferSizeBytes, partitionLocation);
        } catch (IOException e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
    }

    private void releaseResources() {
        if (!isReleased) {
            isReleased = true;
        }
    }

    private void registerShuffle() {
        try {
            if (!hasRegisteredShuffle) {
                partitionLocation =
                        flinkShuffleClient.registerMapPartitionTask(
                                shuffleId, numPartitions, mapId, attemptId, partitionId);
                CheckUtils.checkNotNull(partitionLocation);
                hasRegisteredShuffle = true;
            }
        } catch (IOException e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
    }

    private void write(ByteBuf byteBuf, BufferHeader bufferHeader) {
        try {
            CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
            ByteBuf headerBuf = Unpooled.buffer(BufferUtils.HEADER_LENGTH);
            headerBuf.writeInt(bufferHeader.getSubPartitionId());
            headerBuf.writeInt(attemptId);
            headerBuf.writeInt(0);
            headerBuf.writeInt(
                    byteBuf.readableBytes()
                            + (BufferUtils.HEADER_LENGTH - BufferUtils.HEADER_LENGTH_PREFIX));
            headerBuf.writeByte(bufferHeader.getDataType().ordinal());
            headerBuf.writeBoolean(bufferHeader.isCompressed());
            headerBuf.writeInt(bufferHeader.getSize());
            compositeByteBuf.addComponents(true, headerBuf, byteBuf);
            io.netty.buffer.ByteBuf wrappedBuffer =
                    io.netty.buffer.Unpooled.wrappedBuffer(compositeByteBuf.nioBuffer());

            int numWritten =
                    flinkShuffleClient.pushDataToLocation(
                            shuffleId,
                            mapId,
                            attemptId,
                            bufferHeader.getSubPartitionId(),
                            wrappedBuffer,
                            partitionLocation,
                            compositeByteBuf::release);
            checkState(numWritten == bufferHeader.getSize() + BufferUtils.HEADER_LENGTH);
        } catch (IOException e) {
            CheckUtils.rethrowAsRuntimeException(e);
        }
    }

    private void appendEndOfSegmentBuffer(int subpartitionId) {
        try {
            checkState(bufferPacker.isEmpty());
            MemorySegment endSegmentMemorySegment =
                    MemorySegmentFactory.wrap(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE).array());
            Buffer endOfSegmentBuffer =
                    new NetworkBuffer(
                            endSegmentMemorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            END_OF_SEGMENT,
                            endSegmentMemorySegment.size());
            bufferPacker.process(endOfSegmentBuffer, subpartitionId);
            bufferPacker.drain();
        } catch (Exception e) {
            ExceptionUtils.rethrow(e, "Failed to append end of segment event,");
        }
    }

    @VisibleForTesting
    FlinkShuffleClientImpl getShuffleClient() {
        try {
            return FlinkShuffleClientImpl.get(
                    applicationId,
                    lifecycleManagerHost,
                    lifecycleManagerPort,
                    lifecycleManagerTimestamp,
                    celebornConf,
                    null);
        } catch (DriverChangedException e) {
            // would generate a new attempt to retry output gate
            throw new RuntimeException(e.getMessage());
        }
    }
}