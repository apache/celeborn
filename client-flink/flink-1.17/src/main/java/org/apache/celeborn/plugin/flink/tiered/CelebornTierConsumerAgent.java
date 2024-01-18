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
import org.apache.celeborn.common.exception.PartitionUnRetryAbleException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.plugin.flink.RemoteShuffleDescriptor;
import org.apache.celeborn.plugin.flink.RemoteShuffleResource;
import org.apache.celeborn.plugin.flink.ShuffleResourceDescriptor;
import org.apache.celeborn.plugin.flink.buffer.BufferPacker;
import org.apache.celeborn.plugin.flink.buffer.TransferBufferPool;
import org.apache.celeborn.plugin.flink.readclient.FlinkShuffleClientImpl;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkNotNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

public class CelebornTierConsumerAgent implements TierConsumerAgent {

    private static final Logger LOG = LoggerFactory.getLogger(CelebornTierConsumerAgent.class);

    private final int gateIndex;

    private final List<TieredStorageConsumerSpec> consumerSpecs;

    private final List<TierShuffleDescriptor> shuffleDescriptors;

    private final TransferBufferPool transferBufferPool;

    private final FlinkShuffleClientImpl shuffleClient;

    private final Map<TieredStoragePartitionId,
            Map<TieredStorageSubpartitionId, CelebornChannelBufferReader>> bufferReaders;

    private final Map<ResultPartitionID, Map<Integer, Integer>> partitionLastRequiredSegmentId;

    /** Lock to protect {@link #receivedBuffers} and {@link #cause} and {@link #closed}, etc. */
    private final Object lock = new Object();

    /** Received buffers from remote shuffle worker. It's consumed by upper computing task. */
    @GuardedBy("lock")
    private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Queue<Buffer>>> receivedBuffers;

    @GuardedBy("lock")
    private final Set<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>> partitionsNeedNotifyAvailable;

    @GuardedBy("lock")
    private final AtomicInteger numOpenedReaders = new AtomicInteger(0);

    @GuardedBy("lock")
    private boolean hasStart = false;

    @GuardedBy("lock")
    private Throwable cause;

    /** Whether this remote input gate has been closed or not. */
    @GuardedBy("lock")
    private boolean closed;

    private AvailabilityNotifier availabilityNotifier;

    private int numTotalReaders = 0;

    public CelebornTierConsumerAgent(
            CelebornConf conf,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<TierShuffleDescriptor> shuffleDescriptors) {
        checkArgument(shuffleDescriptors.size() > 0);
        checkArgument(tieredStorageConsumerSpecs.size() == shuffleDescriptors.size());
        this.gateIndex = tieredStorageConsumerSpecs.get(0).getGateIndex();
        this.consumerSpecs = tieredStorageConsumerSpecs;
        this.shuffleDescriptors = shuffleDescriptors;
        this.partitionLastRequiredSegmentId = new HashMap<>();
        this.bufferReaders = new HashMap<>();
        this.receivedBuffers = new HashMap<>();
        this.partitionsNeedNotifyAvailable = new HashSet<>();
        this.transferBufferPool = new TransferBufferPool(Collections.emptySet());
        checkState(shuffleDescriptors.get(0) instanceof RemoteShuffleDescriptor);
        RemoteShuffleDescriptor remoteShuffleDescriptor =
                (RemoteShuffleDescriptor) shuffleDescriptors.get(0);
        RemoteShuffleResource shuffleResource = remoteShuffleDescriptor.getShuffleResource();
        try {
            String appUniqueId = remoteShuffleDescriptor.getCelebornAppId();
            this.shuffleClient =
                    FlinkShuffleClientImpl.get(
                            appUniqueId,
                            shuffleResource.getLifecycleManagerHost(),
                            shuffleResource.getLifecycleManagerPort(),
                            shuffleResource.getLifecycleManagerTimestamp(),
                            conf,
                            new UserIdentifier("default", "default"));
        } catch (DriverChangedException e) {
            throw new RuntimeException(e.getMessage());
        }
        LOG.info("Init consumer agent");
    }

    @Override
    public void setup(TieredStorageMemoryManager memoryManager) {
        for (Map<TieredStorageSubpartitionId, CelebornChannelBufferReader> subpartitionReaders :
                bufferReaders.values()) {
            subpartitionReaders.forEach((partitionId, reader) -> reader.setup(memoryManager));
        }
    }

    @Override
    public void start() {
        synchronized (lock) {
            try {
                partitionsNeedNotifyAvailable.forEach(
                        partitionIdTuple ->
                                availabilityNotifier.notifyAvailable(
                                        partitionIdTuple.f0, partitionIdTuple.f1));
            } catch (Throwable t) {
                LOG.error("Error occurred when notifying subpartitions available", t);
                recycleAllResources();
                ExceptionUtils.rethrow(t);
            }
            partitionsNeedNotifyAvailable.clear();
            hasStart = true;
        }
    }

    @Override
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId tieredStoragePartitionId,
            TieredStorageSubpartitionId tieredStorageSubpartitionId,
            int segmentId) {
        boolean openReaderSuccess = openReader(tieredStoragePartitionId, tieredStorageSubpartitionId);
        if (!openReaderSuccess) {
            return Optional.empty();
        }
        CelebornChannelBufferReader bufferReader =
                bufferReaders.get(tieredStoragePartitionId).get(tieredStorageSubpartitionId);
        checkState(bufferReader.isOpened(), "Reader should be opened");
        ResultPartitionID partitionId = tieredStoragePartitionId.getPartitionID();
        notifyRequiredSegmentIfNeeded(
                partitionId, tieredStorageSubpartitionId.getSubpartitionId(), segmentId, bufferReader);
        try {
            synchronized (lock) {
                Map<TieredStorageSubpartitionId, Queue<Buffer>> partitionBuffers =
                        receivedBuffers.get(tieredStoragePartitionId);
                if (partitionBuffers == null || partitionBuffers.size() == 0) {
                    return Optional.empty();
                }
                Queue<Buffer> subpartitionBuffers = partitionBuffers.get(tieredStorageSubpartitionId);
                if (subpartitionBuffers == null || subpartitionBuffers.size() == 0) {
                    return Optional.empty();
                }
                healthCheck();
                return Optional.ofNullable(subpartitionBuffers.poll());
            }
        } catch (IOException e) {
            LOG.error("Failed to get next buffer.", e);
            ExceptionUtils.rethrow(e);
            recycleAllResources();
            return Optional.empty();
        }
    }

    @Override
    public void registerAvailabilityNotifier(AvailabilityNotifier availabilityNotifier) {
        this.availabilityNotifier = availabilityNotifier;
        initBufferReaders();
        LOG.info("Registered availability notifier for gate {}.", gateIndex);
    }

    @Override
    public void close() {
        Throwable closeException = null;
        // Do not check closed flag, thus to allow calling this method from both task thread and
        // cancel thread.
        try {
            recycleAllResources();
        } catch (Throwable throwable) {
            closeException = throwable;
            LOG.error("Failed to recycle all resources.", throwable);
        }
        try {
            transferBufferPool.destroy();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close transfer buffer pool.", throwable);
        }
        if (closeException != null) {
            ExceptionUtils.rethrow(closeException);
        }
    }

    private void recycleAllResources() {
        List<Buffer> buffersToRecycle = new ArrayList<>();
        for (Map<TieredStorageSubpartitionId, CelebornChannelBufferReader> subpartitionReaders :
                bufferReaders.values()) {
            subpartitionReaders.values().forEach(CelebornChannelBufferReader::close);
        }
        synchronized (lock) {
            for (Map<TieredStorageSubpartitionId, Queue<Buffer>> subpartitionMap :
                    receivedBuffers.values()) {
                buffersToRecycle.addAll(subpartitionMap.values().stream()
                        .flatMap(Queue::stream)
                        .collect(Collectors.toCollection(LinkedList::new)));
            }
            receivedBuffers.clear();
            closed = true;
        }
        try {
            buffersToRecycle.forEach(Buffer::recycleBuffer);
        } catch (Throwable throwable) {
            LOG.error("Failed to recycle buffers.", throwable);
            throw throwable;
        }
    }

    private void notifyRequiredSegmentIfNeeded(
            ResultPartitionID partitionId,
            int subpartitionId,
            int segmentId,
            CelebornChannelBufferReader bufferReader) {
        Integer lastRequiredSegmentId =
                partitionLastRequiredSegmentId
                        .computeIfAbsent(partitionId, partition -> new HashMap<>())
                        .computeIfAbsent(subpartitionId, id -> -1);
        if (segmentId >= 0 && segmentId != lastRequiredSegmentId) {
            LOG.debug(
                    "Notify required segment id {} for {} {}, the last segment id is {}",
                    segmentId,
                    partitionId,
                    subpartitionId,
                    lastRequiredSegmentId);
            partitionLastRequiredSegmentId.get(partitionId).put(subpartitionId,segmentId);
            bufferReader.notifyRequiredSegment(segmentId);
        }
    }

    private boolean openReader(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId) {
        synchronized (lock) {
            if (numOpenedReaders.get() == numTotalReaders) {
                return true;
            }
        }
        CelebornChannelBufferReader bufferReader =
                checkNotNull(checkNotNull(bufferReaders.get(partitionId)).get(subpartitionId));
        if (bufferReader.isOpened()) {
            return true;
        }
        boolean openSuccess = false;
        try {
            openSuccess = openReaderInternal(bufferReader).get();
        } catch (Exception e) {
            recycleAllResources();
            ExceptionUtils.rethrow(e);
        }
        if (!openSuccess) {
            LOG.warn("The reader for {} {} is not opened", partitionId, subpartitionId);
            return false;
        }
        synchronized (lock) {
            numOpenedReaders.incrementAndGet();
        }
        bufferReader.setIsOpened(true);
        return true;
    }

    private CompletableFuture<Boolean> openReaderInternal(
            CelebornChannelBufferReader bufferReader) {
        CompletableFuture<Boolean> openReaderFuture = new CompletableFuture<>();
        if (!bufferReader.isOpened()) {
            try {
                bufferReader.open(0, openReaderFuture);
            } catch (Exception e) {
                LOG.error("Failed to open shuffle read client", e);
                return CompletableFuture.completedFuture(false);
            }
        }
        return openReaderFuture;
    }

    private void initBufferReaders() {
        for (int i = 0; i < shuffleDescriptors.size(); i++) {
            RemoteShuffleDescriptor remoteDescriptor = (RemoteShuffleDescriptor) shuffleDescriptors.get(i);
            ResultPartitionID resultPartitionID = remoteDescriptor.getResultPartitionID();
            ShuffleResourceDescriptor shuffleDescriptor =
                    remoteDescriptor.getShuffleResource().getMapPartitionShuffleDescriptor();
            TieredStoragePartitionId partitionId = new TieredStoragePartitionId(resultPartitionID);
            checkState(consumerSpecs.get(i).getPartitionId().equals(partitionId), "Wrong partition id.");
            TieredStorageSubpartitionId subpartitionId = consumerSpecs.get(i).getSubpartitionId();
            LOG.debug(
                    "create shuffle reader for descriptor {} {} {} {}",
                    gateIndex,
                    shuffleDescriptor,
                    partitionId,
                    subpartitionId);
            CelebornChannelBufferReader reader =
                    new CelebornChannelBufferReader(
                            shuffleClient,
                            shuffleDescriptor,
                            subpartitionId.getSubpartitionId(),
                            subpartitionId.getSubpartitionId(),
                            getDataListener(partitionId, subpartitionId),
                            getFailureListener(partitionId, subpartitionId));
            checkState(!bufferReaders.containsKey(partitionId) ||
                    !bufferReaders.get(partitionId).containsKey(subpartitionId), "Duplicate shuffle reader.");
            bufferReaders.computeIfAbsent(partitionId, partition -> new HashMap<>()).put(
                    subpartitionId, reader);
            numTotalReaders++;
        }
    }

    @GuardedBy("lock")
    private void healthCheck() throws IOException {
        if (closed) {
            recycleAllResources();
            throw new IOException("Input gate already closed.");
        }
        if (cause != null) {
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException(cause);
            }
        }
    }

    private void onBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            Buffer buffer) {
        boolean wasEmpty;
        synchronized (lock) {
            if (closed || cause != null) {
                buffer.recycleBuffer();
                recycleAllResources();
                throw new IllegalStateException("Input gate already closed or failed.");
            }
            Queue<Buffer> buffers =
                    receivedBuffers
                            .computeIfAbsent(partitionId, partition -> new HashMap<>())
                            .computeIfAbsent(subpartitionId, subpartition -> new LinkedList<>());
            wasEmpty = buffers.isEmpty();
            buffers.add(buffer);
            if (wasEmpty && !hasStart) {
                partitionsNeedNotifyAvailable.add(Tuple2.of(partitionId, subpartitionId));
                return;
            }
        }
        if (wasEmpty) {
            availabilityNotifier.notifyAvailable(partitionId, subpartitionId);
        }
    }

    private Consumer<ByteBuf> getDataListener(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId) {
        return byteBuf -> {
            Queue<Buffer> unpackedBuffers = null;
            try {
                unpackedBuffers = BufferPacker.unpack(byteBuf);
                while (!unpackedBuffers.isEmpty()) {
                    onBuffer(partitionId, subpartitionId, unpackedBuffers.poll());
                }
            } catch (Throwable throwable) {
                synchronized (lock) {
                    cause = cause == null ? throwable : cause;
                    availabilityNotifier.notifyAvailable(partitionId, subpartitionId);
                }
                if (unpackedBuffers != null) {
                    unpackedBuffers.forEach(Buffer::recycleBuffer);
                }
                recycleAllResources();
                LOG.error("Failed to process the received buffer.", throwable);
            }
        };
    }

    private Consumer<Throwable> getFailureListener(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId) {
        return throwable -> {
            synchronized (lock) {
                if (cause != null) {
                    return;
                }
                Class<?> clazz = PartitionUnRetryAbleException.class;
                if (throwable.getMessage() != null
                        && throwable.getMessage().contains(clazz.getName())) {
                    cause =
                            new PartitionNotFoundException(
                                    TieredStorageIdMappingUtils.convertId(partitionId));
                } else {
                    cause = throwable;
                }
                availabilityNotifier.notifyAvailable(partitionId, subpartitionId);
            }
        };
    }
}