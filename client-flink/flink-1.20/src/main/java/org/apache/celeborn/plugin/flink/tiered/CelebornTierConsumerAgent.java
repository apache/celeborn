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

import static org.apache.celeborn.plugin.flink.utils.Utils.checkArgument;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkNotNull;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.UnknownTierShuffleDescriptor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class CelebornTierConsumerAgent implements TierConsumerAgent {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornTierConsumerAgent.class);

  private final CelebornConf conf;

  private final int gateIndex;

  private final List<TieredStorageConsumerSpec> consumerSpecs;

  private final List<TierShuffleDescriptor> shuffleDescriptors;

  private final TransferBufferPool transferBufferPool;

  private final Map<
          TieredStoragePartitionId, Map<TieredStorageSubpartitionId, CelebornChannelBufferReader>>
      bufferReaders;

  private final Map<ResultPartitionID, Map<Integer, Integer>> partitionLastRequiredSegmentId;

  /** Lock to protect {@link #receivedBuffers} and {@link #cause} and {@link #closed}, etc. */
  private final Object lock = new Object();

  /** Received buffers from remote shuffle worker. It's consumed by upper computing task. */
  @GuardedBy("lock")
  private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Queue<Buffer>>>
      receivedBuffers;

  @GuardedBy("lock")
  private final Set<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>>
      subPartitionsNeedNotifyAvailable;

  @GuardedBy("lock")
  private final AtomicInteger numOpenedReaders = new AtomicInteger(0);

  @GuardedBy("lock")
  private boolean hasStart = false;

  @GuardedBy("lock")
  private Throwable cause;

  /** Whether this remote input gate has been closed or not. */
  @GuardedBy("lock")
  private boolean closed;

  private FlinkShuffleClientImpl shuffleClient;

  private AvailabilityNotifier availabilityNotifier;

  private TieredStorageMemoryManager memoryManager;

  private final int bufferSizeBytes;

  public CelebornTierConsumerAgent(
      CelebornConf conf,
      List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
      List<TierShuffleDescriptor> shuffleDescriptors,
      int bufferSizeBytes) {
    checkArgument(shuffleDescriptors.size() > 0, "Wrong shuffle descriptors size.");
    checkArgument(
        tieredStorageConsumerSpecs.size() == shuffleDescriptors.size(),
        "Wrong consumer spec size.");
    this.conf = conf;
    this.gateIndex = tieredStorageConsumerSpecs.get(0).getGateIndex();
    this.consumerSpecs = tieredStorageConsumerSpecs;
    this.shuffleDescriptors = shuffleDescriptors;
    this.partitionLastRequiredSegmentId = new HashMap<>();
    this.bufferReaders = new HashMap<>();
    this.receivedBuffers = new HashMap<>();
    this.subPartitionsNeedNotifyAvailable = new HashSet<>();
    this.transferBufferPool = new TransferBufferPool(Collections.emptySet());
    this.bufferSizeBytes = bufferSizeBytes;
    for (TierShuffleDescriptor shuffleDescriptor : shuffleDescriptors) {
      if (!(shuffleDescriptor instanceof RemoteShuffleDescriptor)) {
        continue;
      }
      initShuffleClient((RemoteShuffleDescriptor) shuffleDescriptor);
      break;
    }
    initBufferReaders();
  }

  @Override
  public void setup(TieredStorageMemoryManager memoryManager) {
    this.memoryManager = memoryManager;
    for (Map<TieredStorageSubpartitionId, CelebornChannelBufferReader> subPartitionReaders :
        bufferReaders.values()) {
      subPartitionReaders.forEach((partitionId, reader) -> reader.setup(memoryManager));
    }
  }

  @Override
  public void start() {
    Set<Tuple2<TieredStoragePartitionId, TieredStorageSubpartitionId>> needNotifyAvailable;
    synchronized (lock) {
      needNotifyAvailable = new HashSet<>(subPartitionsNeedNotifyAvailable);
      subPartitionsNeedNotifyAvailable.clear();
      hasStart = true;
    }
    try {
      needNotifyAvailable.forEach(
          partitionIdTuple -> notifyAvailable(partitionIdTuple.f0, partitionIdTuple.f1));
    } catch (Throwable t) {
      LOG.error("Error occurred when notifying sub partitions available", t);
      recycleAllResources();
      ExceptionUtils.rethrow(t);
    }
    needNotifyAvailable.clear();

    // Require segment 0 when starting the client
    for (TieredStorageConsumerSpec spec : consumerSpecs) {
      for (int subpartitionId : spec.getSubpartitionIds().values()) {
        CelebornChannelBufferReader bufferReader =
            getBufferReader(spec.getPartitionId(), new TieredStorageSubpartitionId(subpartitionId));
        if (bufferReader == null) {
          continue;
        }
        boolean openReaderSuccess = openReader(bufferReader);
        if (!openReaderSuccess) {
          LOG.error("Failed to open reader.");
          recycleAllResources();
          ExceptionUtils.rethrow(new IOException("Failed to open reader."));
        }
        notifyRequiredSegmentIfNeeded(
            spec.getPartitionId().getPartitionID(), subpartitionId, 0, bufferReader);
      }
    }
  }

  @Override
  public int peekNextBufferSubpartitionId(
      TieredStoragePartitionId tieredStoragePartitionId,
      ResultSubpartitionIndexSet resultSubpartitionIndexSet) {
    synchronized (lock) {
      try {
        healthCheck();
      } catch (Throwable throwable) {
        LOG.error("Failed to check health.", throwable);
        recycleAllResources();
        ExceptionUtils.rethrow(throwable);
      }
      Map<TieredStorageSubpartitionId, Queue<Buffer>> subPartitionReceivedBuffers =
          receivedBuffers.get(tieredStoragePartitionId);
      if (subPartitionReceivedBuffers == null) {
        return -1;
      }
      for (int subPartitionIndex = resultSubpartitionIndexSet.getStartIndex();
          subPartitionIndex <= resultSubpartitionIndexSet.getEndIndex();
          subPartitionIndex++) {
        Queue<Buffer> buffers =
            subPartitionReceivedBuffers.get(new TieredStorageSubpartitionId(subPartitionIndex));
        if (buffers != null && !buffers.isEmpty()) {
          return subPartitionIndex;
        }
      }
    }
    return -1;
  }

  @Override
  public Optional<Buffer> getNextBuffer(
      TieredStoragePartitionId tieredStoragePartitionId,
      TieredStorageSubpartitionId tieredStorageSubpartitionId,
      int segmentId) {
    synchronized (lock) {
      try {
        healthCheck();
      } catch (Throwable throwable) {
        LOG.error("Failed to check health.", throwable);
        recycleAllResources();
        ExceptionUtils.rethrow(throwable);
      }
    }
    if (!bufferReaders.containsKey(tieredStoragePartitionId)
        || !bufferReaders.get(tieredStoragePartitionId).containsKey(tieredStorageSubpartitionId)) {
      return Optional.empty();
    }
    try {
      boolean openReaderSuccess = openReader(tieredStoragePartitionId, tieredStorageSubpartitionId);
      if (!openReaderSuccess) {
        return Optional.empty();
      }
    } catch (Throwable throwable) {
      LOG.error("Failed to open reader.", throwable);
      recycleAllResources();
      ExceptionUtils.rethrow(throwable);
    }
    try {
      synchronized (lock) {
        CelebornChannelBufferReader bufferReader =
            getBufferReader(tieredStoragePartitionId, tieredStorageSubpartitionId);
        ResultPartitionID partitionId = tieredStoragePartitionId.getPartitionID();
        notifyRequiredSegmentIfNeeded(
            partitionId, tieredStorageSubpartitionId.getSubpartitionId(), segmentId, bufferReader);
        Map<TieredStorageSubpartitionId, Queue<Buffer>> partitionBuffers =
            receivedBuffers.get(tieredStoragePartitionId);
        if (partitionBuffers == null || partitionBuffers.size() == 0) {
          return Optional.empty();
        }
        Queue<Buffer> subPartitionBuffers = partitionBuffers.get(tieredStorageSubpartitionId);
        if (subPartitionBuffers == null || subPartitionBuffers.size() == 0) {
          return Optional.empty();
        }
        healthCheck();
        return Optional.ofNullable(subPartitionBuffers.poll());
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
    LOG.info("Registered availability notifier for gate {}.", gateIndex);
  }

  @Override
  public void updateTierShuffleDescriptor(
      TieredStoragePartitionId tieredStoragePartitionId,
      TieredStorageInputChannelId tieredStorageInputChannelId,
      TieredStorageSubpartitionId subpartitionId,
      TierShuffleDescriptor tierShuffleDescriptor) {
    if (!(tierShuffleDescriptor instanceof RemoteShuffleDescriptor)) {
      return;
    }
    RemoteShuffleDescriptor remoteShuffleDescriptor =
        (RemoteShuffleDescriptor) tierShuffleDescriptor;
    if (shuffleClient == null) {
      initShuffleClient(remoteShuffleDescriptor);
    }
    checkState(
        remoteShuffleDescriptor
            .getResultPartitionID()
            .equals(tieredStoragePartitionId.getPartitionID()),
        "Wrong result partition id.");
    ResultSubpartitionIndexSet subpartitionIndexSet =
        new ResultSubpartitionIndexSet(subpartitionId.getSubpartitionId());
    if (!bufferReaders.containsKey(tieredStoragePartitionId)
        || !bufferReaders.get(tieredStoragePartitionId).containsKey(subpartitionId)) {
      ShuffleResourceDescriptor shuffleDescriptor =
          remoteShuffleDescriptor.getShuffleResource().getMapPartitionShuffleDescriptor();
      createBufferReader(
          shuffleDescriptor,
          tieredStoragePartitionId,
          tieredStorageInputChannelId,
          subpartitionIndexSet);
      CelebornChannelBufferReader bufferReader =
          checkNotNull(getBufferReader(tieredStoragePartitionId, subpartitionId));
      bufferReader.setup(checkNotNull(memoryManager));
      openReader(bufferReader);
    }
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

  private void initShuffleClient(RemoteShuffleDescriptor remoteShuffleDescriptor) {
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
              new UserIdentifier("default", "default"),
              bufferSizeBytes);
    } catch (DriverChangedException e) {
      // If jobmanager failover the whole job will restart, then partition writer will refresh
      // FlinkShuffleClientImpl instance.
      // so InputGate can directly use new FlinkShuffleClientImpl instance.
      throw new RuntimeException(e.getMessage());
    }
  }

  private CelebornChannelBufferReader getBufferReader(
      TieredStoragePartitionId tieredStoragePartitionId,
      TieredStorageSubpartitionId tieredStorageSubpartitionId) {
    return bufferReaders.get(tieredStoragePartitionId).get(tieredStorageSubpartitionId);
  }

  private void recycleAllResources() {
    List<Buffer> buffersToRecycle = new ArrayList<>();
    for (Map<TieredStorageSubpartitionId, CelebornChannelBufferReader> subPartitionReaders :
        bufferReaders.values()) {
      subPartitionReaders.values().forEach(CelebornChannelBufferReader::close);
    }
    synchronized (lock) {
      for (Map<TieredStorageSubpartitionId, Queue<Buffer>> subPartitionMap :
          receivedBuffers.values()) {
        buffersToRecycle.addAll(
            subPartitionMap.values().stream()
                .flatMap(Queue::stream)
                .collect(Collectors.toCollection(LinkedList::new)));
      }
      receivedBuffers.clear();
      bufferReaders.clear();
      partitionLastRequiredSegmentId.clear();
      availabilityNotifier = null;
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
      int subPartitionId,
      int segmentId,
      CelebornChannelBufferReader bufferReader) {
    Integer lastRequiredSegmentId =
        partitionLastRequiredSegmentId
            .computeIfAbsent(partitionId, partition -> new HashMap<>())
            .computeIfAbsent(subPartitionId, id -> -1);
    if (segmentId >= 0 && segmentId != lastRequiredSegmentId) {
      LOG.debug(
          "Notify required segment id {} for {} {}, the last segment id is {}",
          segmentId,
          partitionId,
          subPartitionId,
          lastRequiredSegmentId);
      partitionLastRequiredSegmentId.get(partitionId).put(subPartitionId, segmentId);
      if (!bufferReader.notifyRequiredSegment(segmentId, subPartitionId)) {
        partitionLastRequiredSegmentId
            .computeIfAbsent(partitionId, partition -> new HashMap<>())
            .putIfAbsent(subPartitionId, lastRequiredSegmentId);
      }
    }
  }

  private boolean openReader(
      TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subPartitionId) {
    CelebornChannelBufferReader bufferReader =
        checkNotNull(checkNotNull(bufferReaders.get(partitionId)).get(subPartitionId));
    return openReader(bufferReader);
  }

  private boolean openReader(CelebornChannelBufferReader bufferReader) {
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
      LOG.warn("The reader {} is not opened", bufferReader);
      return false;
    }
    synchronized (lock) {
      numOpenedReaders.incrementAndGet();
    }
    bufferReader.setIsOpened(true);
    return true;
  }

  private CompletableFuture<Boolean> openReaderInternal(CelebornChannelBufferReader bufferReader) {
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
      if (shuffleDescriptors.get(i) instanceof UnknownTierShuffleDescriptor) {
        continue;
      }
      RemoteShuffleDescriptor remoteDescriptor =
          (RemoteShuffleDescriptor) shuffleDescriptors.get(i);
      ResultPartitionID resultPartitionID = remoteDescriptor.getResultPartitionID();
      ShuffleResourceDescriptor shuffleDescriptor =
          remoteDescriptor.getShuffleResource().getMapPartitionShuffleDescriptor();
      TieredStoragePartitionId partitionId = new TieredStoragePartitionId(resultPartitionID);
      checkState(consumerSpecs.get(i).getPartitionId().equals(partitionId), "Wrong partition id.");
      ResultSubpartitionIndexSet subPartitionIdSet = consumerSpecs.get(i).getSubpartitionIds();
      LOG.debug(
          "create shuffle reader for descriptor {} {} {} start id {} end id {}",
          gateIndex,
          shuffleDescriptor,
          partitionId,
          subPartitionIdSet.getStartIndex(),
          subPartitionIdSet.getEndIndex());
      createBufferReader(
          shuffleDescriptor,
          partitionId,
          consumerSpecs.get(i).getInputChannelId(),
          subPartitionIdSet);
    }
  }

  private void createBufferReader(
      ShuffleResourceDescriptor shuffleDescriptor,
      TieredStoragePartitionId partitionId,
      TieredStorageInputChannelId inputChannelId,
      ResultSubpartitionIndexSet subPartitionIdSet) {

    CelebornChannelBufferReader reader =
        new CelebornChannelBufferReader(
            shuffleClient,
            shuffleDescriptor,
            inputChannelId,
            subPartitionIdSet.getStartIndex(),
            subPartitionIdSet.getEndIndex(),
            getDataListener(partitionId),
            getFailureListener(partitionId));

    for (int id = subPartitionIdSet.getStartIndex(); id <= subPartitionIdSet.getEndIndex(); id++) {
      TieredStorageSubpartitionId subPartitionId = new TieredStorageSubpartitionId(id);
      checkState(
          !bufferReaders.containsKey(partitionId)
              || !bufferReaders.get(partitionId).containsKey(subPartitionId),
          "Duplicate shuffle reader.");
      bufferReaders
          .computeIfAbsent(partitionId, partition -> new HashMap<>())
          .put(subPartitionId, reader);
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
      TieredStorageSubpartitionId subPartitionId,
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
              .computeIfAbsent(subPartitionId, subpartition -> new LinkedList<>());
      wasEmpty = buffers.isEmpty();
      buffers.add(buffer);
      if (wasEmpty && !hasStart) {
        subPartitionsNeedNotifyAvailable.add(Tuple2.of(partitionId, subPartitionId));
        return;
      }
    }
    if (wasEmpty) {
      notifyAvailable(partitionId, subPartitionId);
    }
  }

  private BiConsumer<ByteBuf, TieredStorageSubpartitionId> getDataListener(
      TieredStoragePartitionId partitionId) {
    return (byteBuf, subPartitionId) -> {
      Queue<Buffer> unpackedBuffers = null;
      try {
        unpackedBuffers = BufferPacker.unpack(byteBuf);
        while (!unpackedBuffers.isEmpty()) {
          onBuffer(partitionId, subPartitionId, unpackedBuffers.poll());
        }
      } catch (Throwable throwable) {
        synchronized (lock) {
          LOG.error(
              "Failed to process the received buffer. " + (cause == null ? "" : cause), throwable);
          cause = cause == null ? throwable : cause;
        }
        notifyAvailable(partitionId, subPartitionId);
        if (unpackedBuffers != null) {
          unpackedBuffers.forEach(Buffer::recycleBuffer);
        }
        recycleAllResources();
      }
    };
  }

  private BiConsumer<Throwable, TieredStorageSubpartitionId> getFailureListener(
      TieredStoragePartitionId partitionId) {
    return (throwable, subPartitionId) -> {
      synchronized (lock) {
        if (cause != null) {
          return;
        }
        Class<?> clazz = PartitionUnRetryAbleException.class;
        if (throwable.getMessage() != null && throwable.getMessage().contains(clazz.getName())) {
          cause =
              new PartitionNotFoundException(TieredStorageIdMappingUtils.convertId(partitionId));
        } else {
          LOG.error("The consumer agent received an exception.", throwable);
          cause = throwable;
        }
      }
      notifyAvailable(partitionId, subPartitionId);
    };
  }

  private void notifyAvailable(
      TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subPartitionId) {
    Map<TieredStorageSubpartitionId, CelebornChannelBufferReader> subPartitionReaders =
        bufferReaders.get(partitionId);
    if (subPartitionReaders != null) {
      CelebornChannelBufferReader channelBufferReader = subPartitionReaders.get(subPartitionId);
      if (channelBufferReader != null) {
        availabilityNotifier.notifyAvailable(partitionId, channelBufferReader.getInputChannelId());
      }
    }
  }
}
