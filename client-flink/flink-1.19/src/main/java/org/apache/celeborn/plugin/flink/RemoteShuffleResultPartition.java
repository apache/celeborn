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

package org.apache.celeborn.plugin.flink;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkNotNull;
import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.*;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.plugin.flink.buffer.BufferWithSubpartition;
import org.apache.celeborn.plugin.flink.buffer.DataBuffer;
import org.apache.celeborn.plugin.flink.utils.BufferUtils;
import org.apache.celeborn.plugin.flink.utils.Utils;

/**
 * A {@link ResultPartition} which appends records and events to {@link DataBuffer} and after the
 * {@link DataBuffer} is full, all data in the {@link DataBuffer} will be copied and spilled to the
 * remote shuffle service in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link DataBuffer} will be spilled directly.
 */
public class RemoteShuffleResultPartition extends ResultPartition {

  private final RemoteShuffleResultPartitionDelegation delegation;

  private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

  public RemoteShuffleResultPartition(
      String owningTaskName,
      int partitionIndex,
      ResultPartitionID partitionId,
      ResultPartitionType partitionType,
      int numSubpartitions,
      int numTargetKeyGroups,
      int networkBufferSize,
      ResultPartitionManager partitionManager,
      @Nullable BufferCompressor bufferCompressor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      RemoteShuffleOutputGate outputGate) {

    super(
        owningTaskName,
        partitionIndex,
        partitionId,
        partitionType,
        numSubpartitions,
        numTargetKeyGroups,
        partitionManager,
        bufferCompressor,
        bufferPoolFactory);

    delegation =
        new RemoteShuffleResultPartitionDelegation(
            networkBufferSize, outputGate, this::updateStatistics, numSubpartitions);
    this.bufferPoolFactory = bufferPoolFactory;
  }

  @Override
  public void setup() throws IOException {
    // We can't call the `setup` method of the base class, otherwise it will cause a partition leak.
    // The reason is that this partition will be registered to the partition manager during
    // `super.setup()`.
    // Since this is a cluster/remote partition(i.e. resources are not stored on the Flink TM),
    // Flink does not trigger the resource releasing over TM. Therefore, the partition object is
    // leaked.
    // So we copy the logic of `setup` but don't register partition to partition manager.
    checkState(
        this.bufferPool == null,
        "Bug in result partition setup logic: Already registered buffer pool.");
    this.bufferPool = checkNotNull(bufferPoolFactory.get());
    // this is an empty method, but still call it in case of we implement it in the future.
    setupInternal();
    BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);
    delegation.setup(
        bufferPool, bufferCompressor, this::canBeCompressed, this::checkInProduceState);
  }

  @Override
  protected void setupInternal() {
    // do not need to implement
  }

  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    delegation.emit(record, targetSubpartition, DataType.DATA_BUFFER, false);
  }

  @Override
  public void broadcastRecord(ByteBuffer record) throws IOException {
    delegation.broadcast(record, DataType.DATA_BUFFER);
  }

  @Override
  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
    try {
      ByteBuffer serializedEvent = buffer.getNioBufferReadable();
      delegation.broadcast(serializedEvent, buffer.getDataType());
    } finally {
      buffer.recycleBuffer();
    }
  }

  @Override
  public void alignedBarrierTimeout(long l) {}

  @Override
  public void abortCheckpoint(long l, CheckpointException e) {}

  @Override
  public void finish() throws IOException {
    Utils.checkState(!isReleased(), "Result partition is already released.");
    broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
    delegation.finish();
    super.finish();
  }

  @Override
  public synchronized void close() {
    delegation.close(super::close);
  }

  @Override
  protected void releaseInternal() {
    // no-op
  }

  @Override
  public void flushAll() {
    delegation.flushAll();
  }

  @Override
  public void flush(int subpartitionIndex) {
    flushAll();
  }

  @Override
  public CompletableFuture<?> getAvailableFuture() {
    return AVAILABLE;
  }

  @Override
  public int getNumberOfQueuedBuffers() {
    return 0;
  }

  @Override
  public long getSizeOfQueuedBuffersUnsafe() {
    return 0;
  }

  @Override
  public int getNumberOfQueuedBuffers(int targetSubpartition) {
    return 0;
  }

  @Override
  public ResultSubpartitionView createSubpartitionView(
      int index, BufferAvailabilityListener availabilityListener) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public void notifyEndOfData(StopMode mode) throws IOException {
    if (!delegation.isEndOfDataNotified()) {
      broadcastEvent(new EndOfData(mode), false);
      delegation.setEndOfDataNotified(true);
    }
  }

  @Override
  public CompletableFuture<Void> getAllDataProcessedFuture() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return "ResultPartition "
        + partitionId.toString()
        + " ["
        + partitionType
        + ", "
        + numSubpartitions
        + " subpartitions, shuffle-descriptor: "
        + delegation.getOutputGate().getShuffleDesc()
        + "]";
  }

  @VisibleForTesting
  public RemoteShuffleResultPartitionDelegation getDelegation() {
    return delegation;
  }

  public void updateStatistics(BufferWithSubpartition bufferWithSubpartition, boolean isBroadcast) {
    numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
    long readableBytes =
        (long) bufferWithSubpartition.getBuffer().readableBytes() - BufferUtils.HEADER_LENGTH;
    if (isBroadcast) {
      resultPartitionBytes.incAll(readableBytes);
    } else {
      resultPartitionBytes.inc(bufferWithSubpartition.getSubpartitionIndex(), readableBytes);
    }
    numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
  }
}
