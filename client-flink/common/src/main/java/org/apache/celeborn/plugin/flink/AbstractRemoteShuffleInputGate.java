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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.metrics.groups.ShuffleIOMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.common.CelebornConf;

/** An abstract {@link IndexedInputGate} which ingest data from remote shuffle workers. */
public abstract class AbstractRemoteShuffleInputGate extends IndexedInputGate {

  public final RemoteShuffleInputGateDelegation inputGateDelegation;

  public AbstractRemoteShuffleInputGate(
      CelebornConf celebornConf,
      ShuffleIOOwnerContext ownerContext,
      int gateIndex,
      InputGateDeploymentDescriptor gateDescriptor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int numConcurrentReading,
      Map<Integer, ShuffleIOMetricGroup> shuffleIOMetricGroups) {
    Tuple2<Integer, Integer> indexRange = getConsumedSubpartitionIndexRange(gateDescriptor);
    inputGateDelegation =
        new RemoteShuffleInputGateDelegation(
            celebornConf,
            ownerContext,
            gateIndex,
            gateDescriptor,
            bufferPoolFactory,
            bufferDecompressor,
            numConcurrentReading,
            availabilityHelper,
            indexRange.f0,
            indexRange.f1,
            shuffleIOMetricGroups);
  }

  /** Setup gate and build network connections. */
  @Override
  public void setup() throws IOException {
    inputGateDelegation.setup();
  }

  /** Index of the gate of the corresponding computing task. */
  @Override
  public int getGateIndex() {
    return inputGateDelegation.getGateIndex();
  }

  /** Get number of input channels. A channel is a data flow from one shuffle worker. */
  @Override
  public int getNumberOfInputChannels() {
    return inputGateDelegation.getBufferReaders().size();
  }

  /** Whether reading is finished -- all channels are finished and cached buffers are drained. */
  @Override
  public boolean isFinished() {
    return inputGateDelegation.isFinished();
  }

  @Override
  public Optional<BufferOrEvent> getNext() {
    throw new UnsupportedOperationException("Not implemented (DataSet API is not supported).");
  }

  /** Poll a received {@link BufferOrEvent}. */
  @Override
  public Optional<BufferOrEvent> pollNext() throws IOException {
    return inputGateDelegation.pollNext();
  }

  /** Close all reading channels inside this {@link AbstractRemoteShuffleInputGate}. */
  @Override
  public void close() throws Exception {
    inputGateDelegation.close();
  }

  /** Get {@link InputChannelInfo}s of this {@link AbstractRemoteShuffleInputGate}. */
  @Override
  public List<InputChannelInfo> getChannelInfos() {
    return inputGateDelegation.getChannelsInfo();
  }

  @Override
  public void requestPartitions() {
    // do-nothing
  }

  @Override
  public void checkpointStarted(CheckpointBarrier barrier) {
    // do-nothing.
  }

  @Override
  public void checkpointStopped(long cancelledCheckpointId) {
    // do-nothing.
  }

  @Override
  public void triggerDebloating() {
    // do-nothing.
  }

  @Override
  public List<InputChannelInfo> getUnfinishedChannels() {
    return Collections.emptyList();
  }

  @Override
  public EndOfDataStatus hasReceivedEndOfData() {
    if (inputGateDelegation.getPendingEndOfDataEvents() > 0) {
      return EndOfDataStatus.NOT_END_OF_DATA;
    } else {
      // Keep compatibility with streaming mode.
      return EndOfDataStatus.DRAINED;
    }
  }

  @Override
  public void finishReadRecoveredState() {
    // do-nothing.
  }

  @Override
  public abstract InputChannel getChannel(int channelIndex);

  @Override
  public void sendTaskEvent(TaskEvent event) {
    throw new FlinkRuntimeException("Method should not be called.");
  }

  public void resumeGateConsumption() throws IOException {
    throw new FlinkRuntimeException("Method should not be called.");
  }

  @Override
  public void resumeConsumption(InputChannelInfo channelInfo) {
    throw new FlinkRuntimeException("Method should not be called.");
  }

  @Override
  public void acknowledgeAllRecordsProcessed(InputChannelInfo inputChannelInfo) {}

  @Override
  public CompletableFuture<Void> getStateConsumedFuture() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return String.format(
        "ReadGate [owning task: %s, gate index: %d, descriptor: %s]",
        inputGateDelegation.getTaskName(),
        inputGateDelegation.getGateIndex(),
        inputGateDelegation.getGateDescriptor().toString());
  }

  public abstract Tuple2<Integer, Integer> getConsumedSubpartitionIndexRange(
      InputGateDeploymentDescriptor gateDescriptor);
}
