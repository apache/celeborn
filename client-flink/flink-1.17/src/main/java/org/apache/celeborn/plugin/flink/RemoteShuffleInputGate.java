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
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.ShuffleIOMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.common.CelebornConf;

/** A {@link IndexedInputGate} which ingest data from remote shuffle workers. */
public class RemoteShuffleInputGate extends AbstractRemoteShuffleInputGate {

  public RemoteShuffleInputGate(
      CelebornConf celebornConf,
      ShuffleIOOwnerContext ownerContext,
      int gateIndex,
      InputGateDeploymentDescriptor gateDescriptor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int numConcurrentReading,
      Map<Integer, ShuffleIOMetricGroup> shuffleIOMetricGroups) {
    super(
        celebornConf,
        ownerContext,
        gateIndex,
        gateDescriptor,
        bufferPoolFactory,
        bufferDecompressor,
        numConcurrentReading,
        shuffleIOMetricGroups);
  }

  @Override
  public InputChannel getChannel(int channelIndex) {
    return new FakedRemoteInputChannel(channelIndex);
  }

  @Override
  public Tuple2<Integer, Integer> getConsumedSubpartitionIndexRange(
      InputGateDeploymentDescriptor gateDescriptor) {
    IndexRange indexRange = gateDescriptor.getConsumedSubpartitionIndexRange();
    return Tuple2.of(indexRange.getStartIndex(), indexRange.getEndIndex());
  }

  /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
  private class FakedRemoteInputChannel extends RemoteInputChannel {
    FakedRemoteInputChannel(int channelIndex) {
      super(
          new SingleInputGate(
              inputGateDelegation.getTaskName(),
              inputGateDelegation.getGateIndex(),
              new IntermediateDataSetID(),
              ResultPartitionType.BLOCKING,
              new IndexRange(0, 0),
              1,
              (a, b, c) -> {},
              () -> null,
              null,
              new FakedMemorySegmentProvider(),
              0,
              new ThroughputCalculator(SystemClock.getInstance()),
              null),
          channelIndex,
          new ResultPartitionID(),
          0,
          new ConnectionID(
              new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 1),
              0),
          new LocalConnectionManager(),
          0,
          0,
          0,
          new SimpleCounter(),
          new SimpleCounter(),
          new FakedChannelStateWriter());
    }
  }

  /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
  private static class FakedMemorySegmentProvider implements MemorySegmentProvider {

    @Override
    public Collection<MemorySegment> requestUnpooledMemorySegments(int i) throws IOException {
      return null;
    }

    @Override
    public void recycleUnpooledMemorySegments(Collection<MemorySegment> collection)
        throws IOException {}
  }

  /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
  private static class FakedChannelStateWriter implements ChannelStateWriter {

    @Override
    public void start(long cpId, CheckpointOptions checkpointOptions) {}

    @Override
    public void addInputData(
        long cpId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> data) {}

    @Override
    public void addOutputData(
        long cpId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {}

    @Override
    public void addOutputDataFuture(
        long l,
        ResultSubpartitionInfo resultSubpartitionInfo,
        int i,
        CompletableFuture<List<Buffer>> completableFuture)
        throws IllegalArgumentException {}

    @Override
    public void finishInput(long checkpointId) {}

    @Override
    public void finishOutput(long checkpointId) {}

    @Override
    public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

    @Override
    public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
      return null;
    }

    @Override
    public void close() {}
  }
}
