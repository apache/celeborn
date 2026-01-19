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

import java.net.InetAddress;
import java.time.Duration;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;

import org.apache.celeborn.plugin.flink.utils.ReflectionUtils;

public class CelebornFlinkShimV119 extends CelebornFlinkShim {
  @Override
  protected BufferCompressor createBufferCompressorImpl(
      int networkBufferSize, String compressionCodec) {
    return new BufferCompressor(networkBufferSize, compressionCodec);
  }

  @Override
  protected BufferDecompressor createBufferDecompressorImpl(
      int networkBufferSize, String compressionCodec) {
    return new BufferDecompressor(networkBufferSize, compressionCodec);
  }

  @Override
  protected Duration getRequestSegmentsTimeoutImpl(Configuration configuration) {
    return Duration.ofMillis(
        configuration.get(
            NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
  }

  @Override
  protected InputChannel createFakeChannelImpl(
      RemoteShuffleInputGateDelegation inputGateDelegation, int channelIndex) {
    return new RemoteInputChannel(
        new SingleInputGate(
            inputGateDelegation.getTaskName(),
            inputGateDelegation.getGateIndex(),
            new IntermediateDataSetID(),
            ResultPartitionType.BLOCKING,
            1,
            (a, b, c) -> {},
            () -> null,
            null,
            ReflectionUtils.createEmptyImplementation(MemorySegmentProvider.class),
            0,
            new ThroughputCalculator(SystemClock.getInstance()),
            null),
        channelIndex,
        new ResultPartitionID(),
        new ResultSubpartitionIndexSet(new IndexRange(0, 0)),
        new ConnectionID(
            new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 1), 0),
        new LocalConnectionManager(),
        0,
        0,
        0,
        0,
        new SimpleCounter(),
        new SimpleCounter(),
        ReflectionUtils.createEmptyImplementation(ChannelStateWriter.class));
  }

  @Override
  protected Tuple2<Integer, Integer> getConsumedSubpartitionIndexRangeImpl(
      InputGateDeploymentDescriptor gateDescriptor) {
    IndexRange indexRange = gateDescriptor.getConsumedSubpartitionIndexRange();
    return Tuple2.of(indexRange.getStartIndex(), indexRange.getEndIndex());
  }

  @Override
  protected ResultPartitionBytesCounter createResultPartitionBytesCounterImpl(
      RemoteShuffleResultPartition remoteShuffleResultPartition) {
    org.apache.flink.runtime.io.network.metrics.ResultPartitionBytesCounter resultPartitionBytes =
        ReflectionUtils.readDeclaredFieldRecursiveNoThrow(
            remoteShuffleResultPartition, "resultPartitionBytes");
    return (bufferWithSubpartition, isBroadcast, readableBytes) -> {
      if (isBroadcast) {
        resultPartitionBytes.incAll(readableBytes);
      } else {
        resultPartitionBytes.inc(bufferWithSubpartition.getSubpartitionIndex(), readableBytes);
      }
    };
  }

  @Override
  protected Buffer.DataType getEndOfPartitionDataTypeImpl() {
    return Buffer.DataType.END_OF_PARTITION;
  }

  @Override
  protected PartitionDescriptor createPartitionDescriptorImpl(
      IntermediateDataSetID intermediateDataSetId, int partitionNum) {
    IntermediateResultPartitionID intermediateResultPartitionId =
        new IntermediateResultPartitionID(intermediateDataSetId, partitionNum);
    return new PartitionDescriptor(
        intermediateDataSetId,
        10,
        intermediateResultPartitionId,
        ResultPartitionType.BLOCKING,
        5,
        1,
        false,
        false,
        false);
  }

  @Override
  protected TaskInputsOutputsDescriptor createTaskInputsOutputsDescriptorImpl(
      int inputGateNums,
      Map<IntermediateDataSetID, Integer> inputChannelNums,
      Map<IntermediateDataSetID, Integer> partitionReuseCount,
      Map<IntermediateDataSetID, Integer> subpartitionNums,
      Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
      Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {
    return TaskInputsOutputsDescriptor.from(
        inputGateNums,
        inputChannelNums,
        partitionReuseCount,
        subpartitionNums,
        inputPartitionTypes,
        partitionTypes);
  }
}
