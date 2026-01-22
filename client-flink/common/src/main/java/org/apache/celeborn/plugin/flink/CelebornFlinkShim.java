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

import java.time.Duration;
import java.util.Map;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

/** Shim for Celeborn and various versions of Flink. The shim is loaded via a ServiceLoader. */
public abstract class CelebornFlinkShim {

  private static volatile CelebornFlinkShim INSTANCE;

  // Exposed production shims

  public static BufferCompressor createBufferCompressor(
      int networkBufferSize, String compressionCodec) {
    return getInstance().createBufferCompressorImpl(networkBufferSize, compressionCodec);
  }

  public static BufferDecompressor createBufferDecompressor(
      int networkBufferSize, String compressionCodec) {
    return getInstance().createBufferDecompressorImpl(networkBufferSize, compressionCodec);
  }

  public static Duration getRequestSegmentsTimeout(Configuration configuration) {
    return getInstance().getRequestSegmentsTimeoutImpl(configuration);
  }

  public static InputChannel createFakeChannel(
      RemoteShuffleInputGateDelegation inputGateDelegation, int channelIndex) {
    return getInstance().createFakeChannelImpl(inputGateDelegation, channelIndex);
  }

  public static Tuple2<Integer, Integer> getConsumedSubpartitionIndexRange(
      InputGateDeploymentDescriptor gateDescriptor) {
    return getInstance().getConsumedSubpartitionIndexRangeImpl(gateDescriptor);
  }

  public static ResultPartitionBytesCounter createResultPartitionBytesCounter(
      RemoteShuffleResultPartition remoteShuffleResultPartition) {
    return getInstance().createResultPartitionBytesCounterImpl(remoteShuffleResultPartition);
  }

  // Exposed test shims

  @VisibleForTesting
  public static Buffer.DataType getEndOfPartitionDataType() {
    return getInstance().getEndOfPartitionDataTypeImpl();
  }

  @VisibleForTesting
  public static PartitionDescriptor createPartitionDescriptor(
      IntermediateDataSetID intermediateDataSetId, int partitionNum) {
    return getInstance().createPartitionDescriptorImpl(intermediateDataSetId, partitionNum);
  }

  @VisibleForTesting
  public static TaskInputsOutputsDescriptor createTaskInputsOutputsDescriptor(
      int inputGateNums,
      Map<IntermediateDataSetID, Integer> inputChannelNums,
      Map<IntermediateDataSetID, Integer> partitionReuseCount,
      Map<IntermediateDataSetID, Integer> subpartitionNums,
      Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
      Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {
    return getInstance()
        .createTaskInputsOutputsDescriptorImpl(
            inputGateNums,
            inputChannelNums,
            partitionReuseCount,
            subpartitionNums,
            inputPartitionTypes,
            partitionTypes);
  }

  // Loading and management methods

  @VisibleForTesting
  public static boolean isShimLoaded() {
    try {
      getInstance();
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  protected static CelebornFlinkShim getInstance() {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    synchronized (CelebornFlinkShim.class) {
      java.util.ServiceLoader<CelebornFlinkShim> loader =
          java.util.ServiceLoader.load(CelebornFlinkShim.class);
      for (CelebornFlinkShim shim : loader) {
        if (INSTANCE == null) {
          INSTANCE = shim;
        } else {
          throw new IllegalStateException(
              "Multiple CelebornFlinkShim implementations found in classpath:"
                  + INSTANCE.getClass().getSimpleName()
                  + ", "
                  + shim.getClass().getSimpleName());
        }
      }
    }
    if (INSTANCE != null) {
      return INSTANCE;
    } else {
      throw new IllegalStateException("No CelebornFlinkShim implementation found in classpath");
    }
  }

  // Implementations

  protected abstract BufferCompressor createBufferCompressorImpl(
      int networkBufferSize, String compressionCodec);

  protected abstract BufferDecompressor createBufferDecompressorImpl(
      int networkBufferSize, String compressionCodec);

  protected abstract Duration getRequestSegmentsTimeoutImpl(Configuration configuration);

  protected abstract InputChannel createFakeChannelImpl(
      RemoteShuffleInputGateDelegation inputGateDelegation, int channelIndex);

  protected abstract Tuple2<Integer, Integer> getConsumedSubpartitionIndexRangeImpl(
      InputGateDeploymentDescriptor gateDescriptor);

  protected abstract ResultPartitionBytesCounter createResultPartitionBytesCounterImpl(
      RemoteShuffleResultPartition remoteShuffleResultPartition);

  protected abstract Buffer.DataType getEndOfPartitionDataTypeImpl();

  protected abstract PartitionDescriptor createPartitionDescriptorImpl(
      IntermediateDataSetID intermediateDataSetId, int partitionNum);

  protected abstract TaskInputsOutputsDescriptor createTaskInputsOutputsDescriptorImpl(
      int inputGateNums,
      Map<IntermediateDataSetID, Integer> inputChannelNums,
      Map<IntermediateDataSetID, Integer> partitionReuseCount,
      Map<IntermediateDataSetID, Integer> subpartitionNums,
      Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes,
      Map<IntermediateDataSetID, ResultPartitionType> partitionTypes);
}
