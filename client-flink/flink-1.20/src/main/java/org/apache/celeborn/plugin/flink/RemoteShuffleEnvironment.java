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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.netty.NettyShuffleEnvironmentWrapper;

/**
 * The implementation of {@link ShuffleEnvironment} based on the remote shuffle service, providing
 * shuffle environment on flink TM side.
 */
public class RemoteShuffleEnvironment extends AbstractRemoteShuffleEnvironment
    implements ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> {

  /** Factory class to create {@link RemoteShuffleResultPartition}. */
  private final RemoteShuffleResultPartitionFactory resultPartitionFactory;

  private final RemoteShuffleInputGateFactory inputGateFactory;

  private final NettyShuffleEnvironmentWrapper shuffleEnvironmentWrapper;

  private final ConcurrentHashMap.KeySetView<IntermediateDataSetID, Boolean> nettyResultIds =
      ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap.KeySetView<IntermediateResultPartitionID, Boolean>
      nettyResultPartitionIds = ConcurrentHashMap.newKeySet();

  /**
   * @param networkBufferPool Network buffer pool for shuffle read and shuffle write.
   * @param resultPartitionManager A trivial {@link ResultPartitionManager}.
   * @param resultPartitionFactory Factory class to create {@link RemoteShuffleResultPartition}.
   * @param inputGateFactory Factory class to create {@link RemoteShuffleInputGate}.
   * @param nettyShuffleEnvironmentWrapper Wrapper class to create {@link NettyShuffleEnvironment}.
   */
  public RemoteShuffleEnvironment(
      NetworkBufferPool networkBufferPool,
      ResultPartitionManager resultPartitionManager,
      RemoteShuffleResultPartitionFactory resultPartitionFactory,
      RemoteShuffleInputGateFactory inputGateFactory,
      CelebornConf conf,
      NettyShuffleEnvironmentWrapper nettyShuffleEnvironmentWrapper) {

    super(networkBufferPool, resultPartitionManager, conf);
    this.resultPartitionFactory = resultPartitionFactory;
    this.inputGateFactory = inputGateFactory;
    this.shuffleEnvironmentWrapper = nettyShuffleEnvironmentWrapper;
  }

  @Override
  public ResultPartitionWriter createResultPartitionWriterInternal(
      ShuffleIOOwnerContext ownerContext,
      int index,
      ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor,
      CelebornConf conf) {
    if (resultPartitionDeploymentDescriptor.getShuffleDescriptor()
        instanceof RemoteShuffleDescriptor) {
      return resultPartitionFactory.create(
          ownerContext.getOwnerName(), index, resultPartitionDeploymentDescriptor, conf);
    } else {
      nettyResultIds.add(resultPartitionDeploymentDescriptor.getResultId());
      nettyResultPartitionIds.add(resultPartitionDeploymentDescriptor.getPartitionId());
      return shuffleEnvironmentWrapper
          .nettyResultPartitionFactory()
          .create(ownerContext.getOwnerName(), index, resultPartitionDeploymentDescriptor);
    }
  }

  @Override
  IndexedInputGate createInputGateInternal(
      ShuffleIOOwnerContext ownerContext,
      PartitionProducerStateProvider producerStateProvider,
      int gateIndex,
      InputGateDeploymentDescriptor igdd) {
    return nettyResultIds.contains(igdd.getConsumedResultId())
        ? shuffleEnvironmentWrapper
            .nettyInputGateFactory()
            .create(
                ownerContext,
                gateIndex,
                igdd,
                producerStateProvider,
                new InputChannelMetrics(
                    ownerContext.getInputGroup(), ownerContext.getParentGroup()))
        : inputGateFactory.create(ownerContext.getOwnerName(), gateIndex, igdd);
  }

  public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
    List<ResultPartitionID> resultPartitionIds =
        partitionIds.stream()
            .filter(partitionId -> nettyResultPartitionIds.contains(partitionId.getPartitionId()))
            .collect(Collectors.toList());
    if (!resultPartitionIds.isEmpty()) {
      shuffleEnvironmentWrapper
          .nettyShuffleEnvironment()
          .releasePartitionsLocally(resultPartitionIds);
    }
  }

  @VisibleForTesting
  RemoteShuffleResultPartitionFactory getResultPartitionFactory() {
    return resultPartitionFactory;
  }
}
