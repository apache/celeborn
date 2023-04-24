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

import static org.apache.celeborn.plugin.flink.utils.Utils.checkState;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;

import org.apache.celeborn.common.CelebornConf;

/**
 * The implementation of {@link ShuffleEnvironment} based on the remote shuffle service, providing
 * shuffle environment on flink TM side.
 */
public class RemoteShuffleEnvironment extends AbstractRemoteShuffleEnvironment
    implements ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> {

  /** Factory class to create {@link RemoteShuffleResultPartition}. */
  private final RemoteShuffleResultPartitionFactory resultPartitionFactory;

  private final RemoteShuffleInputGateFactory inputGateFactory;

  /**
   * @param networkBufferPool Network buffer pool for shuffle read and shuffle write.
   * @param resultPartitionManager A trivial {@link ResultPartitionManager}.
   * @param resultPartitionFactory Factory class to create {@link RemoteShuffleResultPartition}. //
   *     * @param inputGateFactory Factory class to create {@link RemoteShuffleInputGate}.
   */
  public RemoteShuffleEnvironment(
      NetworkBufferPool networkBufferPool,
      ResultPartitionManager resultPartitionManager,
      RemoteShuffleResultPartitionFactory resultPartitionFactory,
      RemoteShuffleInputGateFactory inputGateFactory,
      CelebornConf conf) {

    super(networkBufferPool, resultPartitionManager, conf);
    this.resultPartitionFactory = resultPartitionFactory;
    this.inputGateFactory = inputGateFactory;
  }

  @Override
  public List<ResultPartitionWriter> createResultPartitionWriters(
      ShuffleIOOwnerContext ownerContext,
      List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {

    synchronized (lock) {
      checkState(!isClosed, "The RemoteShuffleEnvironment has already been shut down.");

      ResultPartitionWriter[] resultPartitions =
          new ResultPartitionWriter[resultPartitionDeploymentDescriptors.size()];
      for (int index = 0; index < resultPartitions.length; index++) {
        resultPartitions[index] =
            resultPartitionFactory.create(
                ownerContext.getOwnerName(), index,
                resultPartitionDeploymentDescriptors.get(index), conf);
      }
      return Arrays.asList(resultPartitions);
    }
  }

  @Override
  public List<IndexedInputGate> createInputGates(
      ShuffleIOOwnerContext ownerContext,
      PartitionProducerStateProvider producerStateProvider,
      List<InputGateDeploymentDescriptor> inputGateDescriptors) {
    synchronized (lock) {
      checkState(!isClosed, "The RemoteShuffleEnvironment has already been shut down.");

      IndexedInputGate[] inputGates = new IndexedInputGate[inputGateDescriptors.size()];
      for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
        InputGateDeploymentDescriptor igdd = inputGateDescriptors.get(gateIndex);
        IndexedInputGate inputGate =
            inputGateFactory.create(ownerContext.getOwnerName(), gateIndex, igdd);
        inputGates[gateIndex] = inputGate;
      }
      return Arrays.asList(inputGates);
    }
  }

  @VisibleForTesting
  RemoteShuffleResultPartitionFactory getResultPartitionFactory() {
    return resultPartitionFactory;
  }
}
