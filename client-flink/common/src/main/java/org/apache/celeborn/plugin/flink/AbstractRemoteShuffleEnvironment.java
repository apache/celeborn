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
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;

/**
 * The implementation of {@link ShuffleEnvironment} based on the remote shuffle service, providing
 * shuffle environment on flink TM side.
 */
public abstract class AbstractRemoteShuffleEnvironment {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractRemoteShuffleEnvironment.class);

  /** Network buffer pool for shuffle read and shuffle write. */
  protected final NetworkBufferPool networkBufferPool;

  /** A trivial {@link ResultPartitionManager}. */
  protected final ResultPartitionManager resultPartitionManager;

  /** Whether the shuffle environment is closed. */
  protected boolean isClosed;

  protected final Object lock = new Object();

  protected final CelebornConf conf;

  /**
   * @param networkBufferPool Network buffer pool for shuffle read and shuffle write.
   * @param resultPartitionManager A trivial {@link ResultPartitionManager}.
   */
  public AbstractRemoteShuffleEnvironment(
      NetworkBufferPool networkBufferPool,
      ResultPartitionManager resultPartitionManager,
      CelebornConf conf) {

    this.networkBufferPool = networkBufferPool;
    this.resultPartitionManager = resultPartitionManager;
    this.conf = conf;
    this.isClosed = false;
  }

  public void close() {
    LOG.info("Close RemoteShuffleEnvironment.");
    synchronized (lock) {
      try {
        networkBufferPool.destroyAllBufferPools();
      } catch (Throwable t) {
        LOG.error("Close RemoteShuffleEnvironment failure.", t);
      }
      try {
        resultPartitionManager.shutdown();
      } catch (Throwable t) {
        LOG.error("Close RemoteShuffleEnvironment failure.", t);
      }
      try {
        networkBufferPool.destroy();
      } catch (Throwable t) {
        LOG.error("Close RemoteShuffleEnvironment failure.", t);
      }
      isClosed = true;
    }
  }

  public int start() throws IOException {
    synchronized (lock) {
      checkState(!isClosed, "The RemoteShuffleEnvironment has already been shut down.");
      LOG.info("Starting the network environment and its components.");
      // trivial value.
      return 1;
    }
  }

  public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo) {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  public ShuffleIOOwnerContext createShuffleIOOwnerContext(
      String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
    MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
    return new ShuffleIOOwnerContext(
        checkNotNull(ownerName),
        checkNotNull(executionAttemptID),
        parentGroup,
        nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
        nettyGroup.addGroup(METRIC_GROUP_INPUT));
  }

  public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
    return new ArrayList<>();
  }

  @VisibleForTesting
  NetworkBufferPool getNetworkBufferPool() {
    return networkBufferPool;
  }

  public abstract List<ResultPartitionWriter> createResultPartitionWriters(
      ShuffleIOOwnerContext ownerContext,
      List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors);

  public abstract List<IndexedInputGate> createInputGates(
      ShuffleIOOwnerContext ownerContext,
      PartitionProducerStateProvider producerStateProvider,
      List<InputGateDeploymentDescriptor> inputGateDescriptors);
}
