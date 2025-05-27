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

package org.apache.celeborn.plugin.flink;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.groups.ShuffleIOMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.utils.Utils;

/** Factory class to create {@link AbstractRemoteShuffleInputGate}. */
public abstract class AbstractRemoteShuffleInputGateFactory {

  public static final int MIN_BUFFERS_PER_GATE = 16;

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractRemoteShuffleInputGateFactory.class);

  /** Number of max concurrent reading channels. */
  protected final int numConcurrentReading;

  /** Network buffer size. */
  protected final int networkBufferSize;

  /**
   * Network buffer pool used for shuffle read buffers. {@link BufferPool}s will be created from it
   * and each of them will be used by a channel exclusively.
   */
  protected final NetworkBufferPool networkBufferPool;

  /** Sum of buffers. */
  protected final int numBuffersPerGate;

  protected boolean supportFloatingBuffers;

  protected CelebornConf celebornConf;

  public AbstractRemoteShuffleInputGateFactory(
      CelebornConf conf, NetworkBufferPool networkBufferPool, int networkBufferSize) {
    this.celebornConf = conf;
    this.numBuffersPerGate =
        Utils.checkedDownCast(celebornConf.clientFlinkMemoryPerInputGate() / networkBufferSize);
    if (numBuffersPerGate < MIN_BUFFERS_PER_GATE) {
      throw new IllegalArgumentException(
          String.format(
              "Insufficient network memory per input gate, please increase %s to at "
                  + "least %d bytes.",
              CelebornConf.CLIENT_MEMORY_PER_INPUT_GATE().key(),
              networkBufferSize * MIN_BUFFERS_PER_GATE));
    }
    this.supportFloatingBuffers = celebornConf.clientFlinkInputGateSupportFloatingBuffer();
    this.networkBufferSize = networkBufferSize;
    this.numConcurrentReading = celebornConf.clientFlinkNumConcurrentReading();
    this.networkBufferPool = networkBufferPool;
  }

  /** Create RemoteShuffleInputGate from {@link InputGateDeploymentDescriptor}. */
  public IndexedInputGate create(
      ShuffleIOOwnerContext ownerContext,
      int gateIndex,
      InputGateDeploymentDescriptor igdd,
      Map<Integer, ShuffleIOMetricGroup> shuffleIOMetricGroups) {
    LOG.info(
        "Create input gate -- number of buffers per input gate={}, "
            + "number of concurrent readings={}.",
        numBuffersPerGate,
        numConcurrentReading);

    SupplierWithException<BufferPool, IOException> bufferPoolFactory =
        createBufferPoolFactory(networkBufferPool, numBuffersPerGate, supportFloatingBuffers);

    return createInputGate(
        ownerContext,
        gateIndex,
        igdd,
        bufferPoolFactory,
        celebornConf.shuffleCompressionCodec().name(),
        shuffleIOMetricGroups);
  }

  protected abstract IndexedInputGate createInputGate(
      ShuffleIOOwnerContext ownerContext,
      int gateIndex,
      InputGateDeploymentDescriptor igdd,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      String compressionCodec,
      Map<Integer, ShuffleIOMetricGroup> shuffleIOMetricGroupMap);

  private SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
      BufferPoolFactory bufferPoolFactory, int numBuffers, boolean supportFloatingBuffers) {
    if (supportFloatingBuffers) {
      return () -> bufferPoolFactory.createBufferPool(1, numBuffers);
    } else {
      return () -> bufferPoolFactory.createBufferPool(numBuffers, numBuffers);
    }
  }
}
