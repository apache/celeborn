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
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.CompressionCodec;

/** Factory class to create {@link RemoteShuffleResultPartition}. */
public class RemoteShuffleResultPartitionFactory
    extends AbstractRemoteShuffleResultPartitionFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(RemoteShuffleResultPartitionFactory.class);

  public RemoteShuffleResultPartitionFactory(
      Configuration flinkConf,
      CelebornConf celebornConf,
      ResultPartitionManager partitionManager,
      BufferPoolFactory bufferPoolFactory,
      int networkBufferSize) {

    super(flinkConf, celebornConf, partitionManager, bufferPoolFactory, networkBufferSize);
  }

  protected ResultPartition create(
      String taskNameWithSubtaskAndId,
      int partitionIndex,
      ResultPartitionID id,
      ResultPartitionType type,
      int numSubpartitions,
      int maxParallelism,
      List<SupplierWithException<BufferPool, IOException>> bufferPoolFactories,
      ShuffleDescriptor shuffleDescriptor,
      CelebornConf celebornConf,
      int numMappers) {

    // in flink1.14/1.15, just support LZ4
    if (!compressionCodec.equals(CompressionCodec.LZ4.name())) {
      throw new IllegalStateException("Unknown CompressionMethod " + compressionCodec);
    }
    final BufferCompressor bufferCompressor =
        new BufferCompressor(networkBufferSize, compressionCodec);
    RemoteShuffleDescriptor rsd = (RemoteShuffleDescriptor) shuffleDescriptor;
    ResultPartition partition =
        new RemoteShuffleResultPartition(
            taskNameWithSubtaskAndId,
            partitionIndex,
            id,
            type,
            numSubpartitions,
            maxParallelism,
            networkBufferSize,
            partitionManager,
            bufferCompressor,
            bufferPoolFactories.get(0),
            new RemoteShuffleOutputGate(
                rsd,
                numSubpartitions,
                networkBufferSize,
                bufferPoolFactories.get(1),
                celebornConf,
                numMappers));
    LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);
    return partition;
  }
}
