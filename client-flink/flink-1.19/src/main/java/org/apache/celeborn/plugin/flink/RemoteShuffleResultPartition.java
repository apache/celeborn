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

import javax.annotation.Nullable;

import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.plugin.flink.buffer.BufferWithSubpartition;
import org.apache.celeborn.plugin.flink.buffer.DataBuffer;

/**
 * A {@link ResultPartition} which appends records and events to {@link DataBuffer} and after the
 * {@link DataBuffer} is full, all data in the {@link DataBuffer} will be copied and spilled to the
 * remote shuffle service in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link DataBuffer} will be spilled directly.
 */
public class RemoteShuffleResultPartition extends AbstractRemoteShuffleResultPartition {

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
        networkBufferSize,
        partitionManager,
        bufferCompressor,
        bufferPoolFactory,
        outputGate);
  }

  public void updateReadableBytes(
      BufferWithSubpartition bufferWithSubpartition, boolean isBroadcast, long readableBytes) {
    if (isBroadcast) {
      resultPartitionBytes.incAll(readableBytes);
    } else {
      resultPartitionBytes.inc(bufferWithSubpartition.getSubpartitionIndex(), readableBytes);
    }
  }
}
