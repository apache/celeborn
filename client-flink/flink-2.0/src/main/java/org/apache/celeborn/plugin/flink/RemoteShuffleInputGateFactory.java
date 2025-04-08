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

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.celeborn.common.CelebornConf;

/** Factory class to create {@link RemoteShuffleInputGate}. */
public class RemoteShuffleInputGateFactory extends AbstractRemoteShuffleInputGateFactory {

  public RemoteShuffleInputGateFactory(
      CelebornConf conf, NetworkBufferPool networkBufferPool, int networkBufferSize) {
    super(conf, networkBufferPool, networkBufferSize);
  }

  // For testing.
  @Override
  protected RemoteShuffleInputGate createInputGate(
      String owningTaskName,
      int gateIndex,
      InputGateDeploymentDescriptor igdd,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      String compressionCodec) {
    BufferDecompressor bufferDecompressor =
        new BufferDecompressor(networkBufferSize, CompressionCodec.valueOf(compressionCodec));
    return new RemoteShuffleInputGate(
        this.celebornConf,
        owningTaskName,
        gateIndex,
        igdd,
        bufferPoolFactory,
        bufferDecompressor,
        numConcurrentReading);
  }
}
