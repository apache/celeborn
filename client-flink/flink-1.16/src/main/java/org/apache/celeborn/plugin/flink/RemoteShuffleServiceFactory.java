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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;

import org.apache.celeborn.plugin.flink.netty.NettyShuffleEnvironmentWrapper;

public class RemoteShuffleServiceFactory extends AbstractRemoteShuffleServiceFactory {

  @Override
  public ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> createShuffleEnvironment(
      ShuffleEnvironmentContext shuffleEnvironmentContext) {
    AbstractRemoteShuffleServiceParameters parameters =
        initializePreCreateShuffleEnvironment(shuffleEnvironmentContext);
    RemoteShuffleResultPartitionFactory resultPartitionFactory =
        new RemoteShuffleResultPartitionFactory(
            parameters.celebornConf,
            parameters.resultPartitionManager,
            parameters.networkBufferPool,
            parameters.bufferSize);
    RemoteShuffleInputGateFactory inputGateFactory =
        new RemoteShuffleInputGateFactory(
            parameters.celebornConf, parameters.networkBufferPool, parameters.bufferSize);

    return new RemoteShuffleEnvironment(
        parameters.networkBufferPool,
        parameters.resultPartitionManager,
        resultPartitionFactory,
        inputGateFactory,
        parameters.celebornConf,
        new NettyShuffleEnvironmentWrapper(nettyShuffleServiceFactory, shuffleEnvironmentContext));
  }

  @Override
  public Duration getRequestSegmentsTimeout(Configuration configuration) {
    return Duration.ofMillis(
        configuration.get(
            NettyShuffleEnvironmentOptions.NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
  }
}
