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

package org.apache.celeborn.plugin.flink.netty;

import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;

import org.apache.celeborn.reflect.DynFields;

/**
 * The wrapper of {@link NettyShuffleEnvironment} to generate {@link ResultPartitionFactory} and
 * {@link SingleInputGateFactory}.
 */
public class NettyShuffleEnvironmentWrapper {

  private final NettyShuffleServiceFactory nettyShuffleServiceFactory;
  private final ShuffleEnvironmentContext shuffleEnvironmentContext;

  private volatile NettyShuffleEnvironment nettyShuffleEnvironment;
  private volatile ResultPartitionFactory nettyResultPartitionFactory;
  private volatile SingleInputGateFactory nettyInputGateFactory;

  private static final DynFields.UnboundField<ResultPartitionFactory>
      RESULT_PARTITION_FACTORY_FIELD =
          DynFields.builder()
              .hiddenImpl(NettyShuffleEnvironment.class, "resultPartitionFactory")
              .defaultAlwaysNull()
              .build();

  private static final DynFields.UnboundField<SingleInputGateFactory> INPUT_GATE_FACTORY_FIELD =
      DynFields.builder()
          .hiddenImpl(NettyShuffleEnvironment.class, "singleInputGateFactory")
          .defaultAlwaysNull()
          .build();

  public NettyShuffleEnvironmentWrapper(
      NettyShuffleServiceFactory nettyShuffleServiceFactory,
      ShuffleEnvironmentContext shuffleEnvironmentContext) {
    this.nettyShuffleServiceFactory = nettyShuffleServiceFactory;
    this.shuffleEnvironmentContext = shuffleEnvironmentContext;
  }

  public NettyShuffleEnvironment nettyShuffleEnvironment() {
    if (nettyShuffleEnvironment == null) {
      synchronized (this) {
        if (nettyShuffleEnvironment == null) {
          nettyShuffleEnvironment =
              nettyShuffleServiceFactory.createShuffleEnvironment(shuffleEnvironmentContext);
        }
      }
    }
    return nettyShuffleEnvironment;
  }

  public ResultPartitionFactory nettyResultPartitionFactory() {
    if (nettyResultPartitionFactory == null) {
      synchronized (this) {
        if (nettyResultPartitionFactory == null) {
          nettyResultPartitionFactory =
              RESULT_PARTITION_FACTORY_FIELD.bind(nettyShuffleEnvironment()).get();
        }
      }
    }
    return nettyResultPartitionFactory;
  }

  public SingleInputGateFactory nettyInputGateFactory() {
    if (nettyInputGateFactory == null) {
      synchronized (this) {
        if (nettyInputGateFactory == null) {
          nettyInputGateFactory = INPUT_GATE_FACTORY_FIELD.bind(nettyShuffleEnvironment()).get();
        }
      }
    }
    return nettyInputGateFactory;
  }
}
