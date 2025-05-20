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

package org.apache.celeborn.plugin.flink.tiered;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

/**
 * The factory class of the Celeborn client, used as a tier of flink hybrid shuffle tiered storage.
 */
public class CelebornTierFactory implements TierFactory {

  private CelebornConf conf;

  /**
   * The bytes size of a single buffer, default value is 32KB, it will be set according to the flink
   * configuration in {@link CelebornTierFactory#setup}.
   */
  private int bufferSizeBytes = -1;

  /**
   * The max bytes size of a single segment, it will determine how many buffer can save in a single
   * segment.
   */
  private static int NUM_BYTES_PER_SEGMENT = 8 * 1024 * 1024;

  private static final String CELEBORN_TIER_NAME = CelebornTierFactory.class.getSimpleName();

  @Override
  public void setup(Configuration configuration) {
    conf = FlinkUtils.toCelebornConf(configuration);
    this.bufferSizeBytes = ConfigurationParserUtils.getPageSize(configuration);
  }

  @Override
  public TieredStorageMemorySpec getMasterAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 0);
  }

  @Override
  public TieredStorageMemorySpec getProducerAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 1);
  }

  @Override
  public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 0);
  }

  @Override
  public TierMasterAgent createMasterAgent(
      TieredStorageResourceRegistry tieredStorageResourceRegistry) {
    return new CelebornTierMasterAgent(conf);
  }

  @Override
  public TierProducerAgent createProducerAgent(
      int numPartitions,
      int numSubpartitions,
      TieredStoragePartitionId partitionId,
      String dataFileBasePath,
      boolean isBroadcastOnly,
      TieredStorageMemoryManager storageMemoryManager,
      TieredStorageNettyService nettyService,
      TieredStorageResourceRegistry resourceRegistry,
      BatchShuffleReadBufferPool bufferPool,
      ScheduledExecutorService ioExecutor,
      List<TierShuffleDescriptor> shuffleDescriptors,
      int maxRequestedBuffers,
      @Nullable BufferCompressor bufferCompressor) {
    return new CelebornTierProducerAgent(
        conf,
        partitionId,
        numPartitions,
        numSubpartitions,
        NUM_BYTES_PER_SEGMENT,
        bufferSizeBytes,
        storageMemoryManager,
        resourceRegistry,
        shuffleDescriptors);
  }

  @Override
  public TierConsumerAgent createConsumerAgent(
      List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
      List<TierShuffleDescriptor> shuffleDescriptors,
      TieredStorageNettyService nettyService) {
    return new CelebornTierConsumerAgent(
        conf, tieredStorageConsumerSpecs, shuffleDescriptors, bufferSizeBytes);
  }

  @Override
  public String identifier() {
    return "celeborn";
  }

  public static String getCelebornTierName() {
    return CELEBORN_TIER_NAME;
  }
}
