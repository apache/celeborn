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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemorySpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierMasterAgent;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.plugin.flink.utils.FlinkUtils;

/**
 * The factory class of the Celeborn client, used as a tier of flink hybrid shuffle tiered storage.
 * The name 'CelebornTierFactory' is reserved for versioned factories so configurations can be same
 * for each version.
 */
public class BaseCelebornTierFactory {

  protected CelebornConf conf;

  /**
   * The bytes size of a single buffer, default value is 32KB, it will be set according to the flink
   * configuration in {@link BaseCelebornTierFactory#setup}.
   */
  protected int bufferSizeBytes = -1;

  /**
   * The max bytes size of a single segment, it will determine how many buffer can save in a single
   * segment.
   */
  protected static int MAX_BYTES_PER_SEGMENT = 8 * 1024 * 1024;

  protected static final String CELEBORN_TIER_NAME = "CelebornTierFactory";

  public void setup(Configuration configuration) {
    conf = FlinkUtils.toCelebornConf(configuration);
    this.bufferSizeBytes = ConfigurationParserUtils.getPageSize(configuration);
  }

  public TieredStorageMemorySpec getMasterAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 0);
  }

  public TieredStorageMemorySpec getProducerAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 1);
  }

  public TieredStorageMemorySpec getConsumerAgentMemorySpec() {
    return new TieredStorageMemorySpec(getCelebornTierName(), 0);
  }

  public TierMasterAgent createMasterAgent(
      TieredStorageResourceRegistry tieredStorageResourceRegistry) {
    return new CelebornTierMasterAgent(conf);
  }

  public static String getCelebornTierName() {
    return CELEBORN_TIER_NAME;
  }

  public String identifier() {
    return "celeborn";
  }
}
