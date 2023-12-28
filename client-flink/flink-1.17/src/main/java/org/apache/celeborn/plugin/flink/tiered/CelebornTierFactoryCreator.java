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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredFactoryCreator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactoryContext;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierFactory;
import java.util.ArrayList;
import java.util.List;

import static org.apache.celeborn.plugin.flink.utils.Utils.checkArgument;

public class CelebornTierFactoryCreator implements TieredFactoryCreator {
    @Override
    public List<Tuple2<TierFactory, TierFactoryContext>> getTierFactories(
            TieredStorageConfiguration configuration) {
        checkArgument(!configuration.getConfigurations().isEmpty(), "Empty configuration map.");
        List<Tuple2<TierFactory, TierFactoryContext>> tierFactories = new ArrayList<>();
        tierFactories.add(
                Tuple2.of(
                        new MemoryTierFactory(
                                configuration.getMemoryTierNumBytesPerSegment(),
                                configuration.getTieredStorageBufferSize(),
                                configuration.getMemoryTierSubpartitionMaxQueuedBuffers()),
                        configuration::getMemoryTierExclusiveBuffers));
        tierFactories.add(
                Tuple2.of(
                        new DiskTierFactory(
                                configuration.getDiskTierNumBytesPerSegment(),
                                configuration.getTieredStorageBufferSize(),
                                configuration.getMinReserveDiskSpaceFraction(),
                                configuration.getRegionGroupSizeInBytes(),
                                configuration.getMaxCachedBytesBeforeFlush(),
                                configuration.getNumRetainedInMemoryRegionsMax()),
                        configuration::getDiskTierExclusiveBuffers));
        tierFactories.add(
                Tuple2.of(
                        new CelebornTierFactory(configuration.getConfigurations()),
                        CelebornTierConfiguration::getTierExclusiveBuffers));
        return tierFactories;
    }
}