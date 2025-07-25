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

package org.apache.celeborn.plugin.flink.metric;

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

/** Factory for remote shuffle service metrics. */
public class RemoteShuffleMetricFactory {

  // shuffle environment level metrics: Shuffle.Remote.*

  private static final String METRIC_TOTAL_MEMORY_SEGMENT = "TotalMemorySegments";
  private static final String METRIC_TOTAL_MEMORY = "TotalMemory";

  private static final String METRIC_AVAILABLE_MEMORY_SEGMENT = "AvailableMemorySegments";
  private static final String METRIC_AVAILABLE_MEMORY = "AvailableMemory";

  private static final String METRIC_USED_MEMORY_SEGMENT = "UsedMemorySegments";
  private static final String METRIC_USED_MEMORY = "UsedMemory";

  public static final String METRIC_REQUESTED_MEMORY_USAGE = "RequestedMemoryUsage";

  // task level metric group structure: Shuffle.Remote.<Input|Output>.Buffers

  public static final String METRIC_GROUP_SHUFFLE = "Shuffle";
  public static final String METRIC_GROUP_REMOTE = "Remote";

  private RemoteShuffleMetricFactory() {}

  public static void registerShuffleMetrics(
      MetricGroup metricGroup, NetworkBufferPool networkBufferPool) {
    checkNotNull(metricGroup);
    checkNotNull(networkBufferPool);
    internalRegisterShuffleMetrics(metricGroup, networkBufferPool);
  }

  private static void internalRegisterShuffleMetrics(
      MetricGroup metricGroup, NetworkBufferPool networkBufferPool) {
    MetricGroup networkGroup =
        metricGroup.addGroup(METRIC_GROUP_SHUFFLE).addGroup(METRIC_GROUP_REMOTE);
    networkGroup.gauge(
        METRIC_TOTAL_MEMORY_SEGMENT, networkBufferPool::getTotalNumberOfMemorySegments);
    networkGroup.gauge(METRIC_TOTAL_MEMORY, networkBufferPool::getTotalMemory);
    networkGroup.gauge(
        METRIC_AVAILABLE_MEMORY_SEGMENT, networkBufferPool::getNumberOfAvailableMemorySegments);
    networkGroup.gauge(METRIC_AVAILABLE_MEMORY, networkBufferPool::getAvailableMemory);
    networkGroup.gauge(
        METRIC_USED_MEMORY_SEGMENT, networkBufferPool::getNumberOfUsedMemorySegments);
    networkGroup.gauge(METRIC_USED_MEMORY, networkBufferPool::getUsedMemory);
    networkGroup.gauge(
        METRIC_REQUESTED_MEMORY_USAGE, new RequestedMemoryUsageMetric(networkBufferPool));
  }

  public static MetricGroup createShuffleIOOwnerMetricGroup(MetricGroup parentGroup) {
    return parentGroup.addGroup(METRIC_GROUP_SHUFFLE).addGroup(METRIC_GROUP_REMOTE);
  }
}
