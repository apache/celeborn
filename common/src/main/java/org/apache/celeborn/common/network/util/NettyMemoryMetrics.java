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

package org.apache.celeborn.common.network.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.metrics.source.AbstractSource;

/** A Netty memory metrics class to collect metrics from Netty ByteBufAllocator. */
public class NettyMemoryMetrics {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final ByteBufAllocator allocator;

  private final boolean verboseMetricsEnabled;

  private final String metricPrefix;

  private final AbstractSource source;

  private final Map<String, String> labels;

  @VisibleForTesting static final Set<String> VERBOSE_METRICS = new HashSet<>();

  static {
    VERBOSE_METRICS.addAll(
        Arrays.asList(
            "numAllocations",
            "numTinyAllocations",
            "numSmallAllocations",
            "numNormalAllocations",
            "numHugeAllocations",
            "numDeallocations",
            "numTinyDeallocations",
            "numSmallDeallocations",
            "numNormalDeallocations",
            "numHugeDeallocations",
            "numActiveAllocations",
            "numActiveTinyAllocations",
            "numActiveSmallAllocations",
            "numActiveNormalAllocations",
            "numActiveHugeAllocations",
            "numActiveBytes"));
  }

  public NettyMemoryMetrics(
      ByteBufAllocator allocator,
      String metricPrefix,
      boolean verboseMetricsEnabled,
      AbstractSource source,
      Map<String, String> labels) {
    this.allocator = allocator;
    this.metricPrefix = metricPrefix;
    this.verboseMetricsEnabled = verboseMetricsEnabled;
    this.source = source;
    this.labels = labels;

    registerMetrics();
  }

  private void registerMetrics() {
    // Register general metrics.
    if (source != null) {
      if (allocator instanceof UnpooledByteBufAllocator) {
        logger.debug("Setup netty metrics for UnpooledByteBufAllocator");
        ByteBufAllocatorMetric unpooledMetric = ((UnpooledByteBufAllocator) allocator).metric();
        source.addGauge(
            MetricRegistry.name(metricPrefix, "usedHeapMemory"),
            labels,
            unpooledMetric::usedHeapMemory);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "usedDirectMemory"),
            labels,
            unpooledMetric::usedDirectMemory);
      } else if (allocator instanceof PooledByteBufAllocator) {
        logger.debug("Setup netty metrics for PooledByteBufAllocator");
        PooledByteBufAllocatorMetric pooledAllocatorMetric =
            ((PooledByteBufAllocator) allocator).metric();
        source.addGauge(
            MetricRegistry.name(metricPrefix, "usedHeapMemory"),
            labels,
            pooledAllocatorMetric::usedHeapMemory);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "usedDirectMemory"),
            labels,
            pooledAllocatorMetric::usedDirectMemory);

        source.addGauge(
            MetricRegistry.name(metricPrefix, "numHeapArenas"),
            labels,
            pooledAllocatorMetric::numHeapArenas);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "numDirectArenas"),
            labels,
            pooledAllocatorMetric::numDirectArenas);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "tinyCacheSize"),
            labels,
            pooledAllocatorMetric::tinyCacheSize);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "smallCacheSize"),
            labels,
            pooledAllocatorMetric::smallCacheSize);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "normalCacheSize"),
            labels,
            pooledAllocatorMetric::normalCacheSize);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "numThreadLocalCaches"),
            labels,
            pooledAllocatorMetric::numThreadLocalCaches);
        source.addGauge(
            MetricRegistry.name(metricPrefix, "chunkSize"),
            labels,
            pooledAllocatorMetric::chunkSize);
        if (verboseMetricsEnabled) {
          int directArenaIndex = 0;
          for (PoolArenaMetric metric : pooledAllocatorMetric.directArenas()) {
            registerArenaMetric(metric, "directArena" + directArenaIndex);
            directArenaIndex++;
          }

          int heapArenaIndex = 0;
          for (PoolArenaMetric metric : pooledAllocatorMetric.heapArenas()) {
            registerArenaMetric(metric, "heapArena" + heapArenaIndex);
            heapArenaIndex++;
          }
        }
      }
    }
  }

  private void registerArenaMetric(PoolArenaMetric arenaMetric, String arenaName) {
    for (String methodName : VERBOSE_METRICS) {
      Method m;
      try {
        m = PoolArenaMetric.class.getMethod(methodName);
      } catch (Exception e) {
        // Failed to find metric related method, ignore this metric.
        continue;
      }

      if (!Modifier.isPublic(m.getModifiers())) {
        // Ignore non-public methods.
        continue;
      }

      Class<?> returnType = m.getReturnType();
      String metricName = MetricRegistry.name(metricPrefix, arenaName, m.getName());
      if (returnType.equals(int.class)) {
        source.addGauge(
            metricName,
            labels,
            () -> {
              try {
                return (Integer) m.invoke(arenaMetric);
              } catch (Exception e) {
                return -1; // Swallow the exceptions.
              }
            });

      } else if (returnType.equals(long.class)) {
        source.addGauge(
            metricName,
            labels,
            () -> {
              try {
                return (Long) m.invoke(arenaMetric);
              } catch (Exception e) {
                return -1L; // Swallow the exceptions.
              }
            });
      }
    }
  }
}
