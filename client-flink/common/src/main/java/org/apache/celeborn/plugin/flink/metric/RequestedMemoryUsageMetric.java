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

package org.apache.celeborn.plugin.flink.metric;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

/**
 * This is a small hack. Instead of spawning a custom thread to monitor {@link NetworkBufferPool}
 * usage, we are re-using {@link View#update()} method for this purpose.
 */
public class RequestedMemoryUsageMetric implements Gauge<Integer>, View {

  private final NetworkBufferPool networkBufferPool;

  public RequestedMemoryUsageMetric(NetworkBufferPool networkBufferPool) {
    this.networkBufferPool = networkBufferPool;
  }

  @Override
  public Integer getValue() {
    return networkBufferPool.getEstimatedRequestedSegmentsUsage();
  }

  @Override
  public void update() {
    networkBufferPool.maybeLogUsageWarning();
  }
}
