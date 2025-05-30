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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;

import org.apache.celeborn.common.CelebornConf;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent shuffle metric group.
 */
public class ShuffleIOMetricGroup extends ProxyMetricGroup<MetricGroup> {

  private final Counter numBytesIn;
  private final Counter numBytesOut;
  private final Counter numRecordsOut;

  private final Meter numBytesInRate;
  private final Meter numBytesOutRate;
  private final Meter numRecordsOutRate;

  public ShuffleIOMetricGroup(ShuffleMetricGroup parent) {
    super(parent);
    this.numBytesIn = counter(MetricNames.IO_NUM_BYTES_IN, new SimpleCounter());
    this.numBytesOut = counter(MetricNames.IO_NUM_BYTES_OUT, new SimpleCounter());
    this.numRecordsOut = counter(MetricNames.IO_NUM_RECORDS_OUT, new SimpleCounter());
    this.numBytesInRate = meter(MetricNames.IO_NUM_BYTES_IN_RATE, new MeterView(numBytesIn));
    this.numBytesOutRate = meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOut));
    this.numRecordsOutRate =
        meter(MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOut));
  }

  // ============================================================================================
  // Getters
  // ============================================================================================

  public Counter getNumBytesIn() {
    return numBytesIn;
  }

  public Counter getNumBytesOut() {
    return numBytesOut;
  }

  public Counter getNumRecordsOut() {
    return numRecordsOut;
  }

  public Meter getNumBytesInRate() {
    return numBytesInRate;
  }

  public Meter getNumBytesOutRate() {
    return numBytesOutRate;
  }

  public Meter getNumRecordsOutRate() {
    return numRecordsOutRate;
  }

  public static ShuffleIOMetricGroup createShuffleIOMetricGroup(
      ShuffleIOOwnerContext ownerContext, int shuffleId, CelebornConf celebornConf) {
    return new ShuffleMetricGroup(
            ownerContext.getParentGroup(),
            shuffleId,
            celebornConf.clientFlinkMetricsScopeNamingShuffle())
        .getIOMetricGroup();
  }
}
