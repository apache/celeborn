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

package org.apache.tez.runtime.library.sort;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;

/** {@link CelebornSorter} is an {@link ExternalSorter} */
public class CelebornSorter extends ExternalSorter {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornSorter.class);
  private CelebornSortBasedPusher celebornSortBasedPusher;

  private int[] numRecordsPerPartition;

  /** Initialization */
  public CelebornSorter(
      OutputContext outputContext,
      Configuration conf,
      int numOutputs,
      int initialMemoryAvailable,
      CelebornTezWriter celebornTezWriter,
      CelebornConf celebornConf)
      throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);

    this.numRecordsPerPartition = new int[numOutputs];

    final float spillper =
        this.conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SORT_SPILL_PERCENT_DEFAULT);
    int pushSize = (int) (availableMemoryMb * spillper);
    LOG.info("availableMemoryMb is {}", availableMemoryMb);
    RawComparator intermediateOutputKeyComparator =
        ConfigUtils.getIntermediateOutputKeyComparator(conf);
    celebornSortBasedPusher =
        new CelebornSortBasedPusher<>(
            keySerializer,
            valSerializer,
            initialMemoryAvailable,
            pushSize,
            intermediateOutputKeyComparator,
            mapOutputByteCounter,
            mapOutputRecordCounter,
            celebornTezWriter,
            celebornConf,
            true);
  }

  @Override
  public void flush() throws IOException {
    celebornSortBasedPusher.close();
  }

  @Override
  public final List<Event> close() throws IOException {
    return super.close();
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    try {
      collect(key, value, partitioner.getPartition(key, value, partitions));
    } catch (InterruptedException e) {
      throw new CelebornIOException(e);
    }
  }

  synchronized void collect(Object key, Object value, final int partition)
      throws IOException, InterruptedException {
    if (key.getClass() != serializationContext.getKeyClass()) {
      throw new IOException(
          "Type mismatch in key from map: expected "
              + serializationContext.getKeyClass().getName()
              + ", received "
              + key.getClass().getName());
    }
    if (value.getClass() != serializationContext.getValueClass()) {
      throw new IOException(
          "Type mismatch in value from map: expected "
              + serializationContext.getValueClass().getName()
              + ", received "
              + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" + partition + ")");
    }

    celebornSortBasedPusher.insert(key, value, partition);
    numRecordsPerPartition[partition]++;
  }

  public int[] getNumRecordsPerPartition() {
    return numRecordsPerPartition;
  }
}
