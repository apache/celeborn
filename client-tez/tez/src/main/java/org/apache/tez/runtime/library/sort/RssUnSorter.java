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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.util.ByteUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** {@link RssUnSorter} is an {@link ExternalSorter} */
public class RssUnSorter extends ExternalSorter {

  private static final Logger LOG = LoggerFactory.getLogger(RssUnSorter.class);
  private WriteBufferManager bufferManager;
  private Set<Long> successBlockIds = Sets.newConcurrentHashSet();
  private Set<Long> failedBlockIds = Sets.newConcurrentHashSet();
  private int[] numRecordsPerPartition;

  /** Initialization */
  public RssUnSorter(
          OutputContext outputContext,
          Configuration conf,
          int numOutputs,
          long initialMemoryAvailable,
          CelebornTezWriter celebornTezWriter)
          throws IOException {
    super(outputContext, conf, numOutputs, initialMemoryAvailable);

    this.numRecordsPerPartition = new int[numOutputs];

    long sortmb = 100;
    LOG.info("conf.sortmb is {}", sortmb);
    sortmb = this.availableMemoryMb;
    LOG.info("sortmb, availableMemoryMb is {}, {}", sortmb, availableMemoryMb);
    double sortThreshold = 0.9;

    long maxSegmentSize = 3 * 1024;
    long maxBufferSize = 1024 * 1024 * 14;
    double memoryThreshold = 0.8;
    double sendThreshold = 0.2f;
    int batch = 50;

    bufferManager =
            new WriteBufferManager(
                    (long) (ByteUnit.MiB.toBytes(sortmb) * sortThreshold),
                    celebornTezWriter,
                    comparator,
                    maxSegmentSize,
                    keySerializer,
                    valSerializer,
                    maxBufferSize,
                    memoryThreshold,
                    sendThreshold,
                    batch,
                    true,
                    mapOutputByteCounter,
                    mapOutputRecordCounter);
    LOG.info("Initialized WriteBufferManager.");

  }

  @Override
  public void flush() throws IOException {
    bufferManager.waitSendFinished();
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

    bufferManager.addRecord(partition, key, value);
    numRecordsPerPartition[partition]++;
  }

  public int[] getNumRecordsPerPartition() {
    return numRecordsPerPartition;
  }

}
