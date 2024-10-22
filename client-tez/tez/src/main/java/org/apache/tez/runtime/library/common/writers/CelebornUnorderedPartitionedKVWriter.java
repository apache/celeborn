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

package org.apache.tez.runtime.library.common.writers;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;

public class CelebornUnorderedPartitionedKVWriter extends KeyValuesWriter {
  private static final Logger Logger =
      LoggerFactory.getLogger(CelebornUnorderedPartitionedKVWriter.class);

  protected final OutputContext outputContext;
  protected final Configuration conf;
  protected final RawLocalFileSystem localFs;
  protected final Partitioner partitioner;
  protected final Class keyClass;
  protected final Class valClass;
  protected final Serializer keySerializer;
  protected final Serializer valSerializer;
  protected final SerializationFactory serializationFactory;
  protected final Serialization keySerialization;
  protected final Serialization valSerialization;
  protected final int numOutputs;
  protected final CompressionCodec codec;

  protected final TezCounter outputRecordBytesCounter;
  protected final TezCounter outputRecordsCounter;
  protected final TezCounter outputBytesWithOverheadCounter;

  private long availableMemory;
  private int[] numRecordsPerPartition;
  private long[] sizePerPartition;
  private AtomicBoolean isShutdown = new AtomicBoolean(false);

  final TezRuntimeConfiguration.ReportPartitionStats reportPartitionStats;

  private CelebornUnorderedSortBasedPusher pusher;

  static final ThreadLocal<Deflater> deflater =
      new ThreadLocal<Deflater>() {

        @Override
        public Deflater initialValue() {
          return TezCommonUtils.newBestCompressionDeflater();
        }

        @Override
        public Deflater get() {
          Deflater deflater = super.get();
          deflater.reset();
          return deflater;
        }
      };

  public CelebornUnorderedPartitionedKVWriter(
      OutputContext outputContext,
      Configuration conf,
      int numOutputs,
      int numPartitions,
      long availableMemoryBytes,
      ShuffleClient shuffleClient,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      CelebornConf celebornConf) {
    this.outputContext = outputContext;
    this.conf = conf;
    try {
      this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.numOutputs = numOutputs;

    // k/v serialization
    keyClass = ConfigUtils.getIntermediateOutputKeyClass(this.conf);
    valClass = ConfigUtils.getIntermediateOutputValueClass(this.conf);
    serializationFactory = new SerializationFactory(this.conf);
    keySerialization = serializationFactory.getSerialization(keyClass);
    valSerialization = serializationFactory.getSerialization(valClass);
    keySerializer = keySerialization.getSerializer(keyClass);
    valSerializer = valSerialization.getSerializer(valClass);

    outputRecordBytesCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES);
    outputRecordsCounter = outputContext.getCounters().findCounter(TaskCounter.OUTPUT_RECORDS);
    outputBytesWithOverheadCounter =
        outputContext.getCounters().findCounter(TaskCounter.OUTPUT_BYTES_WITH_OVERHEAD);

    // stats
    reportPartitionStats =
        TezRuntimeConfiguration.ReportPartitionStats.fromString(
            conf.get(
                TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS,
                TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS_DEFAULT));
    sizePerPartition = (reportPartitionStats.isEnabled()) ? new long[numOutputs] : null;
    numRecordsPerPartition = new int[numOutputs];

    // compression
    try {
      this.codec = CodecUtils.getCodec(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Logger.info(
        "Instantiating Partitioner: [{}]",
        conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS));

    try {
      this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    availableMemory = availableMemoryBytes;
    // assume that there is 64MB memory to writer shuffle data
    if (availableMemory == 0) {
      availableMemory = 64 * 1024 * 1024;
    }
    pusher =
        new CelebornUnorderedSortBasedPusher(
            shuffleId,
            numMappers,
            numPartitions,
            numOutputs,
            mapId,
            attemptId,
            keySerializer,
            valSerializer,
            (int) availableMemory,
            (int) (availableMemory * 0.8),
            shuffleClient,
            celebornConf);
  }

  @Override
  public void write(Object key, Iterable<Object> iterable) throws IOException {
    Iterator<Object> it = iterable.iterator();
    while (it.hasNext()) {
      write(key, it.next());
    }
  }

  @Override
  public void write(Object key, Object value) throws IOException {
    if (isShutdown.get()) {
      throw new RuntimeException("Writer already closed");
    }
    pusher.insert(key, value, partitioner.getPartition(key, value, numOutputs));
  }

  public void close() throws IOException {
    pusher.close();
    isShutdown.set(true);
    updateTezCountersAndNotify();
  }

  private void updateTezCountersAndNotify() {
    outputRecordBytesCounter.increment(pusher.getMapOutputByteCounter().sum());
    outputBytesWithOverheadCounter.increment(pusher.getMapOutputByteCounter().sum());
    outputRecordsCounter.increment(pusher.getMapOutputRecordCounter().sum());
    numRecordsPerPartition = pusher.getRecordsPerPartition();
    if (sizePerPartition != null) {
      sizePerPartition = pusher.getBytesPerPartition();
    }
    outputContext.notifyProgress();
  }

  public int[] getNumRecordsPerPartition() {
    return numRecordsPerPartition;
  }

  public boolean reportDetailedPartitionStats() {
    return reportPartitionStats.isPrecise();
  }

  public long[] getPartitionStats() {
    return sizePerPartition;
  }
}
