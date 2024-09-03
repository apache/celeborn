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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
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
  protected final int numPartitions;
  protected final int numReducers;
  protected final CompressionCodec codec;

  protected final TezCounter outputRecordBytesCounter;
  protected final TezCounter outputRecordsCounter;
  protected final TezCounter outputBytesWithOverheadCounter;

  private final long availableMemory;
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
      CelebornConf celebornConf)
      throws IOException {
    this.outputContext = outputContext;
    this.conf = conf;
    try {
      this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.numPartitions = numOutputs;
    this.numReducers = numPartitions;

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
    sizePerPartition = (reportPartitionStats.isEnabled()) ? new long[numPartitions] : null;
    numRecordsPerPartition = new int[numPartitions];

    // compression
    try {
      this.codec = CodecUtils.getCodec(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Logger.info(
        "Instantiating Partitioner: ["
            + conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS)
            + "]");
    try {
      this.partitioner = TezRuntimeUtils.instantiatePartitioner(this.conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    availableMemory = availableMemoryBytes;
    pusher =
        new CelebornUnorderedSortBasedPusher(
            shuffleId,
            numMappers,
            numReducers,
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
    pusher.insert(key, value, partitioner.getPartition(key, value, numPartitions));
  }

  public List<Event> close() throws IOException {
    pusher.close();
    isShutdown.set(true);
    updateTezCountersAndNotify();
    List<Event> eventList = new ArrayList<>();
    eventList.add(generateVMEvent());
    eventList.add(generateDMEvent());
    return eventList;
  }

  private Event generateVMEvent() throws IOException {
    return ShuffleUtils.generateVMEvent(
        outputContext, this.sizePerPartition, reportPartitionStats.isPrecise(), deflater.get());
  }

  private Event generateDMEvent() throws IOException {
    BitSet emptyPartitions = getEmptyPartitions(numRecordsPerPartition);
    return generateDMEvent(false, -1, false, outputContext.getUniqueIdentifier(), emptyPartitions);
  }

  private Event generateDMEvent(
      boolean addSpillDetails,
      int spillId,
      boolean isLastSpill,
      String pathComponent,
      BitSet emptyPartitions)
      throws IOException {

    outputContext.notifyProgress();
    ShuffleUserPayloads.DataMovementEventPayloadProto.Builder payloadBuilder =
        ShuffleUserPayloads.DataMovementEventPayloadProto.newBuilder();
    if (numPartitions == 1) {
      payloadBuilder.setNumRecord((int) outputRecordsCounter.getValue());
    }

    String host = "fake";
    if (emptyPartitions.cardinality() != 0) {
      // Empty partitions exist
      ByteString emptyPartitionsByteString =
          TezCommonUtils.compressByteArrayToByteString(
              TezUtilsInternal.toByteArray(emptyPartitions), deflater.get());
      payloadBuilder.setEmptyPartitions(emptyPartitionsByteString);
    }

    if (emptyPartitions.cardinality() != numPartitions) {
      // Populate payload only if at least 1 partition has data
      payloadBuilder.setHost(host);
      payloadBuilder.setPort(0);
      payloadBuilder.setPathComponent(pathComponent);
    }

    if (addSpillDetails) {
      payloadBuilder.setSpillId(spillId);
      payloadBuilder.setLastEvent(isLastSpill);
    }

    ByteBuffer payload = payloadBuilder.build().toByteString().asReadOnlyByteBuffer();
    return CompositeDataMovementEvent.create(0, numPartitions, payload);
  }

  private BitSet getEmptyPartitions(int[] recordsPerPartition) {
    Preconditions.checkArgument(
        recordsPerPartition != null, "records per partition can not be null");
    BitSet emptyPartitions = new BitSet();
    for (int i = 0; i < numPartitions; i++) {
      if (recordsPerPartition[i] == 0) {
        emptyPartitions.set(i);
      }
    }
    return emptyPartitions;
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
}
