/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.runtime.library.output;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_APPLICATION_ID;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_LM_HOST;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_LM_PORT;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_USER;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_SHUFFLE_ID;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
import org.apache.tez.runtime.library.sort.CelebornTezPerPartitionRecord;
import org.apache.tez.runtime.library.sort.CelebornUnSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

/**
 * {@link CelebornUnorderedPartitionedKVOutput} is a {@link LogicalOutput} which can be used to
 * write Key-Value pairs. The key-value pairs are written to the correct partition based on the
 * configured Partitioner.
 */
@Public
public class CelebornUnorderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG =
      LoggerFactory.getLogger(CelebornUnorderedPartitionedKVOutput.class);

  protected ExternalSorter sorter;
  @VisibleForTesting Configuration conf;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private UnorderedPartitionedKVWriter kvWriter;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final Deflater deflater;
  private int mapNum;
  private int numOutputs;
  private int mapId;
  private int attemptId;
  private String host;
  private int port;
  private int shuffleId;
  private String appId;
  CelebornTezWriter celebornTezWriter;

  private boolean sendEmptyPartitionDetails;

  public CelebornUnorderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);

    deflater = TezCommonUtils.newBestCompressionDeflater();
    this.numOutputs = getNumPhysicalOutputs();
    this.mapNum = outputContext.getVertexParallelism();
    TezTaskAttemptID taskAttemptId =
        TezTaskAttemptID.fromString(
            CelebornTezUtils.uniqueIdentifierToAttemptId(outputContext.getUniqueIdentifier()));
    attemptId = taskAttemptId.getId();
    mapId = taskAttemptId.getTaskID().getId();
  }

  @Override
  public synchronized List<Event> initialize() throws Exception {
    this.conf = TezUtils.createConfFromBaseConfAndPayload(getContext());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.conf.setInt(
        TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, getNumPhysicalOutputs());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext()
        .requestInitialMemory(
            UnorderedPartitionedKVWriter.getInitialMemoryRequirement(
                conf, getContext().getTotalMemoryAvailableToTask()),
            memoryUpdateCallbackHandler);

    sendEmptyPartitionDetails =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
            TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);

    this.host = this.conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = this.conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = this.conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = this.conf.get(TEZ_CELEBORN_APPLICATION_ID);
    String user = this.conf.get(TEZ_CELEBORN_USER);
    CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
    celebornTezWriter =
        new CelebornTezWriter(
            shuffleId,
            mapId,
            mapId,
            attemptId,
            mapNum,
            numOutputs,
            celebornConf,
            appId,
            host,
            port,
            UserIdentifier.apply(user));

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();

      sorter =
          new CelebornUnSorter(
              getContext(),
              conf,
              getNumPhysicalOutputs(),
              memoryUpdateCallbackHandler.getMemoryAssigned(),
              celebornTezWriter);

      isStarted.set(true);
    }
  }

  @Override
  public synchronized Writer getWriter() throws Exception {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");

    return new KeyValuesWriter() {
      @Override
      public void write(Object key, Iterable<Object> values) throws IOException {
        sorter.write(key, values);
      }

      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {}

  @Override
  public synchronized List<Event> close() throws Exception {
    List<Event> returnEvents = Lists.newLinkedList();
    if (isStarted.get()) {
      if (sorter != null) {
        sorter.flush();
        returnEvents.addAll(sorter.close());
        //        this.endTime = System.nanoTime();
        returnEvents.addAll(generateEvents());
        sorter = null;
      }
    } else {
      LOG.warn(
          getContext().getInputOutputVertexNames()
              + ": Attempting to close output {} of type {} before it was started. Generating empty events",
          getContext().getDestinationVertexName(),
          this.getClass().getSimpleName());
      returnEvents = new LinkedList<Event>();
      ShuffleUtils.generateEventsForNonStartedOutput(
          returnEvents,
          getNumPhysicalOutputs(),
          getContext(),
          false,
          true,
          TezCommonUtils.newBestCompressionDeflater());
    }

    // This works for non-started outputs since new counters will be created with an initial value
    // of 0
    long outputSize = getContext().getCounters().findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    getContext().getStatisticsReporter().reportDataSize(outputSize);
    long outputRecords =
        getContext().getCounters().findCounter(TaskCounter.OUTPUT_RECORDS).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(outputRecords);

    return returnEvents;
  }

  private List<Event> generateEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    boolean isLastEvent = true;
    String auxiliaryService =
        conf.get(
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

    int[] numRecordsPerPartition = ((CelebornUnSorter) sorter).getNumRecordsPerPartition();

    CelebornTezPerPartitionRecord celebornTezPerPartitionRecord =
        new CelebornTezPerPartitionRecord(numOutputs, numRecordsPerPartition);

    LOG.info("CelebornTezPerPartitionRecord is initialized");

    ShuffleUtils.generateEventOnSpill(
        eventList,
        true,
        isLastEvent,
        getContext(),
        0,
        celebornTezPerPartitionRecord,
        getNumPhysicalOutputs(),
        sendEmptyPartitionDetails,
        getContext().getUniqueIdentifier(),
        sorter.getPartitionStats(),
        sorter.reportDetailedPartitionStats(),
        auxiliaryService,
        deflater);
    LOG.info("Generate events.");
    return eventList;
  }

  private List<Event> generateEmptyEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    ShuffleUtils.generateEventsForNonStartedOutput(
        eventList, getNumPhysicalOutputs(), getContext(), true, true, deflater);
    return eventList;
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_MAX_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_TRANSFER_DATA_VIA_EVENTS_SUPPORT_IN_MEM_FILE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
    confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
    confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
    confKeys.add(
        TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }
}
