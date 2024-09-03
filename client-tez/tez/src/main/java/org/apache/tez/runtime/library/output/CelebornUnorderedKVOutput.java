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

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.writers.CelebornUnorderedPartitionedKVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

/**
 * {@link CelebornUnorderedKVOutput} is a {@link LogicalOutput} that writes key value data without
 * applying any ordering or grouping constraints. This can be used to write raw key value data as
 * is.
 */
// broad cast
@Public
public class CelebornUnorderedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornUnorderedKVOutput.class);

  @VisibleForTesting CelebornUnorderedPartitionedKVWriter kvWriter;

  @VisibleForTesting Configuration conf;

  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private static int mapId;
  private int numMapppers;
  private int numOutputs;
  private int attemptId;
  private String host;
  private int port;
  private int shuffleId;
  private String appId;

  public CelebornUnorderedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
    this.numOutputs = getNumPhysicalOutputs();
    this.numMapppers = outputContext.getVertexParallelism();
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

    this.conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, CustomPartitioner.class.getName());

    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();

    boolean pipelinedShuffle =
        this.conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED,
            TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

    Preconditions.checkArgument(
        !pipelinedShuffle, "Tez on Celeborn can not run on pipelined shuffle mode");
    getContext().requestInitialMemory(0, memoryUpdateCallbackHandler);

    this.host = this.conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = this.conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = this.conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = this.conf.get(TEZ_CELEBORN_APPLICATION_ID);
    String user = this.conf.get(TEZ_CELEBORN_USER);
    CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
    ShuffleClient shuffleClient =
        ShuffleClient.get(appId, host, port, celebornConf, UserIdentifier.apply(user));
    this.kvWriter =
        new CelebornUnorderedPartitionedKVWriter(
            getContext(),
            conf,
            1,
            numOutputs,
            memoryUpdateCallbackHandler.getMemoryAssigned(),
            shuffleClient,
            shuffleId,
            mapId,
            attemptId,
            numMapppers,
            celebornConf);

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      // This would have just a single partition
      isStarted.set(true);
      LOG.info(
          getContext().getInputOutputVertexNames()
              + " started. MemoryAssigned="
              + memoryUpdateCallbackHandler.getMemoryAssigned());
    }
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws Exception {
    // Eventually, disallow multiple invocations.
    return kvWriter;
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    throw new TezUncheckedException("Not expecting any events");
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    List<Event> returnEvents = null;
    if (isStarted.get()) {
      returnEvents = kvWriter.close();
      kvWriter = null;
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
          false,
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

  @VisibleForTesting
  @Private
  String getHost() {
    return getContext().getExecutionContext().getHostName();
  }

  private static final Set<String> confKeys = new HashSet<String>();

  static {
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
    confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
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
  }

  // TODO Maybe add helper methods to extract keys
  // TODO Maybe add constants or an Enum to access the keys

  @InterfaceAudience.Private
  public static Set<String> getConfigurationKeySet() {
    return Collections.unmodifiableSet(confKeys);
  }

  @Private
  public static class CustomPartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      return mapId;
    }
  }
}
