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

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.writers.CelebornUnorderedPartitionedKVWriter;
import org.apache.tez.runtime.library.common.writers.UnorderedPartitionedKVWriter;
import org.apache.tez.runtime.library.sort.CelebornTezPerPartitionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

/**
 * {@link UnorderedPartitionedKVOutput} is a {@link LogicalOutput} which can be used to write
 * Key-Value pairs. The key-value pairs are written to the correct partition based on the configured
 * Partitioner.
 */
@Public
public class CelebornUnorderedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG =
      LoggerFactory.getLogger(CelebornUnorderedPartitionedKVOutput.class);

  @VisibleForTesting Configuration conf;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private CelebornUnorderedPartitionedKVWriter kvWriter;
  private final Deflater deflater;
  private boolean sendEmptyPartitionDetails;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  private static int mapId;
  private int numMapppers;
  private int numOutputs;
  private int attemptId;
  private String host;
  private int port;
  private int shuffleId;
  private String appId;
  private static boolean broadcastOrOntToOne;

  public CelebornUnorderedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
    this.numOutputs = getNumPhysicalOutputs();
    this.numMapppers = outputContext.getVertexParallelism();
    TezTaskAttemptID taskAttemptId =
        TezTaskAttemptID.fromString(
            CelebornTezUtils.uniqueIdentifierToAttemptId(outputContext.getUniqueIdentifier()));
    attemptId = taskAttemptId.getId();
    mapId = taskAttemptId.getTaskID().getId();
    deflater = TezCommonUtils.newBestCompressionDeflater();
  }

  @Override
  public synchronized List<Event> initialize() throws Exception {
    this.conf = TezUtils.createConfFromBaseConfAndPayload(getContext());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.conf.setInt(
        TezRuntimeFrameworkConfigs.TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, getNumPhysicalOutputs());
    this.conf.set(
        TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, CustomPartitioner.class.getName());
    sendEmptyPartitionDetails =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
            TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext()
        .requestInitialMemory(
            UnorderedPartitionedKVWriter.getInitialMemoryRequirement(
                conf, getContext().getTotalMemoryAvailableToTask()),
            memoryUpdateCallbackHandler);
    this.host = this.conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = this.conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = this.conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = this.conf.get(TEZ_CELEBORN_APPLICATION_ID);
    this.broadcastOrOntToOne = conf.getBoolean(TEZ_BROADCAST_OR_ONETOONE, false);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
      ShuffleClient shuffleClient =
          ShuffleClient.get(
              appId,
              host,
              port,
              celebornConf,
              new UserIdentifier(
                  celebornConf.quotaUserSpecificTenant(),
                  celebornConf.quotaUserSpecificUserName()));
      this.kvWriter =
          new CelebornUnorderedPartitionedKVWriter(
              getContext(),
              conf,
              numOutputs,
              numOutputs,
              memoryUpdateCallbackHandler.getMemoryAssigned(),
              shuffleClient,
              shuffleId,
              mapId,
              attemptId,
              numMapppers,
              celebornConf);
      isStarted.set(true);
    }
  }

  @Override
  public synchronized Writer getWriter() throws Exception {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return kvWriter;
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {}

  @Override
  public synchronized List<Event> close() throws Exception {
    List<Event> returnEvents;
    if (isStarted.get()) {
      kvWriter.close();
      returnEvents = generateEvents();
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

    int[] numRecordsPerPartition = kvWriter.getNumRecordsPerPartition();

    CelebornTezPerPartitionRecord celebornTezPerPartitionRecord =
        new CelebornTezPerPartitionRecord(numOutputs, numRecordsPerPartition);

    BitSet emptyPartitionDetails = new BitSet();
    for (int i = 0; i < celebornTezPerPartitionRecord.size(); i++) {
      TezIndexRecord indexRecord = celebornTezPerPartitionRecord.getIndex(i);
      if (!indexRecord.hasData()) {
        emptyPartitionDetails.set(i);
      }
    }
    if (emptyPartitionDetails.cardinality() > 0) {
      LOG.info("empty partition details");
    }

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
        kvWriter.getPartitionStats(),
        kvWriter.reportDetailedPartitionStats(),
        auxiliaryService,
        deflater);
    LOG.info("Generate events.");
    return eventList;
  }

  @InterfaceAudience.Private
  public static class CustomPartitioner implements Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
      if (broadcastOrOntToOne) {
        return mapId;
      } else {
        return 0;
      }
    }
  }
}
