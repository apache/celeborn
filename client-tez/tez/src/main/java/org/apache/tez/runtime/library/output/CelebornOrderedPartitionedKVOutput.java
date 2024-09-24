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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValuesWriter;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVOutputConfig.SorterImpl;
import org.apache.tez.runtime.library.sort.CelebornSorter;
import org.apache.tez.runtime.library.sort.CelebornTezPerPartitionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.CelebornTezWriter;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

/**
 * {@link CelebornOrderedPartitionedKVOutput} is an {@link AbstractLogicalOutput} which sorts
 * key/value pairs written to it. It also partitions the output based on a {@link Partitioner}
 */
@Public
public class CelebornOrderedPartitionedKVOutput extends AbstractLogicalOutput {

  private static final Logger LOG =
      LoggerFactory.getLogger(CelebornOrderedPartitionedKVOutput.class);

  protected ExternalSorter sorter;
  protected Configuration conf;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private long startTime;
  private long endTime;
  private int mapNum;
  private int numOutputs;
  private int mapId;
  private int attemptId;
  private String host;
  private int port;
  private int shuffleId;
  private String appId;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final Deflater deflater;
  CelebornTezWriter celebornTezWriter;
  CelebornConf celebornConf;
  @VisibleForTesting boolean pipelinedShuffle;
  private boolean sendEmptyPartitionDetails;
  @VisibleForTesting boolean finalMergeEnabled;

  public CelebornOrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
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
  public synchronized List<Event> initialize() throws IOException {
    this.startTime = System.nanoTime();
    this.conf = TezUtils.createConfFromBaseConfAndPayload(getContext());
    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    getContext()
        .requestInitialMemory(
            ExternalSorter.getInitialMemoryRequirement(
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
    celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
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
      String sorterClass =
          conf.get(
                  TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS,
                  TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS_DEFAULT)
              .toUpperCase(Locale.ENGLISH);
      SorterImpl sorterImpl = null;
      try {
        sorterImpl = SorterImpl.valueOf(sorterClass);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Invalid sorter class specified in config"
                + ", propertyName="
                + TezRuntimeConfiguration.TEZ_RUNTIME_SORTER_CLASS
                + ", value="
                + sorterClass
                + ", validValues="
                + Arrays.asList(SorterImpl.values()));
      }

      finalMergeEnabled =
          conf.getBoolean(
              TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT,
              TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT_DEFAULT);

      pipelinedShuffle =
          this.conf.getBoolean(
              TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED,
              TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED_DEFAULT);

      if (pipelinedShuffle) {
        if (finalMergeEnabled) {
          LOG.info(
              getContext().getInputOutputVertexNames()
                  + " disabling final merge as "
                  + TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED
                  + " is enabled.");
          finalMergeEnabled = false;
          conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);
        }

        Preconditions.checkArgument(
            sorterImpl.equals(SorterImpl.PIPELINED),
            TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED
                + "only works with PipelinedSorter.");
      }

      sorter =
          new CelebornSorter(
              getContext(),
              conf,
              getNumPhysicalOutputs(),
              (int) memoryUpdateCallbackHandler.getMemoryAssigned(),
              celebornTezWriter,
              celebornConf);

      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValuesWriter getWriter() throws IOException {
    com.google.common.base.Preconditions.checkState(
        isStarted.get(), "Cannot get writer before starting the Output");

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
  public synchronized void handleEvents(List<Event> outputEvents) {
    // Not expecting any events.
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    List<Event> returnEvents = Lists.newLinkedList();
    if (sorter != null) {
      sorter.flush();
      returnEvents.addAll(sorter.close());
      this.endTime = System.nanoTime();
      returnEvents.addAll(generateEvents());
      sorter = null;
    } else {
      LOG.warn(
          getContext().getInputOutputVertexNames()
              + ": Attempting to close output {} of type {} before it was started. Generating empty events",
          getContext().getDestinationVertexName(),
          this.getClass().getSimpleName());
      returnEvents = generateEmptyEvents();
    }

    return returnEvents;
  }

  private List<Event> generateEvents() throws IOException {
    List<Event> eventList = Lists.newLinkedList();
    boolean isLastEvent = true;
    String auxiliaryService =
        conf.get(
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);

    int[] numRecordsPerPartition = ((CelebornSorter) sorter).getNumRecordsPerPartition();

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
}
