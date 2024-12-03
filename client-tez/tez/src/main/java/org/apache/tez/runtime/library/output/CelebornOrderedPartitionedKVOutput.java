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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Deflater;

import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.Partitioner;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
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
public class CelebornOrderedPartitionedKVOutput extends OrderedPartitionedKVOutput {

  private static final Logger LOG =
      LoggerFactory.getLogger(CelebornOrderedPartitionedKVOutput.class);

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
  private boolean sendEmptyPartitionDetails;
  CelebornConf celebornConf;

  public CelebornOrderedPartitionedKVOutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
    this.deflater = (Deflater) getParentPrivateField(this, "deflater");
    this.sendEmptyPartitionDetails =
        (boolean) getParentPrivateField(this, "sendEmptyPartitionDetails");
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
    super.initialize();

    this.host = this.conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = this.conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = this.conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = this.conf.get(TEZ_CELEBORN_APPLICATION_ID);
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
            new UserIdentifier(
                celebornConf.quotaUserSpecificTenant(), celebornConf.quotaUserSpecificUserName()));

    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    super.start();
    sorter =
        new CelebornSorter(
            getContext(),
            conf,
            getNumPhysicalOutputs(),
            (int) memoryUpdateCallbackHandler.getMemoryAssigned(),
            celebornTezWriter,
            celebornConf);
    setParentPrivateField(this, "sorter", sorter);
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
      returnEvents = super.close();
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
}
