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
package org.apache.tez.runtime.library.input;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.*;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.ProgressFailedException;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.readers.UnorderedKVReader;
import org.apache.tez.runtime.library.common.shuffle.ShuffleEventHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.impl.CelebornShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleInputEventHandlerImpl;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.impl.SimpleFetchedInputAllocator;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

/**
 * {@link CelebornUnorderedKVInput} provides unordered key value input by bringing together
 * (shuffling) a set of distributed data and providing a unified view to that data. There are no
 * ordering constraints applied by this input.
 */
@Public
public class CelebornUnorderedKVInput extends AbstractLogicalInput {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornUnorderedKVInput.class);

  private Configuration conf;
  private ShuffleManager shuffleManager;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private long firstEventReceivedTime = -1;
  private MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;

  @SuppressWarnings("rawtypes")
  private UnorderedKVReader kvReader;

  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private TezCounter inputRecordCounter;

  private SimpleFetchedInputAllocator inputManager;
  private ShuffleEventHandler inputEventHandler;
  private ApplicationAttemptId applicationAttemptId;

  public CelebornUnorderedKVInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public synchronized List<Event> initialize() throws Exception {
    Preconditions.checkArgument(getNumPhysicalInputs() != -1, "Number of Inputs has not been set");
    this.conf = TezUtils.createConfFromBaseConfAndPayload(getContext());

    if (getNumPhysicalInputs() == 0) {
      getContext().requestInitialMemory(0l, null);
      isStarted.set(true);
      getContext().inputIsReady();
      LOG.info(
          "input fetch not required since there are 0 physical inputs for input vertex: "
              + getContext().getInputOutputVertexNames());
      return Collections.emptyList();
    } else {
      long initialMemReq = getInitialMemoryReq();
      memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
      this.getContext().requestInitialMemory(initialMemReq, memoryUpdateCallbackHandler);
    }

    this.conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, getContext().getWorkDirs());
    this.inputRecordCounter =
        getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);
    this.applicationAttemptId =
        ApplicationAttemptId.newInstance(
            getContext().getApplicationId(), getContext().getDAGAttemptNumber());
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws IOException {
    if (!isStarted.get()) {
      ////// Initial configuration
      memoryUpdateCallbackHandler.validateUpdateReceived();
      CompressionCodec codec = CodecUtils.getCodec(conf);

      boolean compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
      boolean ifileReadAhead =
          conf.getBoolean(
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
      int ifileReadAheadLength = 0;
      int ifileBufferSize = 0;

      if (ifileReadAhead) {
        ifileReadAheadLength =
            conf.getInt(
                TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
                TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
      }
      ifileBufferSize =
          conf.getInt(
              "io.file.buffer.size", TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);

      this.inputManager =
          new SimpleFetchedInputAllocator(
              TezUtilsInternal.cleanVertexName(getContext().getSourceVertexName()),
              getContext().getUniqueIdentifier(),
              getContext().getDagIdentifier(),
              conf,
              getContext().getTotalMemoryAvailableToTask(),
              memoryUpdateCallbackHandler.getMemoryAssigned());

      String host = conf.get(TEZ_CELEBORN_LM_HOST);
      int port = conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
      int shuffleId = conf.getInt(TEZ_SHUFFLE_ID, -1);
      String appId = conf.get(TEZ_CELEBORN_APPLICATION_ID);
      boolean broadcastOrOneToOne = conf.getBoolean(TEZ_BROADCAST_OR_ONETOONE, false);
      CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
      ShuffleClient shuffleClient =
          ShuffleClient.get(
              appId,
              host,
              port,
              celebornConf,
              new UserIdentifier(
                  celebornConf.quotaUserSpecificTenant(), celebornConf.quotaUserSpecificUserName()),
              null);

      this.shuffleManager =
          new CelebornShuffleManager(
              getContext(),
              conf,
              getNumPhysicalInputs(),
              shuffleClient,
              ifileBufferSize,
              ifileReadAhead,
              ifileReadAheadLength,
              codec,
              inputManager,
              shuffleId,
              applicationAttemptId,
              broadcastOrOneToOne);

      this.inputEventHandler =
          new ShuffleInputEventHandlerImpl(
              getContext(),
              shuffleManager,
              inputManager,
              codec,
              ifileReadAhead,
              ifileReadAheadLength,
              compositeFetch);

      ////// End of Initial configuration

      this.shuffleManager.run();
      this.kvReader =
          createReader(
              inputRecordCounter, codec, ifileBufferSize, ifileReadAhead, ifileReadAheadLength);
      List<Event> pending = new LinkedList<Event>();
      pendingEvents.drainTo(pending);
      if (pending.size() > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              getContext().getInputOutputVertexNames()
                  + ": "
                  + "NoAutoStart delay in processing first event: "
                  + (System.currentTimeMillis() - firstEventReceivedTime));
        }
        inputEventHandler.handleEvents(pending);
      }
      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValueReader getReader() throws Exception {
    Preconditions.checkState(isStarted.get(), "Must start input before invoking this method");
    if (getNumPhysicalInputs() == 0) {
      return new KeyValueReader() {
        @Override
        public boolean next() throws IOException {
          getContext().notifyProgress();
          hasCompletedProcessing();
          completedProcessing = true;
          return false;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public Object getCurrentValue() throws IOException {
          throw new RuntimeException("No data available in Input");
        }
      };
    }
    return this.kvReader;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws Exception {
    ShuffleEventHandler inputEventHandlerLocalRef;
    synchronized (this) {
      if (getNumPhysicalInputs() == 0) {
        throw new CelebornException("No input events expected as numInputs is 0");
      }
      if (!isStarted.get()) {
        if (firstEventReceivedTime == -1) {
          firstEventReceivedTime = System.currentTimeMillis();
        }
        // This queue will keep growing if the Processor decides never to
        // start the event. The Input, however has no idea, on whether start
        // will be invoked or not.
        pendingEvents.addAll(inputEvents);
        return;
      }
      inputEventHandlerLocalRef = inputEventHandler;
    }
    inputEventHandlerLocalRef.handleEvents(inputEvents);
  }

  @Override
  public synchronized List<Event> close() throws Exception {
    if (this.inputEventHandler != null) {
      this.inputEventHandler.logProgress(true);
    }

    if (this.shuffleManager != null) {
      this.shuffleManager.shutdown();
    }

    long dataSize =
        getContext().getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED).getValue();
    getContext().getStatisticsReporter().reportDataSize(dataSize);
    long inputRecords =
        getContext().getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(inputRecords);

    return null;
  }

  private long getInitialMemoryReq() {
    return SimpleFetchedInputAllocator.getInitialMemoryReq(
        conf, getContext().getTotalMemoryAvailableToTask());
  }

  @SuppressWarnings("rawtypes")
  private UnorderedKVReader createReader(
      TezCounter inputRecordCounter,
      CompressionCodec codec,
      int ifileBufferSize,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength)
      throws IOException {
    return new UnorderedKVReader(
        shuffleManager,
        conf,
        codec,
        ifileReadAheadEnabled,
        ifileReadAheadLength,
        ifileBufferSize,
        inputRecordCounter,
        getContext());
  }

  @Override
  public float getProgress() throws ProgressFailedException, InterruptedException {
    try {
      return kvReader.getProgress();
    } catch (IOException e) {
      throw new ProgressFailedException("getProgress encountered IOException ", e);
    }
  }
}
