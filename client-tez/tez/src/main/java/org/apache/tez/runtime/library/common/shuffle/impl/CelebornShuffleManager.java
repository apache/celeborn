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

package org.apache.tez.runtime.library.common.shuffle.impl;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_APPLICATION_ID;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_LM_HOST;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_LM_PORT;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_CELEBORN_USER;
import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.TEZ_SHUFFLE_ID;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.GuavaShim;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.InputAttemptFetchFailure;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.CelebornTezReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.exception.CelebornRuntimeException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

// This only knows how to deal with a single srcIndex for a given targetIndex.
// In case the src task generates multiple outputs for the same target Index
// (multiple src-indices), modifications will be required.
public class CelebornShuffleManager extends ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);
  private static final Logger LOG_FETCH;
  private static final ShuffleUtils.FetchStatsLogger fetchStatsLogger;

  static {
    LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
    fetchStatsLogger = new ShuffleUtils.FetchStatsLogger(LOG_FETCH, LOG);
  }

  private final InputContext inputContext;
  private final int numInputs;
  private final int shuffleId;
  private String host;
  private int port;
  private String appId;
  private String user;
  private final ApplicationAttemptId applicationAttemptId;

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final ListeningExecutorService schedulerExecutor;
  private final CelebornRunShuffleCallable celebornRunShuffleCallable;

  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();
  private final String sourceDestNameTrimmed;

  private final ListeningExecutorService fetcherExecutor;
  private Set<CelebornFetcher> runningFetchers;
  private final int numFetchers;

  private final FetchedInputAllocator inputManager;
  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  private final AtomicInteger numFetchedSpills = new AtomicInteger(0);
  private final BlockingQueue<Integer> pendingPartition = new LinkedBlockingQueue<>();

  private final Set<Integer> successCelebornPartitionSet = new HashSet<>();
  private final Set<Integer> runningCelebornPartitionMap = new HashSet<>();
  private final Set<Integer> allCelebornPartition = Sets.newConcurrentHashSet();

  private final BlockingQueue<FetchedInput> completedInputs;
  private final AtomicBoolean inputReadyNotificationSent = new AtomicBoolean(false);

  private volatile Throwable shuffleError;

  private final TezCounter shufflePhaseTime;
  private final long startTime;

  private final TezCounter shuffledInputsCounter;
  private final TezCounter failedShufflesCounter;
  private final TezCounter bytesShuffledCounter;
  private final TezCounter bytesShuffledToMemCounter;
  private final TezCounter bytesShuffledToDiskCounter;
  private final TezCounter decompressedDataSizeCounter;
  private final TezCounter bytesShuffledDirectDiskCounter;
  private long totalBytesShuffledTillNow;
  private final String localhostName;

  private final AtomicInteger nextProgressLineEventCount;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  /** Executor for ReportCallable. */
  private ExecutorService reporterExecutor;

  /** Lock to sync failedEvents. */
  private final ReentrantLock reportLock = new ReentrantLock();

  /** Condition to wake up the thread notifying when events fail. */
  private final Condition reportCondition = reportLock.newCondition();

  /** Events reporting fetcher failed. */
  private final HashMap<InputReadErrorEvent, Integer> failedEvents = new HashMap<>();

  /** Holds the time to wait for failures to batch them and send less events. */
  private final int maxTimeToWaitForReportMillis = 1;

  public CelebornShuffleManager(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      int bufferSize,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength,
      CompressionCodec codec,
      FetchedInputAllocator inputAllocator,
      ApplicationAttemptId applicationAttemptId)
      throws IOException {
    super(
        inputContext,
        conf,
        numInputs,
        bufferSize,
        ifileReadAheadEnabled,
        ifileReadAheadLength,
        codec,
        inputAllocator);
    this.inputContext = inputContext;
    this.numInputs = numInputs;
    this.applicationAttemptId = applicationAttemptId;

    runningFetchers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    int maxConfiguredFetchers =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    this.numFetchers = Math.min(maxConfiguredFetchers, numInputs);
    this.sourceDestNameTrimmed =
        TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName())
            + " -> "
            + TezUtilsInternal.cleanVertexName(inputContext.getTaskVertexName());
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);
    this.startTime = System.currentTimeMillis();
    this.inputManager = inputAllocator;
    this.host = conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = conf.get(TEZ_CELEBORN_APPLICATION_ID);
    this.user = conf.get(TEZ_CELEBORN_USER);
    this.celebornRunShuffleCallable = new CelebornRunShuffleCallable(conf);
    ExecutorService schedulerRawExecutor =
        Executors.newFixedThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ShuffleRunner {" + sourceDestNameTrimmed + "}")
                .build());
    this.schedulerExecutor = MoreExecutors.listeningDecorator(schedulerRawExecutor);

    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor =
          inputContext.createTezFrameworkExecutorService(
              numFetchers, "Fetcher_B {" + sourceDestNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor =
          Executors.newFixedThreadPool(
              numFetchers,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("Fetcher_B {" + sourceDestNameTrimmed + "} #%d")
                  .build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);

    /**
     * In case of pipelined shuffle, it is possible to get multiple FetchedInput per attempt. We do
     * not know upfront the number of spills from source.
     */
    completedInputs = new LinkedBlockingDeque<>();

    this.shuffledInputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.failedShufflesCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledCounter = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.bytesShuffledToMemCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);
    this.bytesShuffledToDiskCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.decompressedDataSizeCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.bytesShuffledDirectDiskCounter =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.nextProgressLineEventCount = new AtomicInteger(0);
    this.localhostName = inputContext.getExecutionContext().getHostName();
  }

  @Override
  public void run() throws IOException {
    Preconditions.checkState(inputManager != null, "InputManager must be configured");
    if (maxTimeToWaitForReportMillis > 0) {
      reporterExecutor =
          Executors.newSingleThreadExecutor(
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("ShuffleRunner {" + sourceDestNameTrimmed + "}")
                  .build());
      Future reporterFuture = reporterExecutor.submit(new ReporterCallable());
    }

    ListenableFuture<Void> runShuffleFuture = schedulerExecutor.submit(celebornRunShuffleCallable);
    Futures.addCallback(
        runShuffleFuture, new SchedulerFutureCallback(), MoreExecutors.directExecutor());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }

  /////////////////// Methods for InputEventHandler
  @Override
  public void addKnownInput(
      String hostName,
      int port,
      CompositeInputAttemptIdentifier srcAttemptIdentifier,
      int srcPhysicalIndex) {
    LOG.info(
        "AddKnowInput, hostname:{}, port:{}, srcAttemptIdentifier:{}, srcPhysicalIndex:{}",
        hostName,
        port,
        srcAttemptIdentifier,
        srcPhysicalIndex);

    lock.lock();
    try {
      for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
        int p = srcPhysicalIndex + i;
        LOG.info(
            "PartitionToInput, original:{}, add:{}",
            srcAttemptIdentifier,
            srcAttemptIdentifier.expand(i));
        if (!allCelebornPartition.contains(srcPhysicalIndex + i)) {
          pendingPartition.add(p);
        }
        allCelebornPartition.add(p);
        LOG.info("Add partition:{}, after add, now partition:{}", p, allCelebornPartition);
      }

      LOG.info("pendingPartition:{}.", pendingPartition.size());
    } finally {
      lock.unlock();
    }
  }

  private void maybeInformInputReady(FetchedInput fetchedInput) {
    lock.lock();
    try {
      if (!(fetchedInput instanceof NullFetchedInput)) {
        LOG.info("maybeInformInputReady");
        completedInputs.add(fetchedInput);
      }
      if (!inputReadyNotificationSent.getAndSet(true)) {
        // Should eventually be controlled by Inputs which are processing the data.
        LOG.info("maybeInformInputReady InputContext inputIsReady");
        inputContext.inputIsReady();
      }
    } finally {
      lock.unlock();
    }
  }

  private void adjustCompletedInputs(FetchedInput fetchedInput) {
    lock.lock();
    try {
      completedInputSet.set(fetchedInput.getInputAttemptIdentifier().getInputIdentifier());
      int numComplete = numCompletedInputs.incrementAndGet();
      LOG.info("AdjustCompletedInputs, numCompletedInputs:{}", numComplete);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addCompletedInputWithData(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) throws IOException {
    // InputIdentifier inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Received Data via Event: " + srcAttemptIdentifier + " to " + fetchedInput.getType());
    }
    // Count irrespective of whether this is a copy of an already fetched input
    boolean committed = false;
    if (!completedInputSet.get(inputIdentifier)) {
      synchronized (completedInputSet) {
        if (!completedInputSet.get(inputIdentifier)) {
          fetchedInput.commit();
          committed = true;
          if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
            registerCompletedInput(fetchedInput);
          } else {
            registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
          }
        }
      }
    }
    if (!committed) {
      fetchedInput.abort(); // If this fails, the fetcher may attempt another
      // abort.
    } else {
      lock.lock();
      try {
        // Signal the wakeLoop to check for termination.
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public void addCompletedInputWithNoData(InputAttemptIdentifier srcAttemptIdentifier) {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    if (LOG.isDebugEnabled()) {
      LOG.debug("No input data exists for SrcTask: " + inputIdentifier + ". Marking as complete.");
    }
    lock.lock();
    try {
      if (!completedInputSet.get(inputIdentifier)) {
        NullFetchedInput fetchedInput = new NullFetchedInput(srcAttemptIdentifier);
        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }
      }
      // Awake the loop to check for termination.
      wakeLoop.signal();
    } finally {
      lock.unlock();
    }
    LOG.info(
        "AddCompletedInputWithNoData, numInputs:{}, successCelebornPartitionSet:{}, allCelebornPartition:{}.",
        numInputs,
        successCelebornPartitionSet,
        allCelebornPartition);
  }

  /** @return true if all of the required inputs have been fetched. */
  public boolean allInputsFetched() {
    lock.lock();
    try {
      return (successCelebornPartitionSet.size() >= allCelebornPartition.size()
          || completedInputs.size() == 0);
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs. This method will
   *     block if there are currently no available inputs, but more may become available.
   */
  @Override
  public FetchedInput getNextInput() throws InterruptedException {
    // Check for no additional inputs
    lock.lock();
    try {
      if (completedInputs.peek() == null && allInputsFetched()) {
        return null;
      }
    } finally {
      lock.unlock();
    }
    LOG.info(
        "getNextInput, ----- completedInputs {}, allInputsFetched {}, numInputs {}, numCompletedInputs {}",
        completedInputs.size(),
        allInputsFetched(),
        numInputs,
        numCompletedInputs.get());
    // Block until next input or End of Input message
    FetchedInput fetchedInput = completedInputs.take();
    if (fetchedInput instanceof NullFetchedInput) {
      LOG.info("getNextInput, NullFetchedInput is null:{}", fetchedInput);
      fetchedInput = null;
    }
    LOG.info("getNextInput, fetchedInput:{}", fetchedInput);
    return fetchedInput;
  }

  private void registerCompletedInput(FetchedInput fetchedInput) {
    lock.lock();
    try {
      maybeInformInputReady(fetchedInput);
      adjustCompletedInputs(fetchedInput);
      numFetchedSpills.getAndIncrement();
    } finally {
      lock.unlock();
    }
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    // For pipelined shuffle.
    // TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        if (eventInfo.scheduledForDownload || !eventInfo.eventsProcessed.isEmpty()) {
          IOException exception =
              new IOException(
                  "Previous event already got scheduled for "
                      + input
                      + ". Previous attempt's data could have been already merged "
                      + "to memory/disk outputs.  Killing (self) this task early."
                      + " currentAttemptNum="
                      + eventInfo.attemptNum
                      + ", eventsProcessed="
                      + eventInfo.eventsProcessed
                      + ", scheduledForDownload="
                      + eventInfo.scheduledForDownload
                      + ", newAttemptNum="
                      + input.getAttemptNumber());
          String message = "Killing self as previous attempt data could have been consumed";
          killSelf(exception, message);
          return false;
        }
      }
    }
    return true;
  }

  private void registerCompletedInputForPipelinedShuffle(
      InputAttemptIdentifier srcAttemptIdentifier, FetchedInput fetchedInput) {
    /**
     * For pipelinedshuffle it is possible to get multiple spills. Claim success only when all
     * spills pertaining to an attempt are done.
     */
    if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
      return;
    }

    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    ShuffleEventInfo eventInfo = shuffleInfoEventsMap.get(inputIdentifier);

    // for empty partition case
    if (eventInfo == null && fetchedInput instanceof NullFetchedInput) {
      eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
      shuffleInfoEventsMap.put(inputIdentifier, eventInfo);
    }

    if (eventInfo == null) {
      throw new CelebornRuntimeException("eventInfo should not be null");
    }
    eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
    numFetchedSpills.getAndIncrement();

    if (srcAttemptIdentifier.getFetchTypeInfo() == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
      eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
    }

    lock.lock();
    try {
      /**
       * When fetch is complete for a spill, add it to completedInputs to ensure that it is
       * available for downstream processing. Final success will be claimed only when all spills are
       * downloaded from the source.
       */
      maybeInformInputReady(fetchedInput);

      // check if we downloaded all spills pertaining to this InputAttemptIdentifier
      if (eventInfo.isDone()) {
        adjustCompletedInputs(fetchedInput);
        shuffleInfoEventsMap.remove(srcAttemptIdentifier.getInputIdentifier());
      }
    } finally {
      lock.unlock();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("eventInfo " + eventInfo.toString());
    }
  }

  private void logProgress() {
    int inputsDone = this.numCompletedInputs.get();
    if (inputsDone > this.nextProgressLineEventCount.get() || inputsDone == this.numInputs) {
      this.nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) this.totalBytesShuffledTillNow / 1048576.0;
      long secsSinceStart = (System.currentTimeMillis() - this.startTime) / 1000L + 1L;
      double transferRate = mbs / (double) secsSinceStart;
      LOG.info(
          "copy("
              + inputsDone
              + " (spillsFetched="
              + this.numFetchedSpills.get()
              + ") of "
              + this.numInputs
              + ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
              + this.mbpsFormat.format(transferRate)
              + " MB/s)");
    }
  }

  @Override
  public void fetchSucceeded(
      String host,
      InputAttemptIdentifier srcAttemptIdentifier,
      FetchedInput fetchedInput,
      long fetchedBytes,
      long decompressedLength,
      long copyDuration)
      throws IOException {
    int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
    // Count irrespective of whether this is a copy of an already fetched input
    lock.lock();
    try {
      inputContext.notifyProgress();
      if (!completedInputSet.get(inputIdentifier)) {
        fetchedInput.commit();
        fetchStatsLogger.logIndividualFetchComplete(
            copyDuration,
            fetchedBytes,
            decompressedLength,
            fetchedInput.getType().toString(),
            srcAttemptIdentifier);

        // Processing counters for completed and commit fetches only. Need
        // additional counters for excessive fetches - which primarily comes
        // in after speculation or retries.
        shuffledInputsCounter.increment(1);
        bytesShuffledCounter.increment(fetchedBytes);
        if (fetchedInput.getType() == FetchedInput.Type.MEMORY) {
          bytesShuffledToMemCounter.increment(fetchedBytes);
        } else if (fetchedInput.getType() == FetchedInput.Type.DISK) {
          LOG.warn("Celeborn bytesShuffledToDiskCounter");
          bytesShuffledToDiskCounter.increment(fetchedBytes);
        } else if (fetchedInput.getType() == FetchedInput.Type.DISK_DIRECT) {
          LOG.warn("Celeborn bytesShuffledDirectDiskCounter");
          bytesShuffledDirectDiskCounter.increment(fetchedBytes);
        }
        decompressedDataSizeCounter.increment(decompressedLength);

        if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
          registerCompletedInput(fetchedInput);
        } else {
          LOG.warn("Celeborn registerCompletedInputForPipelinedShuffle");
          registerCompletedInputForPipelinedShuffle(srcAttemptIdentifier, fetchedInput);
        }

        totalBytesShuffledTillNow += fetchedBytes;
        logProgress();
        wakeLoop.signal();
      } else {
        fetchedInput.abort(); // If this fails, the fetcher may attempt another abort.
      }
    } finally {
      lock.unlock();
    }
    // NEWTEZ Maybe inform fetchers, in case they have an alternate attempt of the same task in
    // their queue.
  }

  private void reportFatalError(Throwable exception, String message) {
    LOG.error(message);
    this.inputContext.reportFailure(TaskFailureType.NON_FATAL, exception, message);
  }

  @Override
  public void fetchFailed(
      String host, InputAttemptFetchFailure inputAttemptFetchFailure, boolean connectFailed) {
    // NEWTEZ. Implement logic to report fetch failures after a threshold.
    // For now, reporting immediately.
    InputAttemptIdentifier srcAttemptIdentifier =
        inputAttemptFetchFailure.getInputAttemptIdentifier();
    LOG.info(
        sourceDestNameTrimmed
            + ": "
            + "Fetch failed for src: "
            + srcAttemptIdentifier
            + "InputIdentifier: "
            + srcAttemptIdentifier
            + ", connectFailed: "
            + connectFailed);
    failedShufflesCounter.increment(1);
    inputContext.notifyProgress();
    if (srcAttemptIdentifier == null) {
      reportFatalError(null, "Received fetchFailure for an unknown src (null)");
    } else {
      InputReadErrorEvent readError =
          InputReadErrorEvent.create(
              "Fetch failure while fetching from "
                  + TezRuntimeUtils.getTaskAttemptIdentifier(
                      this.inputContext.getSourceVertexName(),
                      srcAttemptIdentifier.getInputIdentifier(),
                      srcAttemptIdentifier.getAttemptNumber()),
              srcAttemptIdentifier.getInputIdentifier(),
              srcAttemptIdentifier.getAttemptNumber(),
              inputAttemptFetchFailure.isLocalFetch(),
              inputAttemptFetchFailure.isDiskErrorAtSource(),
              this.localhostName);
      if (maxTimeToWaitForReportMillis > 0) {
        reportLock.lock();
        try {
          failedEvents.merge(readError, 1, (a, b) -> a + b);
          reportCondition.signal();
        } finally {
          reportLock.unlock();
        }
      } else {
        List<Event> events = Lists.newArrayListWithCapacity(1);
        events.add(readError);
        inputContext.sendEvents(events);
      }
    }
  }
  // End of Methods from FetcherCallbackHandler
  /////////////////// End of Methods from FetcherCallbackHandler

  public void shutdown() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      // TODO: need to cleanup all FetchedInput (DiskFetchedInput, LocalDisFetchedInput), lockFile
      // As of now relying on job cleanup (when all directories would be cleared)
      LOG.info(sourceDestNameTrimmed + ": " + "Thread interrupted. Need to cleanup the local dirs");
    }
    if (!isShutdown.getAndSet(true)) {
      // Shut down any pending fetchers
      LOG.info(
          "Shutting down pending fetchers on source"
              + sourceDestNameTrimmed
              + ": "
              + runningFetchers.size());
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (CelebornFetcher fetcher : runningFetchers) {
          try {
            fetcher.close(); // This could be parallelized.
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }

      if (this.schedulerExecutor != null && !this.schedulerExecutor.isShutdown()) {
        this.schedulerExecutor.shutdownNow();
      }
      if (this.reporterExecutor != null && !this.reporterExecutor.isShutdown()) {
        this.reporterExecutor.shutdownNow();
      }
      if (this.fetcherExecutor != null && !this.fetcherExecutor.isShutdown()) {
        this.fetcherExecutor.shutdownNow(); // Interrupts all running fetchers.
      }
    }
  }

  private class ReporterCallable extends CallableWithNdc<Void> {

    /** Measures if the batching interval has ended. */
    ReporterCallable() {}

    @Override
    protected Void callInternal() throws Exception {
      long nextReport = 0;
      while (!isShutdown.get()) {
        reportLock.lock();
        try {
          while (failedEvents.isEmpty()) {
            boolean signaled =
                reportCondition.await(maxTimeToWaitForReportMillis, TimeUnit.MILLISECONDS);
          }

          long currentTime = Time.monotonicNow();
          if (currentTime > nextReport) {
            if (failedEvents.size() > 0) {
              List<Event> failedEventsToSend = Lists.newArrayListWithCapacity(failedEvents.size());
              for (InputReadErrorEvent key : failedEvents.keySet()) {
                failedEventsToSend.add(
                    InputReadErrorEvent.create(
                        key.getDiagnostics(), key.getIndex(), key.getVersion()));
              }
              inputContext.sendEvents(failedEventsToSend);
              failedEvents.clear();
              nextReport = currentTime + maxTimeToWaitForReportMillis;
            }
          }
        } finally {
          reportLock.unlock();
        }
      }
      return null;
    }
  }

  private class CelebornRunShuffleCallable extends CallableWithNdc<Void> {

    private final Configuration conf;

    CelebornRunShuffleCallable(Configuration conf) {
      this.conf = conf;
    }

    @Override
    protected Void callInternal() throws Exception {
      while (!isShutdown.get() && numCompletedInputs.get() < numInputs) {
        lock.lock();
        try {
          while ((runningFetchers.size() >= numFetchers) && numCompletedInputs.get() < numInputs) {
            inputContext.notifyProgress();
            boolean ret = wakeLoop.await(1000, TimeUnit.MILLISECONDS);
            if (ret) {
              LOG.info("wakeLoop is signal");
            }
            if (isShutdown.get()) {
              break;
            }
          }
        } finally {
          lock.unlock();
        }

        if (shuffleError != null) {
          // InputContext has already been informed of a fatal error. Relying on
          // tez to kill the task.
          break;
        }

        LOG.debug("{}: NumCompletedInputs: {}", sourceDestNameTrimmed, numCompletedInputs);
        if (numCompletedInputs.get() < numInputs && !isShutdown.get()) {
          lock.lock();
          try {
            int maxFetchersToRun = numFetchers - runningFetchers.size();
            int count = 0;
            while (pendingPartition.peek() != null && !isShutdown.get()) {
              Integer partition = null;
              try {
                partition = pendingPartition.take();
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(
                      sourceDestNameTrimmed
                          + ": "
                          + "Interrupted and hasBeenShutdown, Breaking out of ShuffleScheduler Loop");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                    sourceDestNameTrimmed + ": " + "Processing pending parition: " + partition);
              }
              if (!isShutdown.get()
                  && (!successCelebornPartitionSet.contains(partition)
                      && !runningCelebornPartitionMap.contains(partition))) {
                CelebornFetcher fetcher = constructFetcherForPartition(partition, conf);
                runningFetchers.add(fetcher);
                if (isShutdown.get()) {
                  LOG.info(
                      sourceDestNameTrimmed
                          + ": "
                          + "hasBeenShutdown,"
                          + "Breaking out of ShuffleScheduler Loop");
                  break;
                }
                ListenableFuture<FetchResult> future = fetcherExecutor.submit(fetcher);
                Futures.addCallback(
                    future, new FetchFutureCallback(fetcher), GuavaShim.directExecutor());
                if (++count >= maxFetchersToRun) {
                  break;
                }
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      sourceDestNameTrimmed
                          + ": "
                          + "Skipping partition: "
                          + partition
                          + " since it has no inputs to process");
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      }
      shufflePhaseTime.setValue(System.currentTimeMillis() - startTime);
      LOG.info(
          sourceDestNameTrimmed
              + ": "
              + "Shutting down FetchScheduler, Was Interrupted: "
              + Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }
  }

  CelebornFetcher constructFetcherForPartition(Integer partition, Configuration conf) {
    ShuffleClient shuffleClient =
        ShuffleClient.get(
            appId,
            host,
            port,
            CelebornTezUtils.fromTezConfiguration(conf),
            UserIdentifier.apply(user),
            null);
    CelebornTezReader reader =
        new CelebornTezReader(
            shuffleClient, shuffleId, partition, applicationAttemptId.getAttemptId());
    pendingPartition.add(partition);
    return new CelebornFetcher(inputManager, CelebornShuffleManager.this, reader, partition);
  }

  private class SchedulerFutureCallback implements FutureCallback<Void> {

    @Override
    public void onSuccess(Void result) {
      LOG.info("{}: Scheduler thread completed", sourceDestNameTrimmed);
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Already shutdown. Ignoring error: ", sourceDestNameTrimmed, t);
        }
      } else {
        LOG.error("{}: Scheduler failed with error: ", sourceDestNameTrimmed, t);
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Scheduler Failed");
      }
    }
  }

  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final CelebornFetcher fetcher;

    FetchFutureCallback(CelebornFetcher fetcher) {
      this.fetcher = fetcher;
    }

    private void doBookKeepingForFetcherComplete() {
      lock.lock();
      try {
        runningFetchers.remove(fetcher);
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void onSuccess(FetchResult result) {
      LOG.info("FetchFutureCallback success, partition:{}", fetcher.getPartition());
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{}: Already shutdown. Ignoring event from fetcher", sourceDestNameTrimmed);
        }
      } else {
        lock.lock();
        try {
          successCelebornPartitionSet.add(fetcher.getPartition());
          runningCelebornPartitionMap.remove(fetcher.getPartition());
          LOG.info(
              "FetchFutureCallback allCelebornPartition:{}, successCelebornPartitionSet:{}, "
                  + "runningCelebornPartitionMap:{}.",
              allCelebornPartition,
              successCelebornPartitionSet,
              runningCelebornPartitionMap);
          doBookKeepingForFetcherComplete();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      if (isShutdown.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "{}: Already shutdown. Ignoring error from fetcher: ", sourceDestNameTrimmed, t);
        }
      } else {
        LOG.error("{}: Fetcher failed with error: ", sourceDestNameTrimmed, t);
        shuffleError = t;
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
