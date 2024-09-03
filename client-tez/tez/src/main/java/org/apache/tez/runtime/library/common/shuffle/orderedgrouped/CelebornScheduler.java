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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.SecretKey;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.shuffle.HostPort;
import org.apache.tez.runtime.library.common.shuffle.InputAttemptFetchFailure;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils.FetchStatsLogger;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapHost.HostPortPartition;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MapOutput.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;

class CelebornScheduler extends ShuffleScheduler {
  @VisibleForTesting static final String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";

  private final AtomicLong shuffleStart = new AtomicLong(0);

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleScheduler.class);
  private static final Logger LOG_FETCH = LoggerFactory.getLogger(LOG.getName() + ".fetch");
  private static final FetchStatsLogger fetchStatsLogger = new FetchStatsLogger(LOG_FETCH, LOG);

  static final long INITIAL_PENALTY = 2000L; // 2 seconds
  private static final float PENALTY_GROWTH_RATE = 1.3f;

  private final BitSet finishedMaps;
  private final int numInputs;
  private int numFetchedSpills;

  @VisibleForTesting
  final Map<HostPortPartition, MapHost> mapLocations = new HashMap<HostPortPartition, MapHost>();
  // TODO Clean this and other maps at some point
  @VisibleForTesting
  final ConcurrentMap<PathPartition, InputAttemptIdentifier> pathToIdentifierMap =
      new ConcurrentHashMap<PathPartition, InputAttemptIdentifier>();

  // To track shuffleInfo events when finalMerge is disabled in source or pipelined shuffle is
  // enabled in source.
  @VisibleForTesting final Map<Integer, ShuffleEventInfo> pipelinedShuffleInfoEventsMap;

  @VisibleForTesting final Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private final Set<InputAttemptIdentifier> obsoleteInputs = new HashSet<InputAttemptIdentifier>();

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final Random random = new Random(System.currentTimeMillis());
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee;

  @VisibleForTesting
  final Map<InputAttemptIdentifier, IntWritable> failureCounts =
      new HashMap<InputAttemptIdentifier, IntWritable>();

  final Set<HostPort> uniqueHosts = Sets.newHashSet();
  private final Map<HostPort, IntWritable> hostFailures = new HashMap<HostPort, IntWritable>();
  private final InputContext inputContext;
  private final TezCounter shuffledInputsCounter;
  private final TezCounter skippedInputCounter;
  private final TezCounter reduceShuffleBytes;
  private final TezCounter reduceBytesDecompressed;
  @VisibleForTesting final TezCounter failedShuffleCounter;
  private final TezCounter bytesShuffledToDisk;
  private final TezCounter bytesShuffledToDiskDirect;
  private final TezCounter bytesShuffledToMem;
  private final TezCounter firstEventReceived;
  private final TezCounter lastEventReceived;

  private final String srcNameTrimmed;
  @VisibleForTesting final AtomicInteger remainingMaps;

  private final long startTime;
  @VisibleForTesting long lastProgressTime;
  @VisibleForTesting long failedShufflesSinceLastCompletion;

  private final int numFetchers;
  private final Set<CelebornTezShuffleDataFetcher> celebornRunningFetchers =
      Collections.newSetFromMap(new ConcurrentHashMap<CelebornTezShuffleDataFetcher, Boolean>());

  private final ListeningExecutorService fetcherExecutor;

  private final HttpConnectionParams httpConnectionParams;
  private final FetchedInputAllocatorOrderedGrouped allocator;
  private final ExceptionReporter exceptionReporter;
  private final MergeManager mergeManager;
  private final JobTokenSecretManager jobTokenSecretManager;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final CompressionCodec codec;
  private final Configuration conf;
  private final RawLocalFileSystem localFs;
  private final boolean localDiskFetchEnabled;
  private final String localHostname;
  private final int shufflePort;
  private final boolean asyncHttp;
  private final boolean sslShuffle;
  private final int shuffleId;
  private final ApplicationAttemptId applicationAttemptId;
  private final ShuffleClient shuffleClient;

  private final TezCounter ioErrsCounter;
  private final TezCounter wrongLengthErrsCounter;
  private final TezCounter badIdErrsCounter;
  private final TezCounter wrongMapErrsCounter;
  private final TezCounter connectionErrsCounter;
  private final TezCounter wrongReduceErrsCounter;

  private final int maxTaskOutputAtOnce;
  private final int maxFetchFailuresBeforeReporting;
  private final boolean reportReadErrorImmediately;
  private final int maxFailedUniqueFetches;
  private final int abortFailureLimit;

  private final int minFailurePerHost;
  private final float hostFailureFraction;
  private final float maxStallTimeFraction;
  private final float minReqProgressFraction;
  private final float maxAllowedFailedFetchFraction;
  private final boolean checkFailedFetchSinceLastCompletion;
  private final boolean verifyDiskChecksum;
  private final boolean compositeFetch;
  private final boolean enableFetcherTestingErrors;
  private volatile Thread shuffleSchedulerThread = null;
  private final int maxPenaltyTime;

  private long totalBytesShuffledTillNow = 0;
  private final DecimalFormat mbpsFormat = new DecimalFormat("0.00");

  // celeborn
  private final Map<Integer, MapHost> runningRssPartitionMap = new HashMap<>();

  private final Set<Integer> successRssPartitionSet = Sets.newConcurrentHashSet();
  private final Set<Integer> allRssPartition = Sets.newConcurrentHashSet();

  private final Map<Integer, Set<InputAttemptIdentifier>> partitionIdToSuccessMapTaskAttempts =
      new HashMap<>();

  public CelebornScheduler(
      InputContext inputContext,
      Configuration conf,
      int numberOfInputs,
      ExceptionReporter exceptionReporter,
      ShuffleClient shuffleClient,
      MergeManager mergeManager,
      FetchedInputAllocatorOrderedGrouped allocator,
      long startTime,
      CompressionCodec codec,
      boolean ifileReadAhead,
      int ifileReadAheadLength,
      String srcNameTrimmed,
      int shuffleId,
      ApplicationAttemptId applicationAttemptId)
      throws IOException {
    super(
        inputContext,
        conf,
        numberOfInputs,
        exceptionReporter,
        mergeManager,
        allocator,
        startTime,
        codec,
        ifileReadAhead,
        ifileReadAheadLength,
        srcNameTrimmed);
    this.inputContext = inputContext;
    this.conf = conf;
    this.localFs = (RawLocalFileSystem) FileSystem.getLocal(conf).getRaw();
    this.exceptionReporter = exceptionReporter;
    this.allocator = allocator;
    this.mergeManager = mergeManager;
    this.numInputs = numberOfInputs;
    int abortFailureLimitConf =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_SOURCE_ATTEMPT_ABORT_LIMIT_DEFAULT);
    if (abortFailureLimitConf <= -1) {
      abortFailureLimit = Math.max(15, numberOfInputs / 10);
    } else {
      // No upper cap, as user is setting this intentionally
      abortFailureLimit = abortFailureLimitConf;
    }
    remainingMaps = new AtomicInteger(numberOfInputs);
    finishedMaps = new BitSet(numberOfInputs);
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.srcNameTrimmed = srcNameTrimmed;
    this.shuffleClient = shuffleClient;
    this.shuffleId = shuffleId;
    this.applicationAttemptId = applicationAttemptId;
    this.codec = codec;
    int configuredNumFetchers =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES_DEFAULT);
    numFetchers = Math.min(configuredNumFetchers, numInputs);

    localDiskFetchEnabled =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH,
            TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH_DEFAULT);

    this.minFailurePerHost =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST_DEFAULT);
    Preconditions.checkArgument(
        minFailurePerHost >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST
            + "="
            + minFailurePerHost
            + " should not be negative");

    this.hostFailureFraction =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION,
            TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION_DEFAULT);

    this.maxStallTimeFraction =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION_DEFAULT);
    Preconditions.checkArgument(
        maxStallTimeFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_STALL_TIME_FRACTION
            + "="
            + maxStallTimeFraction
            + " should not be negative");

    this.minReqProgressFraction =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION_DEFAULT);
    Preconditions.checkArgument(
        minReqProgressFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_REQUIRED_PROGRESS_FRACTION
            + "="
            + minReqProgressFraction
            + " should not be negative");

    this.maxAllowedFailedFetchFraction =
        conf.getFloat(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION,
            TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION_DEFAULT);
    Preconditions.checkArgument(
        maxAllowedFailedFetchFraction >= 0,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MAX_ALLOWED_FAILED_FETCH_ATTEMPT_FRACTION
            + "="
            + maxAllowedFailedFetchFraction
            + " should not be negative");

    this.checkFailedFetchSinceLastCompletion =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION_DEFAULT);

    this.localHostname = inputContext.getExecutionContext().getHostName();
    String auxiliaryService =
        conf.get(
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT);
    final ByteBuffer shuffleMetadata = inputContext.getServiceProviderMetaData(auxiliaryService);
    this.shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);

    this.referee = new Referee();
    // Counters used by the ShuffleScheduler
    this.shuffledInputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_SHUFFLED_INPUTS);
    this.reduceShuffleBytes = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES);
    this.reduceBytesDecompressed =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DECOMPRESSED);
    this.failedShuffleCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_FAILED_SHUFFLE_INPUTS);
    this.bytesShuffledToDisk =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_DISK);
    this.bytesShuffledToDiskDirect =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_DISK_DIRECT);
    this.bytesShuffledToMem =
        inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_BYTES_TO_MEM);

    // Counters used by Fetchers
    ioErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.BAD_ID.toString());
    wrongMapErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_MAP.toString());
    connectionErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.CONNECTION.toString());
    wrongReduceErrsCounter =
        inputContext
            .getCounters()
            .findCounter(SHUFFLE_ERR_GRP_NAME, ShuffleErrors.WRONG_REDUCE.toString());

    this.startTime = startTime;
    this.lastProgressTime = startTime;

    this.sslShuffle =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL_DEFAULT);
    this.asyncHttp =
        conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP, false);
    this.httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);
    SecretKey jobTokenSecret =
        ShuffleUtils.getJobTokenSecretFromTokenBytes(
            inputContext.getServiceConsumerMetaData(auxiliaryService));
    this.jobTokenSecretManager = new JobTokenSecretManager(jobTokenSecret);

    final ExecutorService fetcherRawExecutor;
    if (conf.getBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL_DEFAULT)) {
      fetcherRawExecutor =
          inputContext.createTezFrameworkExecutorService(
              numFetchers, "Fetcher_O {" + srcNameTrimmed + "} #%d");
    } else {
      fetcherRawExecutor =
          Executors.newFixedThreadPool(
              numFetchers,
              new ThreadFactoryBuilder()
                  .setDaemon(true)
                  .setNameFormat("Fetcher_O {" + srcNameTrimmed + "} #%d")
                  .build());
    }
    this.fetcherExecutor = MoreExecutors.listeningDecorator(fetcherRawExecutor);

    this.maxFailedUniqueFetches = Math.min(numberOfInputs, 5);
    referee.start();
    this.maxFetchFailuresBeforeReporting =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT_DEFAULT);
    this.reportReadErrorImmediately =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_NOTIFY_READERROR_DEFAULT);
    this.verifyDiskChecksum =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_VERIFY_DISK_CHECKSUM_DEFAULT);

    /**
     * Setting to very high val can lead to Http 400 error. Cap it to 75; every attempt id would be
     * approximately 48 bytes; 48 * 75 = 3600 which should give some room for other info in URL.
     */
    this.maxTaskOutputAtOnce =
        Math.max(
            1,
            Math.min(
                75,
                conf.getInt(
                    TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE,
                    TezRuntimeConfiguration
                        .TEZ_RUNTIME_SHUFFLE_FETCH_MAX_TASK_OUTPUT_AT_ONCE_DEFAULT)));

    this.skippedInputCounter =
        inputContext.getCounters().findCounter(TaskCounter.NUM_SKIPPED_INPUTS);
    this.firstEventReceived =
        inputContext.getCounters().findCounter(TaskCounter.FIRST_EVENT_RECEIVED);
    this.lastEventReceived =
        inputContext.getCounters().findCounter(TaskCounter.LAST_EVENT_RECEIVED);
    this.compositeFetch = ShuffleUtils.isTezShuffleHandler(conf);
    this.maxPenaltyTime =
        conf.getInt(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_HOST_PENALTY_TIME_LIMIT_MS,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_HOST_PENALTY_TIME_LIMIT_MS_DEFAULT);

    this.enableFetcherTestingErrors =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_ENABLE_TESTING_ERRORS,
            TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_ENABLE_TESTING_ERRORS_DEFAULT);

    pipelinedShuffleInfoEventsMap = Maps.newConcurrentMap();
    LOG.info(
        "ShuffleScheduler running for sourceVertex: "
            + inputContext.getSourceVertexName()
            + " with configuration: "
            + "maxFetchFailuresBeforeReporting="
            + maxFetchFailuresBeforeReporting
            + ", reportReadErrorImmediately="
            + reportReadErrorImmediately
            + ", maxFailedUniqueFetches="
            + maxFailedUniqueFetches
            + ", abortFailureLimit="
            + abortFailureLimit
            + ", maxTaskOutputAtOnce="
            + maxTaskOutputAtOnce
            + ", numFetchers="
            + numFetchers
            + ", hostFailureFraction="
            + hostFailureFraction
            + ", minFailurePerHost="
            + minFailurePerHost
            + ", maxAllowedFailedFetchFraction="
            + maxAllowedFailedFetchFraction
            + ", maxStallTimeFraction="
            + maxStallTimeFraction
            + ", minReqProgressFraction="
            + minReqProgressFraction
            + ", checkFailedFetchSinceLastCompletion="
            + checkFailedFetchSinceLastCompletion
            + ", asyncHttp="
            + asyncHttp
            + ", enableFetcherTestingErrors="
            + enableFetcherTestingErrors);
  }

  public void start() throws Exception {
    shuffleSchedulerThread = Thread.currentThread();
    mergeManager.setupParentThread(shuffleSchedulerThread);
    CelebornShuffleSchedulerCallable schedulerCallable = new CelebornShuffleSchedulerCallable();
    schedulerCallable.call();
  }

  public void close() {
    try {
      if (!isShutdown.getAndSet(true)) {
        try {
          logProgress();
        } catch (Exception e) {
          LOG.warn(
              "Failed log progress while closing, ignoring and continuing shutdown. Message={}",
              e.getMessage());
        }

        // Notify and interrupt the waiting scheduler thread
        synchronized (this) {
          notifyAll();
        }
        // Interrupt the ShuffleScheduler thread only if the close is invoked by another thread.
        // If this is invoked on the same thread, then the shuffleRunner has already complete, and
        // there's
        // no point interrupting it.
        // The interrupt is needed to unblock any merges or waits which may be happening, so that
        // the thread can
        // exit.
        if (shuffleSchedulerThread != null
            && !Thread.currentThread().equals(shuffleSchedulerThread)) {
          shuffleSchedulerThread.interrupt();
        }

        // Interrupt the fetchers.
        for (CelebornTezShuffleDataFetcher fetcher : celebornRunningFetchers) {
          try {
            fetcher.shutDown();
          } catch (Exception e) {
            LOG.warn(
                "Error while shutting down fetcher. Ignoring and continuing shutdown. Message={}",
                e.getMessage());
          }
        }

        // Kill the Referee thread.
        try {
          referee.interrupt();
          referee.join();
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while shutting down referee. Ignoring and continuing shutdown");
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOG.warn(
              "Error while shutting down referee. Ignoring and continuing shutdown. Message={}",
              e.getMessage());
        }
      }
    } finally {
      long startTime = System.currentTimeMillis();
      if (!fetcherExecutor.isShutdown()) {
        // Ensure that fetchers respond to cancel request.
        fetcherExecutor.shutdownNow();
      }
      long endTime = System.currentTimeMillis();
      LOG.info(
          "Shutting down fetchers for input: {}, shutdown timetaken: {} ms, "
              + "hasFetcherExecutorStopped: {}",
          srcNameTrimmed,
          (endTime - startTime),
          hasFetcherExecutorStopped());
    }
  }

  @VisibleForTesting
  boolean hasFetcherExecutorStopped() {
    return fetcherExecutor.isShutdown();
  }

  @VisibleForTesting
  public boolean isShutdown() {
    return isShutdown.get();
  }

  protected synchronized void updateEventReceivedTime() {
    long relativeTime = System.currentTimeMillis() - startTime;
    if (firstEventReceived.getValue() == 0) {
      firstEventReceived.setValue(relativeTime);
      lastEventReceived.setValue(relativeTime);
      return;
    }
    lastEventReceived.setValue(relativeTime);
  }

  /**
   * Placeholder for tracking shuffle events in case we get multiple spills info for the same
   * attempt.
   */
  static class ShuffleEventInfo {
    BitSet eventsProcessed;
    int finalEventId = -1; // 0 indexed
    int attemptNum;
    String id;
    boolean scheduledForDownload; // whether chunks got scheduled for download (getMapHost)

    ShuffleEventInfo(InputAttemptIdentifier input) {
      this.id = input.getInputIdentifier() + "_" + input.getAttemptNumber();
      this.eventsProcessed = new BitSet();
      this.attemptNum = input.getAttemptNumber();
    }

    void spillProcessed(int spillId) {
      if (finalEventId != -1) {
        Preconditions.checkState(
            eventsProcessed.cardinality() <= (finalEventId + 1),
            "Wrong state. eventsProcessed cardinality="
                + eventsProcessed.cardinality()
                + " "
                + "finalEventId="
                + finalEventId
                + ", spillId="
                + spillId
                + ", "
                + toString());
      }
      eventsProcessed.set(spillId);
    }

    void setFinalEventId(int spillId) {
      finalEventId = spillId;
    }

    boolean isDone() {
      return ((finalEventId != -1) && (finalEventId + 1) == eventsProcessed.cardinality());
    }

    public String toString() {
      return "[eventsProcessed="
          + eventsProcessed
          + ", finalEventId="
          + finalEventId
          + ", id="
          + id
          + ", attemptNum="
          + attemptNum
          + ", scheduledForDownload="
          + scheduledForDownload
          + "]";
    }
  }

  public synchronized void copySucceeded(
      InputAttemptIdentifier srcAttemptIdentifier,
      MapHost host,
      long bytesCompressed,
      long bytesDecompressed,
      long millis,
      MapOutput output,
      boolean isLocalFetch)
      throws IOException {

    inputContext.notifyProgress();
    if (!isInputFinished(srcAttemptIdentifier.getInputIdentifier())) {
      if (!isLocalFetch) {
        /** Reset it only when it is a non-local-disk copy. */
        failedShufflesSinceLastCompletion = 0;
      }
      if (output != null) {

        failureCounts.remove(srcAttemptIdentifier);
        if (host != null) {
          hostFailures.remove(new HostPort(host.getHost(), host.getPort()));
        }

        output.commit();
        fetchStatsLogger.logIndividualFetchComplete(
            millis,
            bytesCompressed,
            bytesDecompressed,
            output.getType().toString(),
            srcAttemptIdentifier);
        if (output.getType() == Type.DISK) {
          bytesShuffledToDisk.increment(bytesCompressed);
        } else if (output.getType() == Type.DISK_DIRECT) {
          bytesShuffledToDiskDirect.increment(bytesCompressed);
        } else {
          bytesShuffledToMem.increment(bytesCompressed);
        }
        shuffledInputsCounter.increment(1);
      } else {
        // Output null implies that a physical input completion is being
        // registered without needing to fetch data
        skippedInputCounter.increment(1);
      }

      /**
       * In case of pipelined shuffle, it is quite possible that fetchers pulled the FINAL_UPDATE
       * spill in advance due to smaller output size. In such scenarios, we need to wait until we
       * retrieve all spill details to claim success.
       */
      if (!srcAttemptIdentifier.canRetrieveInputInChunks()) {
        remainingMaps.decrementAndGet();
        setInputFinished(srcAttemptIdentifier.getInputIdentifier());
        numFetchedSpills++;
      } else {
        int inputIdentifier = srcAttemptIdentifier.getInputIdentifier();
        // Allow only one task attempt to proceed.
        if (!validateInputAttemptForPipelinedShuffle(srcAttemptIdentifier)) {
          return;
        }

        ShuffleEventInfo eventInfo = pipelinedShuffleInfoEventsMap.get(inputIdentifier);

        // Possible that Shuffle event handler invoked this, due to empty partitions
        if (eventInfo == null && output == null) {
          eventInfo = new ShuffleEventInfo(srcAttemptIdentifier);
          pipelinedShuffleInfoEventsMap.put(inputIdentifier, eventInfo);
        }

        assert (eventInfo != null);
        eventInfo.spillProcessed(srcAttemptIdentifier.getSpillEventId());
        numFetchedSpills++;

        if (srcAttemptIdentifier.getFetchTypeInfo()
            == InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE) {
          eventInfo.setFinalEventId(srcAttemptIdentifier.getSpillEventId());
        }

        // check if we downloaded all spills pertaining to this InputAttemptIdentifier
        if (eventInfo.isDone()) {
          remainingMaps.decrementAndGet();
          setInputFinished(inputIdentifier);
          pipelinedShuffleInfoEventsMap.remove(inputIdentifier);
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "Removing : "
                    + srcAttemptIdentifier
                    + ", pending: "
                    + pipelinedShuffleInfoEventsMap);
          }
        }

        if (LOG.isTraceEnabled()) {
          LOG.trace("eventInfo " + eventInfo.toString());
        }
      }

      if (remainingMaps.get() == 0) {
        notifyAll(); // Notify the getHost() method.
        LOG.info(
            "All inputs fetched for input vertex : " + inputContext.getInputOutputVertexNames());
      }

      // update the status
      lastProgressTime = System.currentTimeMillis();
      totalBytesShuffledTillNow += bytesCompressed;
      logProgress();
      reduceShuffleBytes.increment(bytesCompressed);
      reduceBytesDecompressed.increment(bytesDecompressed);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "src task: "
                + TezRuntimeUtils.getTaskAttemptIdentifier(
                    inputContext.getSourceVertexName(),
                    srcAttemptIdentifier.getInputIdentifier(),
                    srcAttemptIdentifier.getAttemptNumber())
                + " done");
      }
    } else {
      // input is already finished. duplicate fetch.
      LOG.warn("Duplicate fetch of input no longer needs to be fetched: " + srcAttemptIdentifier);
      // free the resource - specially memory

      // If the src does not generate data, output will be null.
      if (output != null) {
        output.abort();
      }
    }
    // TODO NEWTEZ Should this be releasing the output, if not committed ? Possible memory leak in
    // case of speculation.
  }

  private boolean validateInputAttemptForPipelinedShuffle(InputAttemptIdentifier input) {
    // For pipelined shuffle.
    // TODO: TEZ-2132 for error handling. As of now, fail fast if there is a different attempt
    if (input.canRetrieveInputInChunks()) {
      ShuffleEventInfo eventInfo = pipelinedShuffleInfoEventsMap.get(input.getInputIdentifier());
      if (eventInfo != null && input.getAttemptNumber() != eventInfo.attemptNum) {
        /*
         * Check if current attempt has been scheduled for download.
         * e.g currentAttemptNum=0, eventsProcessed={}, newAttemptNum=1
         * If nothing is scheduled in current attempt and no events are processed
         * (i.e copySucceeded), we can ignore current attempt and start processing the new
         * attempt (e.g LLAP).
         */
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
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Ignoring current attempt="
                  + eventInfo.attemptNum
                  + " with eventInfo="
                  + eventInfo.toString()
                  + "and processing new attempt="
                  + input.getAttemptNumber());
        }
      }

      if (eventInfo == null) {
        pipelinedShuffleInfoEventsMap.put(input.getInputIdentifier(), new ShuffleEventInfo(input));
      }
    }
    return true;
  }

  @VisibleForTesting
  void killSelf(Exception exception, String message) {
    LOG.error(message, exception);
    exceptionReporter.killSelf(exception, message);
  }

  private final AtomicInteger nextProgressLineEventCount = new AtomicInteger(0);

  private void logProgress() {
    int inputsDone = numInputs - remainingMaps.get();
    if (inputsDone > nextProgressLineEventCount.get()
        || inputsDone == numInputs
        || isShutdown.get()) {
      nextProgressLineEventCount.addAndGet(50);
      double mbs = (double) totalBytesShuffledTillNow / (1024 * 1024);
      long secsSinceStart = (System.currentTimeMillis() - startTime) / 1000 + 1;

      double transferRate = mbs / secsSinceStart;
      LOG.info(
          "copy("
              + inputsDone
              + " (spillsFetched="
              + numFetchedSpills
              + ") of "
              + numInputs
              + ". Transfer rate (CumulativeDataFetched/TimeSinceInputStarted)) "
              + mbpsFormat.format(transferRate)
              + " MB/s)");
    }
  }

  public synchronized void copyFailed(
      InputAttemptFetchFailure fetchFailure,
      MapHost host,
      boolean readError,
      boolean connectError) {
    failedShuffleCounter.increment(1);
    inputContext.notifyProgress();
    int failures = incrementAndGetFailureAttempt(fetchFailure.getInputAttemptIdentifier());

    if (!fetchFailure.isLocalFetch()) {
      /**
       * Track the number of failures that has happened since last completion. This gets reset on a
       * successful copy.
       */
      failedShufflesSinceLastCompletion++;
    }

    /**
     * Inform AM: - In case of read/connect error - In case attempt failures exceed threshold of
     * maxFetchFailuresBeforeReporting (5) Bail-out if needed: - Check whether individual attempt
     * crossed failure threshold limits - Check overall shuffle health. Bail out if needed.*
     */

    // TEZ-2890
    boolean shouldInformAM =
        (reportReadErrorImmediately && (readError || connectError))
            || ((failures % maxFetchFailuresBeforeReporting) == 0);

    if (shouldInformAM) {
      // Inform AM. In case producer needs to be restarted, it is handled at AM.
      informAM(fetchFailure);
    }

    // Restart consumer in case shuffle is not healthy
    if (!isShuffleHealthy(fetchFailure)) {
      return;
    }

    penalizeHost(host, failures);
  }

  private boolean isAbortLimitExceeedFor(InputAttemptIdentifier srcAttempt) {
    int attemptFailures = getFailureCount(srcAttempt);
    if (attemptFailures >= abortFailureLimit) {
      // This task has seen too many fetch failures - report it as failed. The
      // AM may retry it if max failures has not been reached.

      // Between the task and the AM - someone needs to determine who is at
      // fault. If there's enough errors seen on the task, before the AM informs
      // it about source failure, the task considers itself to have failed and
      // allows the AM to re-schedule it.
      String errorMsg =
          "Failed "
              + attemptFailures
              + " times trying to "
              + "download from "
              + TezRuntimeUtils.getTaskAttemptIdentifier(
                  inputContext.getSourceVertexName(),
                  srcAttempt.getInputIdentifier(),
                  srcAttempt.getAttemptNumber())
              + ". threshold="
              + abortFailureLimit;
      IOException ioe = new IOException(errorMsg);
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      exceptionReporter.reportException(ioe);
      return true;
    }
    return false;
  }

  private void penalizeHost(MapHost host, int failures) {
    host.penalize();

    HostPort hostPort = new HostPort(host.getHost(), host.getPort());
    // TODO TEZ-922 hostFailures isn't really used for anything apart from
    // hasFailedAcrossNodes().Factor it into error
    // reporting / potential blacklisting of hosts.
    if (hostFailures.containsKey(hostPort)) {
      IntWritable x = hostFailures.get(hostPort);
      x.set(x.get() + 1);
    } else {
      hostFailures.put(hostPort, new IntWritable(1));
    }

    long delay = (long) (INITIAL_PENALTY * Math.pow(PENALTY_GROWTH_RATE, failures));
    long penaltyDelay = Math.min(delay, maxPenaltyTime);
    penalties.add(new Penalty(host, penaltyDelay));
  }

  private int getFailureCount(InputAttemptIdentifier srcAttempt) {
    IntWritable failureCount = failureCounts.get(srcAttempt);
    return (failureCount == null) ? 0 : failureCount.get();
  }

  private int incrementAndGetFailureAttempt(InputAttemptIdentifier srcAttempt) {
    int failures = 1;
    if (failureCounts.containsKey(srcAttempt)) {
      IntWritable x = failureCounts.get(srcAttempt);
      x.set(x.get() + 1);
      failures = x.get();
    } else {
      failureCounts.put(srcAttempt, new IntWritable(1));
    }
    return failures;
  }

  public void reportLocalError(IOException ioe) {
    LOG.error(srcNameTrimmed + ": " + "Shuffle failed : caused by local error", ioe);
    // Shuffle knows how to deal with failures post shutdown via the onFailure hook
    exceptionReporter.reportException(ioe);
  }

  // Notify AM
  private void informAM(InputAttemptFetchFailure fetchFailure) {
    InputAttemptIdentifier srcAttempt = fetchFailure.getInputAttemptIdentifier();
    LOG.info(
        "{}: Reporting fetch failure for InputIdentifier: {} taskAttemptIdentifier: {}, "
            + "local fetch: {}, remote fetch failure reported as local failure: {}) to AM.",
        srcNameTrimmed,
        srcAttempt,
        TezRuntimeUtils.getTaskAttemptIdentifier(
            inputContext.getSourceVertexName(),
            srcAttempt.getInputIdentifier(),
            srcAttempt.getAttemptNumber()),
        fetchFailure.isLocalFetch(),
        fetchFailure.isDiskErrorAtSource());
    List<Event> failedEvents = Lists.newArrayListWithCapacity(1);
    failedEvents.add(
        InputReadErrorEvent.create(
            "Fetch failure for "
                + TezRuntimeUtils.getTaskAttemptIdentifier(
                    inputContext.getSourceVertexName(),
                    srcAttempt.getInputIdentifier(),
                    srcAttempt.getAttemptNumber())
                + " to jobtracker.",
            srcAttempt.getInputIdentifier(),
            srcAttempt.getAttemptNumber(),
            fetchFailure.isLocalFetch(),
            fetchFailure.isDiskErrorAtSource(),
            localHostname));

    inputContext.sendEvents(failedEvents);
  }

  /**
   * To determine if failures happened across nodes or not. This will help in determining whether
   * this task needs to be restarted or source needs to be restarted.
   *
   * @param logContext context info for logging
   * @return boolean true indicates this task needs to be restarted
   */
  private boolean hasFailedAcrossNodes(String logContext) {
    int numUniqueHosts = uniqueHosts.size();
    Preconditions.checkArgument(numUniqueHosts > 0, "No values in unique hosts");
    int threshold = Math.max(3, (int) Math.ceil(numUniqueHosts * hostFailureFraction));
    int total = 0;
    boolean failedAcrossNodes = false;
    for (HostPort host : uniqueHosts) {
      IntWritable failures = hostFailures.get(host);
      if (failures != null && failures.get() > minFailurePerHost) {
        total++;
        failedAcrossNodes = (total > (threshold * minFailurePerHost));
        if (failedAcrossNodes) {
          break;
        }
      }
    }

    LOG.info(
        logContext
            + ", numUniqueHosts="
            + numUniqueHosts
            + ", hostFailureThreshold="
            + threshold
            + ", hostFailuresCount="
            + hostFailures.size()
            + ", hosts crossing threshold="
            + total
            + ", reducerFetchIssues="
            + failedAcrossNodes);

    return failedAcrossNodes;
  }

  private boolean allEventsReceived() {
    if (!pipelinedShuffleInfoEventsMap.isEmpty()) {
      return (pipelinedShuffleInfoEventsMap.size() == numInputs);
    } else {
      // no pipelining
      return ((pathToIdentifierMap.size() + skippedInputCounter.getValue()) == numInputs);
    }
  }

  private boolean isAllInputFetched() {
    return allEventsReceived() && (successRssPartitionSet.size() >= allRssPartition.size());
  }

  /**
   * Check if consumer needs to be restarted based on total failures w.r.t completed outputs and
   * based on number of errors that have happened since last successful completion. Consider into
   * account whether failures have been seen across different nodes.
   *
   * @return true to indicate fetchers are healthy
   */
  private boolean isFetcherHealthy(String logContext) {

    long totalFailures = failedShuffleCounter.getValue();
    int doneMaps = numInputs - remainingMaps.get();

    boolean fetcherHealthy = true;
    if (doneMaps > 0) {
      fetcherHealthy =
          (((float) totalFailures / (totalFailures + doneMaps)) < maxAllowedFailedFetchFraction);
    }

    if (fetcherHealthy) {
      // Compute this logic only when all events are received
      if (allEventsReceived()) {
        if (hostFailureFraction > 0) {
          boolean failedAcrossNodes = hasFailedAcrossNodes(logContext);
          if (failedAcrossNodes) {
            return false; // not healthy
          }
        }

        if (checkFailedFetchSinceLastCompletion) {
          /**
           * remainingMaps works better instead of pendingHosts in the following condition because
           * of the way the fetcher reports failures
           */
          if (failedShufflesSinceLastCompletion >= remainingMaps.get() * minFailurePerHost) {
            /**
             * Check if lots of errors are seen after last progress time.
             *
             * <p>E.g totalFailures = 20. doneMaps = 320 - 300; fetcherHealthy = (20/(20+300)) <
             * 0.5. So reducer would be marked as healthy. Assume 20 errors happen when downloading
             * the last 20 attempts. Host failure & individual attempt failures would keep
             * increasing; but at very slow rate 15 * 180 seconds per attempt to find out the issue.
             *
             * <p>Instead consider the new errors with the pending items to be fetched. Assume 21
             * new errors happened after last progress; remainingMaps = (320-300) = 20; (21 / (21 +
             * 20)) > 0.5 So we reset the reducer to unhealthy here (special case)
             *
             * <p>In normal conditions (i.e happy path), this wouldn't even cause any issue as
             * failedShufflesSinceLastCompletion is reset as soon as we see successful download.
             */
            fetcherHealthy =
                (((float) failedShufflesSinceLastCompletion
                        / (failedShufflesSinceLastCompletion + remainingMaps.get()))
                    < maxAllowedFailedFetchFraction);

            LOG.info(
                logContext
                    + ", fetcherHealthy="
                    + fetcherHealthy
                    + ", failedShufflesSinceLastCompletion="
                    + failedShufflesSinceLastCompletion
                    + ", remainingMaps="
                    + remainingMaps.get());
          }
        }
      }
    }
    return fetcherHealthy;
  }

  boolean isShuffleHealthy(InputAttemptFetchFailure fetchFailure) {
    InputAttemptIdentifier srcAttempt = fetchFailure.getInputAttemptIdentifier();
    if (isAbortLimitExceeedFor(srcAttempt)) {
      return false;
    }

    final float MIN_REQUIRED_PROGRESS_PERCENT = minReqProgressFraction;
    final float MAX_ALLOWED_STALL_TIME_PERCENT = maxStallTimeFraction;

    int doneMaps = numInputs - remainingMaps.get();

    String logContext = "srcAttempt=" + srcAttempt.toString();
    boolean fetcherHealthy = isFetcherHealthy(logContext);

    // check if the reducer has progressed enough
    boolean reducerProgressedEnough =
        (((float) doneMaps / numInputs) >= MIN_REQUIRED_PROGRESS_PERCENT);

    // check if the reducer is stalled for a long time
    // duration for which the reducer is stalled
    int stallDuration = (int) (System.currentTimeMillis() - lastProgressTime);

    // duration for which the reducer ran with progress
    int shuffleProgressDuration = (int) (lastProgressTime - startTime);

    boolean reducerStalled =
        (shuffleProgressDuration > 0)
            && (((float) stallDuration / shuffleProgressDuration)
                >= MAX_ALLOWED_STALL_TIME_PERCENT);

    // kill if not healthy and has insufficient progress
    if ((failureCounts.size() >= maxFailedUniqueFetches
            || failureCounts.size() == (numInputs - doneMaps))
        && !fetcherHealthy
        && (!reducerProgressedEnough || reducerStalled)) {
      String errorMsg =
          (srcNameTrimmed
                  + ": "
                  + "Shuffle failed with too many fetch failures and insufficient progress: "
                  + "[failureCounts="
                  + failureCounts.size()
                  + ", pendingInputs="
                  + (numInputs - doneMaps)
                  + ", fetcherHealthy="
                  + fetcherHealthy
                  + ", reducerProgressedEnough="
                  + reducerProgressedEnough
                  + ", reducerStalled="
                  + reducerStalled
                  + ", hostFailures="
                  + hostFailures)
              + "]";
      LOG.error(errorMsg);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Host failures=" + hostFailures.keySet());
      }
      // Shuffle knows how to deal with failures post shutdown via the onFailure hook
      exceptionReporter.reportException(new IOException(errorMsg, fetchFailure.getCause()));
      return false;
    }
    return true;
  }

  public synchronized void addKnownMapOutput(
      String inputHostName, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {

    allRssPartition.add(partitionId);
    if (!partitionIdToSuccessMapTaskAttempts.containsKey(partitionId)) {
      partitionIdToSuccessMapTaskAttempts.put(partitionId, new HashSet<>());
    }
    partitionIdToSuccessMapTaskAttempts.get(partitionId).add(srcAttempt);

    uniqueHosts.add(new HostPort(inputHostName, port));
    HostPortPartition identifier = new HostPortPartition(inputHostName, port, partitionId);

    MapHost host = mapLocations.get(identifier);
    if (host == null) {
      host = new MapHost(inputHostName, port, partitionId, srcAttempt.getInputIdentifierCount());
      mapLocations.put(identifier, host);
    }

    // Allow only one task attempt to proceed.
    if (!validateInputAttemptForPipelinedShuffle(srcAttempt)) {
      return;
    }

    host.addKnownMap(srcAttempt);
    for (int i = 0; i < srcAttempt.getInputIdentifierCount(); i++) {
      PathPartition pathPartition =
          new PathPartition(srcAttempt.getPathComponent(), partitionId + i);
      pathToIdentifierMap.put(pathPartition, srcAttempt.expand(i));
    }

    // Mark the host as pending
    if (host.getState() == MapHost.State.PENDING) {
      pendingHosts.add(host);
      notifyAll();
    }
  }

  public void obsoleteInput(InputAttemptIdentifier srcAttempt) {
    // The incoming srcAttempt does not contain a path component.
    LOG.info(srcNameTrimmed + ": " + "Adding obsolete input: " + srcAttempt);
    ShuffleEventInfo eventInfo = pipelinedShuffleInfoEventsMap.get(srcAttempt.getInputIdentifier());

    // Pipelined shuffle case (where pipelinedShuffleInfoEventsMap gets populated).
    // Fail fast here.
    if (eventInfo != null) {
      // In case this we haven't started downloading it, get rid of it.
      if (eventInfo.eventsProcessed.isEmpty() && !eventInfo.scheduledForDownload) {
        // obsoleted anyways; no point tracking if nothing is started
        pipelinedShuffleInfoEventsMap.remove(srcAttempt.getInputIdentifier());
        LOG.debug("Removing {} from tracking", eventInfo);
        return;
      }
      IOException exception =
          new IOException(
              srcAttempt
                  + " is marked as obsoleteInput, but it "
                  + "exists in shuffleInfoEventMap. Some data could have been already merged "
                  + "to memory/disk outputs. Failing the fetch early. eventInfo: "
                  + eventInfo);
      String message =
          "Got obsolete event. Killing self as attempt's data could have been consumed";
      killSelf(exception, message);
      return;
    }
    synchronized (this) {
      obsoleteInputs.add(srcAttempt);
    }
  }

  public synchronized void putBackKnownMapOutput(MapHost host, InputAttemptIdentifier srcAttempt) {
    host.addKnownMap(srcAttempt);
  }

  public synchronized MapHost getHost() throws InterruptedException {
    while (pendingHosts.isEmpty() && !isAllInputFetched()) {
      LOG.debug("PendingHosts={}", pendingHosts);
      waitAndNotifyProgress();
    }

    if (!pendingHosts.isEmpty()) {

      MapHost host = null;
      Iterator<MapHost> iter = pendingHosts.iterator();
      int numToPick = random.nextInt(pendingHosts.size());
      for (int i = 0; i <= numToPick; ++i) {
        host = iter.next();
      }

      pendingHosts.remove(host);
      host.markBusy();
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            srcNameTrimmed
                + ": "
                + "Assigning "
                + host
                + " with "
                + host.getNumKnownMapOutputs()
                + " to "
                + Thread.currentThread().getName());
      }
      shuffleStart.set(System.currentTimeMillis());
      return host;
    } else {
      return null;
    }
  }

  public InputAttemptIdentifier getIdentifierForFetchedOutput(String path, int reduceId) {
    return pathToIdentifierMap.get(new PathPartition(path, reduceId));
  }

  private synchronized boolean inputShouldBeConsumed(InputAttemptIdentifier id) {
    boolean isInputFinished = false;
    if (id instanceof CompositeInputAttemptIdentifier) {
      CompositeInputAttemptIdentifier cid = (CompositeInputAttemptIdentifier) id;
      isInputFinished =
          isInputFinished(
              cid.getInputIdentifier(), cid.getInputIdentifier() + cid.getInputIdentifierCount());
    } else {
      isInputFinished = isInputFinished(id.getInputIdentifier());
    }
    return !obsoleteInputs.contains(id) && !isInputFinished;
  }

  public synchronized List<InputAttemptIdentifier> getMapsForHost(MapHost host) {
    List<InputAttemptIdentifier> origList = host.getAndClearKnownMaps();

    ListMultimap<Integer, InputAttemptIdentifier> dedupedList = LinkedListMultimap.create();

    Iterator<InputAttemptIdentifier> listItr = origList.iterator();
    while (listItr.hasNext()) {
      // we may want to try all versions of the input but with current retry
      // behavior older ones are likely to be lost and should be ignored.
      // This may be removed after TEZ-914
      InputAttemptIdentifier id = listItr.next();
      if (inputShouldBeConsumed(id)) {
        Integer inputNumber = Integer.valueOf(id.getInputIdentifier());
        List<InputAttemptIdentifier> oldIdList = dedupedList.get(inputNumber);

        if (oldIdList == null || oldIdList.isEmpty()) {
          dedupedList.put(inputNumber, id);
          continue;
        }

        // In case of pipelined shuffle, we can have multiple spills. In such cases, we can have
        // more than one item in the oldIdList.
        boolean addIdentifierToList = false;
        Iterator<InputAttemptIdentifier> oldIdIterator = oldIdList.iterator();
        while (oldIdIterator.hasNext()) {
          InputAttemptIdentifier oldId = oldIdIterator.next();

          // no need to add if spill ids are same
          if (id.canRetrieveInputInChunks()) {
            if (oldId.getSpillEventId() == id.getSpillEventId()) {
              // TODO: need to handle deterministic spills later.
              addIdentifierToList = false;
              continue;
            } else if (oldId.getAttemptNumber() == id.getAttemptNumber()) {
              // but with different spill id.
              addIdentifierToList = true;
              break;
            }
          }

          // if its from different attempt, take the latest attempt
          if (oldId.getAttemptNumber() < id.getAttemptNumber()) {
            // remove existing identifier
            oldIdIterator.remove();
            LOG.warn(
                "Old Src for InputIndex: "
                    + inputNumber
                    + " with attemptNumber: "
                    + oldId.getAttemptNumber()
                    + " was not determined to be invalid. Ignoring it for now in favour of "
                    + id.getAttemptNumber());
            addIdentifierToList = true;
            break;
          }
        }
        if (addIdentifierToList) {
          dedupedList.put(inputNumber, id);
        }
      } else {
        LOG.info("Ignoring finished or obsolete source: " + id);
      }
    }

    // Compute the final list, limited by NUM_FETCHERS_AT_ONCE
    List<InputAttemptIdentifier> result = new ArrayList<InputAttemptIdentifier>();
    int includedMaps = 0;
    int totalSize = dedupedList.size();

    for (Integer inputIndex : dedupedList.keySet()) {
      List<InputAttemptIdentifier> attemptIdentifiers = dedupedList.get(inputIndex);
      for (InputAttemptIdentifier inputAttemptIdentifier : attemptIdentifiers) {
        if (includedMaps++ >= maxTaskOutputAtOnce) {
          host.addKnownMap(inputAttemptIdentifier);
        } else {
          if (inputAttemptIdentifier.canRetrieveInputInChunks()) {
            ShuffleEventInfo shuffleEventInfo =
                pipelinedShuffleInfoEventsMap.get(inputAttemptIdentifier.getInputIdentifier());
            if (shuffleEventInfo != null) {
              shuffleEventInfo.scheduledForDownload = true;
            }
          }
          result.add(inputAttemptIdentifier);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "assigned "
              + includedMaps
              + " of "
              + totalSize
              + " to "
              + host
              + " to "
              + Thread.currentThread().getName());
    }
    return result;
  }

  public synchronized void freeHost(MapHost host) {
    if (host.getState() != MapHost.State.PENALIZED) {
      if (host.markAvailable() == MapHost.State.PENDING) {
        pendingHosts.add(host);
        notifyAll();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          host
              + " freed by "
              + Thread.currentThread().getName()
              + " in "
              + (System.currentTimeMillis() - shuffleStart.get())
              + "ms");
    }
  }

  public synchronized void resetKnownMaps() {
    mapLocations.clear();
    obsoleteInputs.clear();
    pendingHosts.clear();
    pathToIdentifierMap.clear();
  }

  /**
   * Utility method to check if the Shuffle data fetch is complete.
   *
   * @return true if complete
   */
  public synchronized boolean isDone() {
    return remainingMaps.get() == 0;
  }

  /** A structure that records the penalty for a host. */
  static class Penalty implements Delayed {
    MapHost host;
    private long endTime;

    Penalty(MapHost host, long delay) {
      this.host = host;
      this.endTime = System.currentTimeMillis() + delay;
    }

    public long getDelay(TimeUnit unit) {
      long remainingTime = endTime - System.currentTimeMillis();
      return unit.convert(remainingTime, TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed o) {
      long other = ((Penalty) o).endTime;
      return endTime == other ? 0 : (endTime < other ? -1 : 1);
    }
  }

  /** A thread that takes hosts off of the penalty list when the timer expires. */
  private class Referee extends Thread {
    public Referee() {
      setName(
          "ShufflePenaltyReferee {"
              + TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName())
              + "}");
      setDaemon(true);
    }

    public void run() {
      try {
        while (!isShutdown.get()) {
          // take the first host that has an expired penalty
          MapHost host = penalties.take().host;
          synchronized (CelebornScheduler.this) {
            if (host.markAvailable() == MapHost.State.PENDING) {
              pendingHosts.add(host);
              CelebornScheduler.this.notifyAll();
            }
          }
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        // This handles shutdown of the entire fetch / merge process.
      } catch (Throwable t) {
        // Shuffle knows how to deal with failures post shutdown via the onFailure hook
        exceptionReporter.reportException(t);
      }
    }
  }

  void setInputFinished(int inputIndex) {
    synchronized (finishedMaps) {
      finishedMaps.set(inputIndex, true);
    }
  }

  boolean isInputFinished(int inputIndex) {
    synchronized (finishedMaps) {
      return finishedMaps.get(inputIndex);
    }
  }

  boolean isInputFinished(int inputIndex, int inputEnd) {
    synchronized (finishedMaps) {
      return finishedMaps.nextClearBit(inputIndex) > inputEnd;
    }
  }

  private class CelebornShuffleSchedulerCallable extends CallableWithNdc<Void> {

    @Override
    protected Void callInternal() throws InterruptedException {
      while (!isShutdown.get() && !isAllInputFetched()) {
        synchronized (CelebornScheduler.this) {
          while (!allEventsReceived()
              || ((celebornRunningFetchers.size() >= numFetchers || pendingHosts.isEmpty())
                  && !isAllInputFetched())) {
            try {
              waitAndNotifyProgress();
            } catch (InterruptedException e) {
              if (isShutdown.get()) {
                LOG.info(
                    srcNameTrimmed
                        + ": "
                        + "Interrupted while waiting for fetchers to complete "
                        + "and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
                Thread.currentThread().interrupt();
                break;
              } else {
                throw e;
              }
            }
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(
              srcNameTrimmed + ": " + "NumCompletedInputs: {}" + (numInputs - remainingMaps.get()));
        }

        // Ensure there's memory available before scheduling the next Fetcher.
        try {
          // If merge is on, block
          mergeManager.waitForInMemoryMerge();
          // In case usedMemory > memorylimit, wait until some memory is released
          mergeManager.waitForShuffleToMergeMemory();
        } catch (InterruptedException e) {
          if (isShutdown.get()) {
            LOG.info(
                srcNameTrimmed
                    + ": "
                    + "Interrupted while waiting for merge to complete and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
            Thread.currentThread().interrupt();
            break;
          } else {
            throw e;
          }
        }

        if (!isShutdown.get() && !isAllInputFetched()) {
          synchronized (CelebornScheduler.this) {
            int numFetchersToRun = numFetchers - celebornRunningFetchers.size();
            int count = 0;
            while (count < numFetchersToRun && !isShutdown.get() && !isAllInputFetched()) {
              MapHost mapHost;
              try {
                mapHost = getHost(); // Leads to a wait.
              } catch (InterruptedException e) {
                if (isShutdown.get()) {
                  LOG.info(
                      srcNameTrimmed
                          + ": "
                          + "Interrupted while waiting for host and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
                  Thread.currentThread().interrupt();
                  break;
                } else {
                  throw e;
                }
              }
              if (mapHost == null) {
                break; // Check for the exit condition.
              }
              LOG.debug("{}: Processing pending host: {}", srcNameTrimmed, mapHost);
              if (!isShutdown.get()) {
                count++;
                if (LOG.isDebugEnabled()) {
                  LOG.debug(
                      srcNameTrimmed + ": " + "Scheduling fetch for inputHost: {}",
                      mapHost.getHostIdentifier() + ":" + mapHost.getPartitionId());
                }

                if (isFirstRssPartitionFetch(mapHost)) {
                  CelebornTezShuffleDataFetcher celebornTezShuffleDataFetcher =
                      constructCelebornFetcherForPartition(mapHost);

                  celebornRunningFetchers.add(celebornTezShuffleDataFetcher);
                  ListenableFuture<Void> future =
                      fetcherExecutor.submit(celebornTezShuffleDataFetcher);
                  Futures.addCallback(
                      future,
                      new FetchFutureCallback(celebornTezShuffleDataFetcher),
                      MoreExecutors.directExecutor());
                } else {
                  for (int i = 0; i < mapHost.getAndClearKnownMaps().size(); i++) {
                    remainingMaps.decrementAndGet();
                  }
                  LOG.info(
                      "Partition was fetched, remainingMaps desc, now value:{}",
                      remainingMaps.get());
                }
              }
            }
          }
        }
      }
      LOG.info(
          "Shutting down FetchScheduler for input: {}, wasInterrupted={}",
          srcNameTrimmed,
          Thread.currentThread().isInterrupted());
      if (!fetcherExecutor.isShutdown()) {
        fetcherExecutor.shutdownNow();
      }
      return null;
    }

    private synchronized boolean isFirstRssPartitionFetch(MapHost mapHost) {
      Integer partitionId = mapHost.getPartitionId();
      LOG.info("Check isFirstRssPartitionFetch, mapHost:{},partitionId:{}", mapHost, partitionId);

      if (runningRssPartitionMap.containsKey(partitionId)
          || successRssPartitionSet.contains(partitionId)) {
        return false;
      }
      runningRssPartitionMap.put(partitionId, mapHost);
      return true;
    }

    private CelebornTezShuffleDataFetcher constructCelebornFetcherForPartition(MapHost mapHost) {
      int partitionId = mapHost.getPartitionId();
      CelebornTezReader reader =
          new CelebornTezReader(
              shuffleClient, partitionId, shuffleId, applicationAttemptId.getAttemptId());
      CelebornTezShuffleDataFetcher fetcher =
          new CelebornTezShuffleDataFetcher(
              partitionIdToSuccessMapTaskAttempts.get(mapHost.getPartitionId()).iterator().next(),
              mapHost.getPartitionId(),
              mergeManager,
              inputContext.getCounters(),
              reader,
              exceptionReporter);
      return fetcher;
    }
  }

  private synchronized void waitAndNotifyProgress() throws InterruptedException {
    inputContext.notifyProgress();
    wait(1000);
  }

  private class FetchFutureCallback implements FutureCallback<Void> {

    private final CelebornTezShuffleDataFetcher fetcherOrderedGrouped;
    private final Integer partitionId;

    public FetchFutureCallback(CelebornTezShuffleDataFetcher fetcherOrderedGrouped) {
      this.fetcherOrderedGrouped = fetcherOrderedGrouped;
      this.partitionId = fetcherOrderedGrouped.getPartitionId();
    }

    private void doBookKeepingForFetcherComplete() {
      synchronized (CelebornScheduler.this) {
        celebornRunningFetchers.remove(fetcherOrderedGrouped);
        CelebornScheduler.this.notifyAll();
      }
    }

    @Override
    public void onSuccess(Void result) {
      fetcherOrderedGrouped.shutDown();
      if (isShutdown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring fetch complete");
      } else {
        successRssPartitionSet.add(partitionId);
        MapHost mapHost = runningRssPartitionMap.remove(partitionId);
        if (mapHost != null) {
          for (int i = 0; i < mapHost.getAndClearKnownMaps().size(); i++) {
            remainingMaps.decrementAndGet();
          }
        }
        doBookKeepingForFetcherComplete();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      fetcherOrderedGrouped.shutDown();
      if (isShutdown.get()) {
        LOG.info(srcNameTrimmed + ": " + "Already shutdown. Ignoring fetch complete");
      } else {
        LOG.error(srcNameTrimmed + ": " + "Fetcher failed with error", t);
        exceptionReporter.reportException(t);
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
