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

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.getParentPrivateField;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;

class CelebornScheduler extends ShuffleScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleScheduler.class);

  private final AtomicBoolean isShutdown;
  private final Random random = new Random(System.currentTimeMillis());

  private final String srcNameTrimmed;

  private final int numFetchers;
  private final ListeningExecutorService fetcherExecutor;
  private final Set<CelebornTezShuffleDataFetcher> celebornRunningFetchers =
      Collections.newSetFromMap(new ConcurrentHashMap<CelebornTezShuffleDataFetcher, Boolean>());
  private volatile Thread shuffleSchedulerThread = null;
  private final MergeManager mergeManager;
  private final InputContext inputContext;
  private final ExceptionReporter exceptionReporter;
  private final int numInputs;

  // celeborn
  private final int shuffleId;
  private final ApplicationAttemptId applicationAttemptId;
  private final ShuffleClient shuffleClient;
  private final Map<Integer, MapHost> runningRssPartitionMap = new HashMap<>();

  private final Set<Integer> successRssPartitionSet = Sets.newConcurrentHashSet();
  private final Set<Integer> allRssPartition = Sets.newConcurrentHashSet();

  private final Map<Integer, Set<InputAttemptIdentifier>> partitionIdToSuccessMapTaskAttempts =
      new HashMap<>();
  final Map<Integer, Set<TezTaskID>> partitionIdToSuccessTezTasks = new HashMap<>();
  private final TezCounter skippedInputCounter;

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
    this.exceptionReporter = exceptionReporter;
    this.srcNameTrimmed = srcNameTrimmed;
    this.shuffleClient = shuffleClient;
    this.shuffleId = shuffleId;
    this.applicationAttemptId = applicationAttemptId;
    this.mergeManager = mergeManager;
    this.numInputs = numberOfInputs;
    this.numFetchers = (int) getParentPrivateField(this, "numFetchers");
    this.fetcherExecutor =
        (ListeningExecutorService) getParentPrivateField(this, "fetcherExecutor");
    this.isShutdown = (AtomicBoolean) getParentPrivateField(this, "isShutdown");
    this.skippedInputCounter = (TezCounter) getParentPrivateField(this, "skippedInputCounter");
  }

  public void start() throws Exception {
    shuffleSchedulerThread = Thread.currentThread();
    mergeManager.setupParentThread(shuffleSchedulerThread);
    CelebornShuffleSchedulerCallable schedulerCallable = new CelebornShuffleSchedulerCallable();
    schedulerCallable.call();
  }

  private boolean allInputTaskAttemptDone() {
    return (this.partitionIdToSuccessTezTasks.values().stream().mapToInt(s -> s.size()).sum()
            + skippedInputCounter.getValue())
        == numInputs;
  }

  private boolean isAllInputFetched() {
    return allInputTaskAttemptDone() && (successRssPartitionSet.size() >= allRssPartition.size());
  }

  public synchronized void addKnownMapOutput(
      String inputHostName, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {

    allRssPartition.add(partitionId);
    Set<InputAttemptIdentifier> inputAttemptIdentifiers =
        partitionIdToSuccessMapTaskAttempts.computeIfAbsent(partitionId, id -> new HashSet<>());
    String pathComponent = srcAttempt.getPathComponent();
    TezTaskAttemptID tezTaskAttemptId =
        TezTaskAttemptID.fromString(pathComponent.substring(0, pathComponent.length() - 6));
    partitionIdToSuccessTezTasks.putIfAbsent(partitionId, new HashSet<>());
    partitionIdToSuccessTezTasks.get(partitionId).add(tezTaskAttemptId.getTaskID());

    inputAttemptIdentifiers.add(srcAttempt);
    super.addKnownMapOutput(inputHostName, port, partitionId, srcAttempt);
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
      return host;
    } else {
      return null;
    }
  }

  private class CelebornShuffleSchedulerCallable extends CallableWithNdc<Void> {

    @Override
    protected Void callInternal() throws InterruptedException {
      while (!isShutdown.get() && !isAllInputFetched()) {
        synchronized (CelebornScheduler.this) {
          while (!allInputTaskAttemptDone()
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
      LOG.info(
          "Check isFirstCelebornPartitionFetch, mapHost:{},partitionId:{}", mapHost, partitionId);

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
              shuffleClient, shuffleId, partitionId, applicationAttemptId.getAttemptId());
      return new CelebornTezShuffleDataFetcher(
          partitionIdToSuccessMapTaskAttempts.get(mapHost.getPartitionId()).iterator().next(),
          mapHost.getPartitionId(),
          mergeManager,
          inputContext.getCounters(),
          reader,
          exceptionReporter);
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
