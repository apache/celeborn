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
package org.apache.tez.runtime.library.common.shuffle.impl;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.getParentPrivateField;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.*;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.*;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.CelebornTezReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

public class CelebornShuffleManager extends ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornShuffleManager.class);

  private final InputContext inputContext;
  private final int numInputs;
  private final ListeningExecutorService schedulerExecutor;
  private final int shuffleId;
  private final ApplicationAttemptId applicationAttemptId;
  private final ShuffleClient shuffleClient;
  private final FetchedInputAllocator inputManager;

  private final Set<CelebornFetcher> celebornRunningFetchers;
  private final BlockingQueue<FetchedInput> completedInputs;
  private final BlockingQueue<InputHost> pendingHosts;

  private final AtomicInteger numCompletedInputs = new AtomicInteger(0);
  // Required to be held when manipulating pendingHosts
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition wakeLoop = lock.newCondition();

  private final int numFetchers;
  private final int maxTimeToWaitForReportMillis;
  private final String sourceDestNameTrimmed;
  private final TezCounter shufflePhaseTime;

  private final AtomicBoolean isShutdown;
  private final long startTime;

  private volatile Throwable shuffleError;
  private final Set<Integer> successRssPartitionSet = new HashSet<>();
  private final Set<Integer> runningRssPartitionMap = new HashSet<>();
  private final Set<Integer> allRssPartition = Sets.newConcurrentHashSet();
  private final BlockingQueue<Integer> pendingPartition = new LinkedBlockingQueue<>();
  Map<Integer, List<InputAttemptIdentifier>> partitionToInput = new HashMap<>();
  private final AtomicInteger numNoDataInput = new AtomicInteger(0);
  private final AtomicInteger numWithDataInput = new AtomicInteger(0);
  private final boolean broadcastOrOneToOne;

  public CelebornShuffleManager(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      ShuffleClient shuffleClient,
      int bufferSize,
      boolean ifileReadAheadEnabled,
      int ifileReadAheadLength,
      CompressionCodec codec,
      FetchedInputAllocator inputAllocator,
      int shuffleId,
      ApplicationAttemptId applicationAttemptId,
      boolean broadcastOrOneToOne)
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
    this.shuffleId = shuffleId;
    this.applicationAttemptId = applicationAttemptId;
    this.shuffleClient = shuffleClient;
    this.broadcastOrOneToOne = broadcastOrOneToOne;
    this.inputManager = inputAllocator;
    this.celebornRunningFetchers =
        Collections.newSetFromMap(new ConcurrentHashMap<CelebornFetcher, Boolean>());
    this.numInputs = numInputs;
    this.numFetchers = (int) getParentPrivateField(this, "numFetchers");
    this.schedulerExecutor =
        (ListeningExecutorService) getParentPrivateField(this, "schedulerExecutor");
    this.maxTimeToWaitForReportMillis =
        (int) getParentPrivateField(this, "maxTimeToWaitForReportMillis");
    this.isShutdown = (AtomicBoolean) getParentPrivateField(this, "isShutdown");
    this.sourceDestNameTrimmed = (String) getParentPrivateField(this, "sourceDestNameTrimmed");
    this.completedInputs =
        (BlockingQueue<FetchedInput>) getParentPrivateField(this, "completedInputs");
    this.pendingHosts = (BlockingQueue<InputHost>) getParentPrivateField(this, "pendingHosts");
    this.shufflePhaseTime = (TezCounter) getParentPrivateField(this, "shufflePhaseTime");
    this.startTime = (long) getParentPrivateField(this, "startTime");
  }

  @Override
  public void run() throws IOException {
    Preconditions.checkState(inputManager != null, "InputManager must be configured");
    // todo maxTimeToWaitForReportMillis
    ListenableFuture<Void> runShuffleFuture =
        schedulerExecutor.submit(new CelebornRunShuffleCallable());
    Futures.addCallback(
        runShuffleFuture, new SchedulerFutureCallback(), GuavaShim.directExecutor());
    // Shutdown this executor once this task, and the callback complete.
    schedulerExecutor.shutdown();
  }

  private boolean isAllInputFetched() {
    LOG.info(
        "Check isAllInputFetched, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{},  allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
    return (numNoDataInput.get() + numWithDataInput.get() >= numInputs)
        && (successRssPartitionSet.size() >= allRssPartition.size());
  }

  private boolean isAllInputAdded() {
    LOG.info(
        "Check isAllInputAdded, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{}, allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
    return numNoDataInput.get() + numWithDataInput.get() >= numInputs;
  }

  private class CelebornRunShuffleCallable extends CallableWithNdc<Void> {

    public CelebornRunShuffleCallable() {}

    @Override
    protected Void callInternal() throws Exception {
      while (!isShutdown.get() && !isAllInputFetched()) {
        lock.lock();
        try {
          while (((celebornRunningFetchers.size() >= numFetchers || pendingPartition.isEmpty())
                  && !isAllInputFetched())
              || !isAllInputAdded()) {
            inputContext.notifyProgress();
            boolean ret = wakeLoop.await(1000, TimeUnit.MILLISECONDS);
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
        if (!isAllInputFetched() && !isShutdown.get()) {
          lock.lock();
          try {
            int maxFetchersToRun = numFetchers - celebornRunningFetchers.size();
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
                    sourceDestNameTrimmed + ": " + "Processing pending partition: " + partition);
              }
              if (!isShutdown.get()
                  && (!successRssPartitionSet.contains(partition)
                      && !runningRssPartitionMap.contains(partition))) {

                runningRssPartitionMap.add(partition);

                CelebornFetcher fetcher = constructFetcherForCeleborn(partition);
                celebornRunningFetchers.add(fetcher);
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
                          + " since is shutdown");
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

  CelebornFetcher constructFetcherForCeleborn(int partition) {
    CelebornTezReader reader =
        new CelebornTezReader(
            shuffleClient, shuffleId, partition, applicationAttemptId.getAttemptId());
    CelebornFetcher celebornFetcher = new CelebornFetcher(this, inputManager, reader);
    return celebornFetcher;
  }

  /////////////////// Methods for InputEventHandler

  public void addKnownInput(
      String hostName,
      int port,
      CompositeInputAttemptIdentifier srcAttemptIdentifier,
      int srcPhysicalIndex) {
    HostPort identifier = new HostPort(hostName, port);
    super.addKnownInput(hostName, port, srcAttemptIdentifier, srcPhysicalIndex);
    lock.lock();
    try {
      // pendingHosts is useless in celeborn
      pendingHosts.remove(identifier);
      if (broadcastOrOneToOne) {
        String pathComponent = srcAttemptIdentifier.getPathComponent();
        TezTaskAttemptID taskAttemptId =
            TezTaskAttemptID.fromString(
                CelebornTezUtils.uniqueIdentifierToAttemptId(pathComponent));
        int partitionId = taskAttemptId.getTaskID().getId();
        LOG.info("Read partition, shuffleId {} partitionId {}", shuffleId, partitionId);
        if (!allRssPartition.contains(partitionId)) {
          pendingPartition.add(partitionId);
        }
        allRssPartition.add(partitionId);
        partitionToInput.putIfAbsent(partitionId, new ArrayList<>());
        partitionToInput.get(partitionId).add(srcAttemptIdentifier);
      } else {
        for (int i = 0; i < srcAttemptIdentifier.getInputIdentifierCount(); i++) {
          int p = srcPhysicalIndex + i;
          LOG.info(
              "PartitionToInput, original:{}, add:{},  now:{}",
              srcAttemptIdentifier,
              srcAttemptIdentifier.expand(i),
              partitionToInput.get(p));
          if (!allRssPartition.contains(srcPhysicalIndex + i)) {
            pendingPartition.add(p);
          }
          allRssPartition.add(p);
          partitionToInput.putIfAbsent(p, new ArrayList<>());
          partitionToInput.get(p).add(srcAttemptIdentifier);
          LOG.info("Add partition:{}, after add, now partition:{}", p, allRssPartition);
        }
      }

      numWithDataInput.incrementAndGet();
      LOG.info("numWithDataInput:{}.", numWithDataInput.get());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void addCompletedInputWithNoData(InputAttemptIdentifier srcAttemptIdentifier) {
    super.addCompletedInputWithNoData(srcAttemptIdentifier);
    numNoDataInput.incrementAndGet();
    LOG.info(
        "AddCompletedInputWithNoData, numNoDataInput:{}, numWithDataInput:{},numInputs:{},  "
            + "successRssPartitionSet:{}, allRssPartition:{}.",
        numNoDataInput,
        numWithDataInput,
        numInputs,
        successRssPartitionSet,
        allRssPartition);
  }

  public void shutdown() throws InterruptedException {

    if (!isShutdown.get()) {
      super.shutdown();
      lock.lock();
      try {
        wakeLoop.signal(); // signal the fetch-scheduler
        for (CelebornFetcher fetcher : celebornRunningFetchers) {
          try {
            fetcher.shutdown(); // This could be parallelized.
          } catch (Exception e) {
            LOG.warn(
                "Error while stopping fetcher during shutdown. Ignoring and continuing. Message={}",
                e.getMessage());
          }
        }
      } finally {
        lock.unlock();
      }
    }
  }

  public boolean isAllPartitionFetched() {
    lock.lock();
    try {
      if (!allRssPartition.containsAll(successRssPartitionSet)) {
        LOG.error(
            "Failed to check partition, all partition:{}, success partiton:{}",
            allRssPartition,
            successRssPartitionSet);
      }
      return isAllInputFetched();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return the next available input, or null if there are no available inputs. This method will
   *     block if there are currently no available inputs, but more may become available.
   */
  public FetchedInput getNextInput() throws InterruptedException {
    // Check for no additional inputs
    FetchedInput fetchedInput = null;
    if (completedInputs.peek() == null) {
      while (true) {
        fetchedInput = completedInputs.poll(2000, TimeUnit.MICROSECONDS);
        if (fetchedInput != null) {
          break;
        } else if (isAllPartitionFetched()) {
          fetchedInput = completedInputs.poll(100, TimeUnit.MICROSECONDS);
          LOG.info("GetNextInput, enter isAllPartitionFetched");
          break;
        }
        LOG.info("GetNextInput, out loop");
        Thread.sleep(1000);
      }
    } else {
      fetchedInput = completedInputs.take();
    }

    if (fetchedInput instanceof NullFetchedInput) {
      LOG.info("getNextInput, NullFetchedInput is null:{}", fetchedInput);
      fetchedInput = null;
    }
    LOG.info("getNextInput, fetchedInput:{}", fetchedInput);
    return fetchedInput;
  }

  private class SchedulerFutureCallback implements FutureCallback<Void> {

    @Override
    public void onSuccess(Void result) {
      LOG.info(sourceDestNameTrimmed + ": " + "Scheduler thread completed");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutdown.get()) {
        LOG.debug("{}: Already shutdown. Ignoring error.", sourceDestNameTrimmed, t);
      } else {
        LOG.error(sourceDestNameTrimmed + ": " + "Scheduler failed with error: ", t);
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Scheduler Failed");
      }
    }
  }

  private class FetchFutureCallback implements FutureCallback<FetchResult> {

    private final CelebornFetcher fetcher;

    public FetchFutureCallback(CelebornFetcher fetcher) {
      this.fetcher = fetcher;
    }

    private void doBookKeepingForFetcherComplete() {
      lock.lock();
      try {
        celebornRunningFetchers.remove(fetcher);
        wakeLoop.signal();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void onSuccess(FetchResult result) {
      fetcher.shutdown();
      if (isShutdown.get()) {
        LOG.debug("{}: Already shutdown. Ignoring event from fetcher", sourceDestNameTrimmed);
      } else {
        lock.lock();
        try {
          successRssPartitionSet.add(fetcher.getPartitionId());
          runningRssPartitionMap.remove(fetcher.getPartitionId());
          LOG.info(
              "FetchFutureCallback allRssPartition:{}, successRssPartitionSet:{}, runningRssPartitionMap:{}.",
              allRssPartition,
              successRssPartitionSet,
              runningRssPartitionMap);
          doBookKeepingForFetcherComplete();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void onFailure(Throwable t) {
      // Unsuccessful - the fetcher may not have shutdown correctly. Try shutting it down.
      fetcher.shutdown();
      if (isShutdown.get()) {
        LOG.debug("{}: Already shutdown. Ignoring error from fetcher.", sourceDestNameTrimmed, t);
      } else {
        LOG.error(sourceDestNameTrimmed + ": " + "Fetcher failed with error: ", t);
        shuffleError = t;
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Fetch failed");
        doBookKeepingForFetcherComplete();
      }
    }
  }
}
