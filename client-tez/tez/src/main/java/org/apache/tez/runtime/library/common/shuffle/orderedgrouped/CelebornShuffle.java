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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.*;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.GuavaShim;
import org.apache.tez.common.Preconditions;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.TezRuntimeUtils;
import org.apache.tez.runtime.library.common.combine.Combiner;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.apache.tez.runtime.library.exceptions.InputAlreadyClosedException;
import org.apache.tez.runtime.library.utils.CodecUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

public class CelebornShuffle implements ExceptionReporter {
  private static final Logger LOG = LoggerFactory.getLogger(Shuffle.class);

  private final Configuration conf;
  private final InputContext inputContext;

  private final ShuffleInputEventHandlerOrderedGrouped eventHandler;

  final CelebornScheduler scheduler;

  @VisibleForTesting final MergeManager merger;

  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private AtomicReference<Throwable> throwable = new AtomicReference<Throwable>();
  private String throwingThreadName = null;

  private final RunShuffleCallable runShuffleCallable;
  private volatile ListenableFuture<TezRawKeyValueIterator> runShuffleFuture;
  private final ListeningExecutorService executor;

  private final String sourceDestNameTrimmed;

  private AtomicBoolean isShutDown = new AtomicBoolean(false);
  private AtomicBoolean fetchersClosed = new AtomicBoolean(false);
  private AtomicBoolean schedulerClosed = new AtomicBoolean(false);
  private AtomicBoolean mergerClosed = new AtomicBoolean(false);

  private final TezCounter mergePhaseTime;
  private final TezCounter shufflePhaseTime;

  private final long startTime;
  private Configuration remoteConf;

  public CelebornShuffle(
      InputContext inputContext,
      Configuration conf,
      int numInputs,
      long initialMemoryAvailable,
      ApplicationAttemptId applicationAttemptId)
      throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;

    this.sourceDestNameTrimmed =
        TezUtilsInternal.cleanVertexName(inputContext.getSourceVertexName())
            + " -> "
            + TezUtilsInternal.cleanVertexName(inputContext.getTaskVertexName());

    this.codec = CodecUtils.getCodec(conf);

    this.ifileReadAhead =
        conf.getBoolean(
            TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD,
            TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
    if (this.ifileReadAhead) {
      this.ifileReadAheadLength =
          conf.getInt(
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
              TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
    } else {
      this.ifileReadAheadLength = 0;
    }

    Combiner combiner = TezRuntimeUtils.instantiateCombiner(conf, inputContext);

    FileSystem localFS = FileSystem.getLocal(this.conf);
    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    // TODO TEZ Get rid of Map / Reduce references.
    TezCounter spilledRecordsCounter =
        inputContext.getCounters().findCounter(TaskCounter.SPILLED_RECORDS);
    TezCounter reduceCombineInputCounter =
        inputContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    TezCounter mergedMapOutputsCounter =
        inputContext.getCounters().findCounter(TaskCounter.MERGED_MAP_OUTPUTS);

    startTime = System.currentTimeMillis();
    merger =
        new MergeManager(
            this.conf,
            localFS,
            localDirAllocator,
            inputContext,
            combiner,
            spilledRecordsCounter,
            reduceCombineInputCounter,
            mergedMapOutputsCounter,
            this,
            initialMemoryAvailable,
            codec,
            ifileReadAhead,
            ifileReadAheadLength);

    String host = conf.get(TEZ_CELEBORN_LM_HOST);
    int port = conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    int shuffleId = conf.getInt(TEZ_SHUFFLE_ID, -1);
    String appId = conf.get(TEZ_CELEBORN_APPLICATION_ID);
    String user = conf.get(TEZ_CELEBORN_USER);
    CelebornConf celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
    ShuffleClient shuffleClient =
        ShuffleClient.get(appId, host, port, celebornConf, UserIdentifier.apply(user), null);

    scheduler =
        new CelebornScheduler(
            this.inputContext,
            this.conf,
            numInputs,
            this,
            shuffleClient,
            merger,
            merger,
            startTime,
            codec,
            ifileReadAhead,
            ifileReadAheadLength,
            sourceDestNameTrimmed,
            shuffleId,
            applicationAttemptId);

    this.mergePhaseTime = inputContext.getCounters().findCounter(TaskCounter.MERGE_PHASE_TIME);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

    eventHandler =
        new ShuffleInputEventHandlerOrderedGrouped(
            inputContext, scheduler, ShuffleUtils.isTezShuffleHandler(conf));

    ExecutorService rawExecutor =
        Executors.newFixedThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ShuffleAndMergeRunner {" + sourceDestNameTrimmed + "}")
                .build());

    executor = MoreExecutors.listeningDecorator(rawExecutor);
    runShuffleCallable = new RunShuffleCallable();
  }

  public void run() throws IOException {
    merger.configureAndStart();
    runShuffleFuture = executor.submit(runShuffleCallable);
    Futures.addCallback(
        runShuffleFuture, new ShuffleRunnerFutureCallback(), GuavaShim.directExecutor());
    executor.shutdown();
  }

  public void handleEvents(List<Event> events) throws IOException {
    if (!isShutDown.get()) {
      eventHandler.handleEvents(events);
    } else {
      LOG.info(
          sourceDestNameTrimmed
              + ": "
              + "Ignoring events since already shutdown. EventCount: "
              + events.size());
    }
  }

  public boolean isInputReady() throws IOException, InterruptedException, TezException {
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
    }
    if (runShuffleFuture == null) {
      return false;
    }
    // Don't need to check merge status, since runShuffleFuture will only
    // complete once merge is complete.
    return runShuffleFuture.isDone();
  }

  public void shutdown() {
    if (!isShutDown.getAndSet(true)) {
      // Interrupt so that the scheduler / merger sees this interrupt.
      LOG.info("Shutting down Shuffle for source: " + sourceDestNameTrimmed);
      runShuffleFuture.cancel(true);
      cleanupIgnoreErrors();
    }
  }

  @Override
  public synchronized void reportException(Throwable t) {
    // RunShuffleCallable onFailure deals with ignoring errors on shutdown.
    if (throwable.get() == null) {
      LOG.info(
          sourceDestNameTrimmed
              + ": "
              + "Setting throwable in reportException with message ["
              + t.getMessage()
              + "] from thread ["
              + Thread.currentThread().getName());
      throwable.set(t);
      throwingThreadName = Thread.currentThread().getName();
      // Notify the scheduler so that the reporting thread finds the
      // exception immediately.
      cleanupShuffleSchedulerIgnoreErrors();
    }
  }

  private void cleanupShuffleSchedulerIgnoreErrors() {
    try {
      cleanupShuffleScheduler();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info(
          sourceDestNameTrimmed
              + ": "
              + "Interrupted while attempting to close the scheduler during cleanup. Ignoring");
    }
  }

  private void cleanupShuffleScheduler() throws InterruptedException {
    if (!schedulerClosed.getAndSet(true)) {
      scheduler.close();
    }
  }

  @Override
  public void killSelf(Exception e, String s) {}

  public TezRawKeyValueIterator waitForInput()
      throws IOException, InterruptedException, TezException {
    Preconditions.checkState(runShuffleFuture != null, "waitForInput can only be called after run");
    TezRawKeyValueIterator kvIter = null;
    try {
      kvIter = runShuffleFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // Processor interrupted while waiting for errors, will see an InterruptedException.
      handleThrowable(cause);
    }
    if (isShutDown.get()) {
      throw new InputAlreadyClosedException();
    }
    if (throwable.get() != null) {
      handleThrowable(throwable.get());
    }
    return kvIter;
  }

  private void handleThrowable(Throwable t) throws IOException, InterruptedException {
    if (t instanceof IOException) {
      throw (IOException) t;
    } else if (t instanceof InterruptedException) {
      throw (InterruptedException) t;
    } else {
      throw new UndeclaredThrowableException(t);
    }
  }

  private class RunShuffleCallable extends CallableWithNdc<TezRawKeyValueIterator> {
    @Override
    protected TezRawKeyValueIterator callInternal() throws IOException, InterruptedException {
      if (!isShutDown.get()) {
        try {
          scheduler.start();
        } catch (Throwable e) {
          throw new Shuffle.ShuffleError("Error during shuffle", e);
        } finally {
        }
      }
      // The ShuffleScheduler may have exited cleanly as a result of a shutdown invocation
      // triggered by a previously reportedException. Check before proceeding further.s
      synchronized (CelebornShuffle.this) {
        if (throwable.get() != null) {
          throw new Shuffle.ShuffleError(
              "error in shuffle in " + throwingThreadName, throwable.get());
        }
      }

      // Finish the on-going merges...
      TezRawKeyValueIterator kvIter = null;
      inputContext.notifyProgress();
      try {
        kvIter = merger.close(true);
      } catch (Throwable e) {
        // Set the throwable so that future.get() sees the reported errror.
        throwable.set(e);
        throw new Shuffle.ShuffleError("Error while doing final merge ", e);
      }
      mergePhaseTime.setValue(System.currentTimeMillis() - startTime);

      inputContext.notifyProgress();
      // Sanity check
      synchronized (CelebornShuffle.this) {
        if (throwable.get() != null) {
          throw new Shuffle.ShuffleError(
              "error in shuffle in " + throwingThreadName, throwable.get());
        }
      }

      inputContext.inputIsReady();
      LOG.info("merge complete for input vertex : " + sourceDestNameTrimmed);
      return kvIter;
    }
  }

  private class ShuffleRunnerFutureCallback implements FutureCallback<TezRawKeyValueIterator> {
    @Override
    public void onSuccess(TezRawKeyValueIterator result) {
      LOG.info(sourceDestNameTrimmed + ": " + "Shuffle Runner thread complete");
    }

    @Override
    public void onFailure(Throwable t) {
      if (isShutDown.get()) {
        LOG.info(sourceDestNameTrimmed + ": " + "Already shutdown. Ignoring error");
      } else {
        LOG.error(sourceDestNameTrimmed + ": " + "ShuffleRunner failed with error", t);
        // In case of an abort / Interrupt - the runtime makes sure that this is ignored.
        inputContext.reportFailure(TaskFailureType.NON_FATAL, t, "Shuffle Runner Failed");
        cleanupIgnoreErrors();
      }
    }
  }

  private void cleanupIgnoreErrors() {
    try {
      if (eventHandler != null) {
        eventHandler.logProgress(true);
      }
      try {
        cleanupShuffleSchedulerIgnoreErrors();
      } catch (Exception e) {
        LOG.warn(
            "Error cleaning up shuffle scheduler. Ignoring and continuing with shutdown. Message={}",
            e.getMessage());
      }
      cleanupMerger(true);
    } catch (Throwable t) {
      LOG.info(sourceDestNameTrimmed + ": " + "Error in cleaning up.., ", t);
    }
  }

  private void cleanupMerger(boolean ignoreErrors) throws Throwable {
    if (!mergerClosed.getAndSet(true)) {
      try {
        merger.close(false);
      } catch (InterruptedException e) {
        if (ignoreErrors) {
          // Reset the status
          Thread.currentThread().interrupt();
          LOG.info(
              sourceDestNameTrimmed
                  + ": "
                  + "Interrupted while attempting to close the merger during cleanup. Ignoring");
        } else {
          throw e;
        }
      } catch (Throwable e) {
        if (ignoreErrors) {
          LOG.info(
              sourceDestNameTrimmed + ": " + "Exception while trying to shutdown merger, Ignoring",
              e);
        } else {
          throw e;
        }
      }
    }
  }
}
