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
import com.google.common.util.concurrent.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.tez.common.*;
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

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;

public class CelebornShuffle implements ExceptionReporter {
  private static final Logger LOG = LoggerFactory.getLogger(Shuffle.class);

  private final Configuration conf;
  private final InputContext inputContext;

  private final CelebornEventHandlerOrderedGrouped eventHandler;

  @VisibleForTesting final MergeManager merger;

  private final CompressionCodec codec;
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private String host;
  private int port;
  private int shuffleId;
  private String appId;
  private String user;
  private CelebornConf celebornConf;

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
      int numPhysicalInputs,
      long initialMemoryAvailable)
      throws IOException {
    this.inputContext = inputContext;
    this.conf = conf;

    this.host = this.conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = this.conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = this.conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = this.conf.get(TEZ_CELEBORN_APPLICATION_ID);
    this.user = this.conf.get(TEZ_CELEBORN_USER);
    this.celebornConf = CelebornTezUtils.fromTezConfiguration(conf);

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

    this.mergePhaseTime = inputContext.getCounters().findCounter(TaskCounter.MERGE_PHASE_TIME);
    this.shufflePhaseTime = inputContext.getCounters().findCounter(TaskCounter.SHUFFLE_PHASE_TIME);

    eventHandler =
        new CelebornEventHandlerOrderedGrouped(
            inputContext, ShuffleUtils.isTezShuffleHandler(conf));

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
  public void reportException(Throwable throwable) {}

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
      int partitionId = 0;
      if (!isShutDown.get()) {
        try {
          partitionId = eventHandler.getPartitionId();
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

      CelebornTezReader reader =
          new CelebornTezReader(
              appId,
              host,
              port,
              shuffleId,
              partitionId,
              0,
              inputContext.getCounters(),
              UserIdentifier.apply(user),
              merger,
              celebornConf);
      reader.fetchAndMerge();

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
