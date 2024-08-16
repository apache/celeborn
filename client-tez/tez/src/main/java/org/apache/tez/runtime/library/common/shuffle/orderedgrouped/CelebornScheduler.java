package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.apache.celeborn.tez.plugin.util.CelebornTezUtils.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.tez.plugin.util.CelebornTezUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CelebornScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(CelebornScheduler.class);

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final String srcNameTrimmed;

  private String host;
  private int port;
  private int shuffleId;
  private String appId;
  private String user;
  final MergeManager merger;
  Configuration conf;
  private final InputContext inputContext;
  private volatile Thread shuffleSchedulerThread = null;
  private volatile int partitionId = -1;
  private CelebornConf celebornConf;
  private final ExceptionReporter exceptionReporter;

  public CelebornScheduler(InputContext inputContext, Configuration conf, String srcNameTrimmed,
                           MergeManager merger, ExceptionReporter exceptionReporter) {
    this.inputContext = inputContext;
    this.merger = merger;
    this.conf = conf;
    this.host = conf.get(TEZ_CELEBORN_LM_HOST);
    this.port = conf.getInt(TEZ_CELEBORN_LM_PORT, -1);
    this.shuffleId = conf.getInt(TEZ_SHUFFLE_ID, -1);
    this.appId = conf.get(TEZ_CELEBORN_APPLICATION_ID);
    this.user = conf.get(TEZ_CELEBORN_USER);
    this.celebornConf = CelebornTezUtils.fromTezConfiguration(conf);
    this.exceptionReporter = exceptionReporter;
    this.srcNameTrimmed = srcNameTrimmed;
  }

  private final Set<Integer> allRssPartition = Sets.newConcurrentHashSet();
  private int partitionIndex = 0;
  private final Map<Integer, Set<InputAttemptIdentifier>> partitionIdToSuccessMapTaskAttempts =
      new HashMap<>();

  public synchronized void addKnownMapOutput(
      String inputHostName, int port, int partitionId, CompositeInputAttemptIdentifier srcAttempt) {

    allRssPartition.add(partitionId);
    this.partitionId = partitionId;
    if (!partitionIdToSuccessMapTaskAttempts.containsKey(partitionId)) {
      partitionIdToSuccessMapTaskAttempts.put(partitionId, new HashSet<>());
    }
    partitionIdToSuccessMapTaskAttempts.get(partitionId).add(srcAttempt);
  }

  public void start() throws Exception {
    shuffleSchedulerThread = Thread.currentThread();
    RssShuffleSchedulerCallable rssShuffleSchedulerCallable = new RssShuffleSchedulerCallable();
    rssShuffleSchedulerCallable.call();
  }

  public void close() {
    try {
      if (!isShutdown.getAndSet(true)) {
        try {
          logProgress();
        } catch (Exception e) {
          LOG.warn("Failed log progress while closing, ignoring and continuing shutdown. Message={}",
                  e.getMessage());
        }
        // Interrupt the ShuffleScheduler thread only if the close is invoked by another thread.
        // If this is invoked on the same thread, then the shuffleRunner has already complete, and there's
        // no point interrupting it.
        // The interrupt is needed to unblock any merges or waits which may be happening, so that the thread can
        // exit.
        if (shuffleSchedulerThread != null && !Thread.currentThread()
                .equals(shuffleSchedulerThread)) {
          shuffleSchedulerThread.interrupt();
        }
      }
    } finally {
      LOG.info("Shutting down fetchers for input: {}", srcNameTrimmed);
    }
  }

  public void logProgress() {
  }

  private class RssShuffleSchedulerCallable extends CallableWithNdc<Void> {

    @Override
    protected Void callInternal() throws IOException, InterruptedException, TezException {
      while (!isShutdown.get()) {
        if (partitionId == -1) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            if (isShutdown.get()) {
              LOG.info(
                      srcNameTrimmed
                              + ": "
                              + "Interrupted while waiting for fetchers to complete"
                              + "and hasBeenShutdown. Breaking out of ShuffleSchedulerCallable loop");
              Thread.currentThread().interrupt();
              break;
            } else {
              throw e;
            }
          }
        } else {
          break;
        }
      }
      try {
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
      } catch (Exception e) {
        exceptionReporter.reportException(e);
      }

      return null;
    }
  }
}
