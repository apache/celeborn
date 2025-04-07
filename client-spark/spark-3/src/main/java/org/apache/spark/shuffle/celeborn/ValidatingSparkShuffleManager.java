package org.apache.spark.shuffle.celeborn;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import scala.Product2;
import scala.collection.Iterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker;

public class ValidatingSparkShuffleManager implements ShuffleManager {
  private static final Logger logger = LoggerFactory.getLogger(ValidatingSparkShuffleManager.class);
  private final SparkShuffleManager sparkShuffleManager;
  private final boolean handleEmptyPartitions;

  public ValidatingSparkShuffleManager(SparkConf conf, boolean isDriver) {
    this.sparkShuffleManager = new SparkShuffleManager(conf, isDriver);
    this.handleEmptyPartitions = conf.getBoolean("celeborn.stripe.handle.empty.partitions", true);
  }

  private boolean emptyShuffleReader(ShuffleHandle handle) {
    if (handle instanceof CelebornShuffleHandle) {
      var celebornHandle = (CelebornShuffleHandle) handle;
      if (celebornHandle.numMappers() == 0) {
        logger.error("Reading empty shuffle {}", celebornHandle.shuffleId());
        return handleEmptyPartitions;
      }
    }

    return false;
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    if (dependency.rdd().partitions().length == 0) {
      logger.error("Registering empty shuffle {}", shuffleId);
    }

    try (var ignored = new TimeLogger("registerShuffle")) {
      return this.sparkShuffleManager.registerShuffle(shuffleId, dependency);
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      var celebornHandle = (CelebornShuffleHandle) handle;
      if (celebornHandle.numMappers() == 0) {
        logger.error("Writing empty shuffle {}", celebornHandle.shuffleId());
      }
      Preconditions.checkArgument(!this.handleEmptyPartitions || celebornHandle.numMappers() > 0);
    }

    try (var ignored = new TimeLogger("getWriter")) {
      CelebornIntegrityCheckTracker.checkEnabled();
      return sparkShuffleManager.getWriter(handle, mapId, context, metrics);
    }
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (emptyShuffleReader(handle)) {
      return ValidatingIterator::empty;
    }
    try (var ignored = new TimeLogger("getReader")) {
      return new ValidatingShuffleReader<>(
          sparkShuffleManager.getReader(handle, startPartition, endPartition, context, metrics),
          handle.shuffleId(),
          0,
          Integer.MAX_VALUE,
          startPartition,
          endPartition);
    }
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (emptyShuffleReader(handle)) {
      return ValidatingIterator::empty;
    }
    try (var ignored = new TimeLogger("getReader")) {
      return new ValidatingShuffleReader<>(
          sparkShuffleManager.getReader(
              handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics),
          handle.shuffleId(),
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition);
    }
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    try (var ignored = new TimeLogger("unregisterShuffle")) {
      return sparkShuffleManager.unregisterShuffle(shuffleId);
    }
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    try (var ignored = new TimeLogger("shuffleBlockResolver")) {
      return sparkShuffleManager.shuffleBlockResolver();
    }
  }

  @Override
  public void stop() {
    try (var ignored = new TimeLogger("stop")) {
      sparkShuffleManager.stop();
    }
  }

  @VisibleForTesting
  public LifecycleManager getLifecycleManager() {
    try (var ignored = new TimeLogger("getLifecycleManager")) {
      return sparkShuffleManager.getLifecycleManager();
    }
  }

  private static class TimeLogger implements AutoCloseable {
    private final String operation;
    private final Stopwatch sw;

    public TimeLogger(String operation) {
      this.operation = operation;
      this.sw = Stopwatch.createStarted();
    }

    @Override
    public void close() {
      logger.info(
          "Shuffle manager operation {} took {} ms", operation, sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  static class ValidatingShuffleReader<K, C> implements ShuffleReader<K, C> {
    private final ShuffleReader<K, C> reader;
    private final int shuffleId;
    private final int startMapIndex;
    private final int endMapIndex;
    private final int startPartition;
    private final int endPartition;

    public ValidatingShuffleReader(
        ShuffleReader<K, C> reader,
        int shuffleId,
        int startMapIndex,
        int endMapIndex,
        int startPartition,
        int endPartition) {
      this.reader = checkNotNull(reader, "reader");
      this.shuffleId = shuffleId;
      this.startMapIndex = startMapIndex;
      this.endMapIndex = endMapIndex;
      this.startPartition = startPartition;
      this.endPartition = endPartition;
    }

    @Override
    public Iterator<Product2<K, C>> read() {
      return new ValidatingIterator<>(
          this.reader.read(), shuffleId, startMapIndex, endMapIndex, startPartition, endPartition);
    }
  }
}
