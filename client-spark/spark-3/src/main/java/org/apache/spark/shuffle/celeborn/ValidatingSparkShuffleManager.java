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

import org.apache.celeborn.client.read.CelebornIntegrityCheckTracker;

/**
 * A {@link ShuffleManager} that wraps {@link SparkShuffleManager} and enforces data integrity
 * checks on every shuffle read.
 *
 * <p>Set {@code spark.shuffle.manager=org.apache.spark.shuffle.celeborn.ValidatingSparkShuffleManager}
 * and register {@link CelebornIntegrityCheckExecutorPlugin} via {@code spark.plugins} to enable
 * end-to-end enforcement that every {@link org.apache.celeborn.client.read.CelebornInputStream}
 * performs its server-side integrity RPC before the task completes.
 *
 * <p>Additional checks performed:
 * <ul>
 *   <li>{@link #registerShuffle} logs a warning when an empty shuffle (zero mappers) is registered.
 *   <li>{@link #getWriter} asserts the integrity check tracker is initialised (i.e. the executor
 *       plugin is active) and logs a warning for empty shuffles.
 *   <li>{@link #getReader} returns an empty iterator for Celeborn handles with zero mappers,
 *       avoiding unnecessary RPCs for empty partitions.
 * </ul>
 */
public class ValidatingSparkShuffleManager implements ShuffleManager {
  private static final Logger logger =
      LoggerFactory.getLogger(ValidatingSparkShuffleManager.class);

  private final SparkShuffleManager sparkShuffleManager;

  public ValidatingSparkShuffleManager(SparkConf conf, boolean isDriver) {
    this.sparkShuffleManager = new SparkShuffleManager(conf, isDriver);
  }

  private boolean emptyShuffleReader(ShuffleHandle handle) {
    if (handle instanceof CelebornShuffleHandle) {
      CelebornShuffleHandle<?, ?, ?> celebornHandle = (CelebornShuffleHandle<?, ?, ?>) handle;
      if (celebornHandle.numMappers() == 0) {
        logger.error("Reading empty shuffle {}", celebornHandle.shuffleId());
        return true;
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
      return sparkShuffleManager.registerShuffle(shuffleId, dependency);
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      CelebornShuffleHandle<?, ?, ?> celebornHandle = (CelebornShuffleHandle<?, ?, ?>) handle;
      if (celebornHandle.numMappers() == 0) {
        logger.error("Writing empty shuffle {}", celebornHandle.shuffleId());
      }
      Preconditions.checkArgument(
          celebornHandle.numMappers() > 0,
          "Expected non-empty shuffle for shuffleId %s",
          celebornHandle.shuffleId());
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
  public SparkShuffleManager unwrap() {
    return sparkShuffleManager;
  }

  private static class TimeLogger implements AutoCloseable {
    private final String operation;
    private final Stopwatch sw;

    TimeLogger(String operation) {
      this.operation = operation;
      this.sw = Stopwatch.createStarted();
    }

    @Override
    public void close() {
      logger.info(
          "Shuffle manager operation {} took {} ms",
          operation,
          sw.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  static class ValidatingShuffleReader<K, C> implements ShuffleReader<K, C> {
    private final ShuffleReader<K, C> reader;
    private final int shuffleId;
    private final int startMapIndex;
    private final int endMapIndex;
    private final int startPartition;
    private final int endPartition;

    ValidatingShuffleReader(
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
          reader.read(), shuffleId, startMapIndex, endMapIndex, startPartition, endPartition);
    }
  }
}
