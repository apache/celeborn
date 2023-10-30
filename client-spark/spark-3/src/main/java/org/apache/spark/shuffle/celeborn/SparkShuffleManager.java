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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.*;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.ShuffleMode;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.reflect.DynMethods;

public class SparkShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(SparkShuffleManager.class);

  private static final String SORT_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.SortShuffleManager";

  private final SparkConf conf;
  private final Boolean isDriver;
  private final CelebornConf celebornConf;
  private final int cores;
  // either be "{appId}_{appAttemptId}" or "{appId}"
  private String appUniqueId;

  private LifecycleManager lifecycleManager;
  private ShuffleClient shuffleClient;
  private volatile SortShuffleManager _sortShuffleManager;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> sortShuffleIds =
      ConcurrentHashMap.newKeySet();
  private final CelebornShuffleFallbackPolicyRunner fallbackPolicyRunner;

  private final ExecutorService[] asyncPushers;
  private AtomicInteger pusherIdx = new AtomicInteger(0);

  private long sendBufferPoolCheckInterval;
  private long sendBufferPoolExpireTimeout;

  public SparkShuffleManager(SparkConf conf, boolean isDriver) {
    if (conf.getBoolean(SQLConf.LOCAL_SHUFFLE_READER_ENABLED().key(), true)) {
      logger.warn(
          "Detected {} (default is true) is enabled, it's highly recommended to disable it when "
              + "use Celeborn as Remote Shuffle Service to avoid performance degradation.",
          SQLConf.LOCAL_SHUFFLE_READER_ENABLED().key());
    }
    this.conf = conf;
    this.isDriver = isDriver;
    this.celebornConf = SparkUtils.fromSparkConf(conf);
    this.cores = executorCores(conf);
    this.fallbackPolicyRunner = new CelebornShuffleFallbackPolicyRunner(celebornConf);
    if (ShuffleMode.SORT.equals(celebornConf.shuffleWriterMode())
        && celebornConf.clientPushSortPipelineEnabled()) {
      asyncPushers = new ExecutorService[cores];
      for (int i = 0; i < asyncPushers.length; i++) {
        asyncPushers[i] = ThreadUtils.newDaemonSingleThreadExecutor("async-pusher-" + i);
      }
    } else {
      asyncPushers = null;
    }
    this.sendBufferPoolCheckInterval = celebornConf.clientPushSendBufferPoolExpireCheckInterval();
    this.sendBufferPoolExpireTimeout = celebornConf.clientPushSendBufferPoolExpireTimeout();
  }

  private SortShuffleManager sortShuffleManager() {
    if (_sortShuffleManager == null) {
      synchronized (this) {
        if (_sortShuffleManager == null) {
          _sortShuffleManager =
              SparkUtils.instantiateClass(SORT_SHUFFLE_MANAGER_NAME, conf, isDriver);
        }
      }
    }
    return _sortShuffleManager;
  }

  private void initializeLifecycleManager() {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf);
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // Note: generate app unique id at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    appUniqueId = SparkUtils.appUniqueId(dependency.rdd().context());
    initializeLifecycleManager();

    if (fallbackPolicyRunner.applyAllFallbackPolicy(
        lifecycleManager, dependency.partitioner().numPartitions())) {
      if (conf.getBoolean("spark.dynamicAllocation.enabled", false)
          && !conf.getBoolean("spark.shuffle.service.enabled", false)) {
        logger.error(
            "DRA is enabled but we fallback to vanilla Spark SortShuffleManager for "
                + "shuffle: {} due to fallback policy. It may cause block can not found when reducer "
                + "task fetch data.",
            shuffleId);
      } else {
        logger.warn("Fallback to vanilla Spark SortShuffleManager for shuffle: {}", shuffleId);
      }
      sortShuffleIds.add(shuffleId);
      return sortShuffleManager().registerShuffle(shuffleId, dependency);
    } else {
      return new CelebornShuffleHandle<>(
          appUniqueId,
          lifecycleManager.getHost(),
          lifecycleManager.getPort(),
          lifecycleManager.getUserIdentifier(),
          shuffleId,
          dependency.rdd().getNumPartitions(),
          dependency);
    }
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    if (sortShuffleIds.contains(shuffleId)) {
      return sortShuffleManager().unregisterShuffle(shuffleId);
    }
    // For Spark driver side trigger unregister shuffle.
    if (lifecycleManager != null) {
      lifecycleManager.unregisterShuffle(shuffleId);
    }
    // For Spark executor side cleanup shuffle related info.
    if (shuffleClient != null) {
      shuffleClient.cleanupShuffle(shuffleId);
    }
    return true;
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return sortShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (shuffleClient != null) {
      shuffleClient.shutdown();
      ShuffleClient.reset();
      shuffleClient = null;
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
      lifecycleManager = null;
    }
    if (_sortShuffleManager != null) {
      _sortShuffleManager.stop();
      _sortShuffleManager = null;
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    try {
      if (handle instanceof CelebornShuffleHandle) {
        @SuppressWarnings("unchecked")
        CelebornShuffleHandle<K, V, ?> h = ((CelebornShuffleHandle<K, V, ?>) handle);
        shuffleClient =
            ShuffleClient.get(
                h.appUniqueId(),
                h.lifecycleManagerHost(),
                h.lifecycleManagerPort(),
                celebornConf,
                h.userIdentifier());
        if (ShuffleMode.SORT.equals(celebornConf.shuffleWriterMode())) {
          ExecutorService pushThread =
              celebornConf.clientPushSortPipelineEnabled() ? getPusherThread() : null;
          return new SortBasedShuffleWriter<>(
              h.dependency(),
              h.numMappers(),
              context,
              celebornConf,
              shuffleClient,
              metrics,
              pushThread,
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout));
        } else if (ShuffleMode.HASH.equals(celebornConf.shuffleWriterMode())) {
          return new HashBasedShuffleWriter<>(
              h,
              context,
              celebornConf,
              shuffleClient,
              metrics,
              SendBufferPool.get(cores, sendBufferPoolCheckInterval, sendBufferPoolExpireTimeout));
        } else {
          throw new UnsupportedOperationException(
              "Unrecognized shuffle write mode!" + celebornConf.shuffleWriterMode());
        }
      } else {
        sortShuffleIds.add(handle.shuffleId());
        return sortShuffleManager().getWriter(handle, mapId, context, metrics);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Added in SPARK-32055, for Spark 3.1 and above
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      return new CelebornShuffleReader<>(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          celebornConf,
          metrics);
    }
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics);
  }

  // Marked as final in SPARK-32055, reserved for Spark 3.0
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      return new CelebornShuffleReader<>(
          h, startPartition, endPartition, 0, Integer.MAX_VALUE, context, celebornConf, metrics);
    }
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        0,
        Integer.MAX_VALUE,
        startPartition,
        endPartition,
        context,
        metrics);
  }

  // Renamed to getReader in SPARK-32055, reserved for Spark 3.0
  public <K, C> ShuffleReader<K, C> getReaderForRange(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      return new CelebornShuffleReader<>(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          celebornConf,
          metrics);
    }
    return SparkUtils.getReader(
        sortShuffleManager(),
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics);
  }

  private ExecutorService getPusherThread() {
    ExecutorService pusherThread = asyncPushers[pusherIdx.get() % asyncPushers.length];
    pusherIdx.incrementAndGet();
    return pusherThread;
  }

  private int executorCores(SparkConf conf) {
    if (Utils.isLocalMaster(conf)) {
      // SparkContext.numDriverCores is package private.
      return DynMethods.builder("numDriverCores")
          .impl("org.apache.spark.SparkContext$", String.class)
          .build()
          .bind(SparkContext$.MODULE$)
          .invoke(conf.get("spark.master"));
    } else {
      return conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
    }
  }

  // for testing
  public LifecycleManager getLifecycleManager() {
    return this.lifecycleManager;
  }
}
