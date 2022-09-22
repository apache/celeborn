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

package org.apache.spark.shuffle.rss;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.*;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.emr.rss.client.ShuffleClient;
import com.aliyun.emr.rss.client.write.LifecycleManager;
import com.aliyun.emr.rss.common.RssConf;
import scala.Int;

public class RssShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(RssShuffleManager.class);

  private static final String sortShuffleManagerName =
      "org.apache.spark.shuffle.sort.SortShuffleManager";

  private final SparkConf conf;
  private final RssConf rssConf;
  private final int cores;
  private String newAppId;

  private LifecycleManager lifecycleManager;
  private ShuffleClient rssShuffleClient;
  private volatile SortShuffleManager _sortShuffleManager;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> sortShuffleIds =
      ConcurrentHashMap.newKeySet();
  private final RssShuffleFallbackPolicyRunner fallbackPolicyRunner;

  public RssShuffleManager(SparkConf conf) {
    this.conf = conf;
    this.rssConf = SparkUtils.fromSparkConf(conf);
    this.cores = conf.getInt(SparkLauncher.EXECUTOR_CORES, 1);
    this.fallbackPolicyRunner = new RssShuffleFallbackPolicyRunner(conf);
  }

  private boolean isDriver() {
    return "driver".equals(SparkEnv.get().executorId());
  }

  private SortShuffleManager sortShuffleManager() {
    if (_sortShuffleManager == null) {
      synchronized (this) {
        if (_sortShuffleManager == null) {
          _sortShuffleManager =
              SparkUtils.instantiateClass(sortShuffleManagerName, conf, isDriver());
        }
      }
    }
    return _sortShuffleManager;
  }

  private void initializeLifecycleManager(String appId) {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver() && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          lifecycleManager = new LifecycleManager(appId, rssConf);
          rssShuffleClient =
              ShuffleClient.get(
                  lifecycleManager.self(), rssConf, lifecycleManager.getUserIdentifier());
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    newAppId = SparkUtils.genNewAppId(dependency.rdd().context());
    initializeLifecycleManager(newAppId);

    if (fallbackPolicyRunner.applyAllFallbackPolicy(
        lifecycleManager, dependency.partitioner().numPartitions())) {
      logger.warn("Fallback to SortShuffleManager!");
      sortShuffleIds.add(shuffleId);
      return sortShuffleManager().registerShuffle(shuffleId, numMaps, dependency);
    } else {
      return new RssShuffleHandle<>(
          newAppId,
          lifecycleManager.getRssMetaServiceHost(),
          lifecycleManager.getRssMetaServicePort(),
          lifecycleManager.getUserIdentifier(),
          shuffleId,
          numMaps,
          dependency);
    }
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    if (sortShuffleIds.contains(shuffleId)) {
      return sortShuffleManager().unregisterShuffle(shuffleId);
    }
    if (newAppId == null) {
      return true;
    }
    if (rssShuffleClient == null) {
      return false;
    }
    return rssShuffleClient.unregisterShuffle(newAppId, shuffleId, isDriver());
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return sortShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (rssShuffleClient != null) {
      rssShuffleClient.shutDown();
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
    }
    if (sortShuffleManager() != null) {
      sortShuffleManager().stop();
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, int mapId, TaskContext context) {
    try {
      if (handle instanceof RssShuffleHandle) {
        @SuppressWarnings("unchecked")
        RssShuffleHandle<K, V, ?> h = ((RssShuffleHandle<K, V, ?>) handle);
        ShuffleClient client =
            ShuffleClient.get(
                h.rssMetaServiceHost(), h.rssMetaServicePort(), rssConf, h.userIdentifier());
        if ("sort".equals(RssConf.shuffleWriterMode(rssConf))) {
          return new SortBasedShuffleWriter<>(
              h.dependency(), h.newAppId(), h.numMaps(), context, rssConf, client);
        } else if ("hash".equals(RssConf.shuffleWriterMode(rssConf))) {
          return new HashBasedShuffleWriter<>(
              h, mapId, context, rssConf, client, SendBufferPool.get(cores));
        } else {
          throw new UnsupportedOperationException(
              "Unrecognized shuffle write mode!" + RssConf.shuffleWriterMode(rssConf));
        }
      } else {
        return sortShuffleManager().getWriter(handle, mapId, context);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      @SuppressWarnings("unchecked")
      RssShuffleHandle<K, ?, C> h = (RssShuffleHandle<K, ?, C>) handle;
      return new RssShuffleReader<>(
          h, startPartition, endPartition, 0, Int.MaxValue(), context, rssConf);
    }
    return _sortShuffleManager.getReader(handle, startPartition, endPartition, context);
  }

  // remove override for compatibility
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context) {
    if (handle instanceof RssShuffleHandle) {
      @SuppressWarnings("unchecked")
      RssShuffleHandle<K, ?, C> h = (RssShuffleHandle<K, ?, C>) handle;
      return new RssShuffleReader<>(
          h, startPartition, endPartition, startMapIndex, endMapIndex, context, rssConf);
    }
    return SparkUtils.invokeGetReaderMethod(
        sortShuffleManagerName,
        "getReader",
        sortShuffleManager(),
        handle,
        startPartition,
        endPartition,
        context);
  }
}
