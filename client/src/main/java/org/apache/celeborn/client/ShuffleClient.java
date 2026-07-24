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

package org.apache.celeborn.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

import scala.Tuple2;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.client.read.CelebornInputStream;
import org.apache.celeborn.client.read.MetricsCallback;
import org.apache.celeborn.client.security.CryptoHandler;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.network.client.TransportClientFactory;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.PbStreamHandler;
import org.apache.celeborn.common.protocol.StorageInfo;
import org.apache.celeborn.common.protocol.message.ControlMessages;
import org.apache.celeborn.common.rpc.RpcEndpointRef;
import org.apache.celeborn.common.util.CelebornHadoopUtils;
import org.apache.celeborn.common.util.ExceptionMaker;
import org.apache.celeborn.common.write.LocationPushFailedBatches;
import org.apache.celeborn.common.write.PushState;

/**
 * ShuffleClient hides the specific PartitionLocation from callers. A process holds one client per
 * appUniqueId (see {@link #get}); most deployments run a single application per JVM and therefore a
 * single client, but a process that drives several applications (e.g. the multi-app spark-it JVM)
 * holds one isolated client per app.
 */
public abstract class ShuffleClient {
  private static Logger logger = LoggerFactory.getLogger(ShuffleClient.class);
  // One client per appUniqueId. Keying by appUniqueId keeps concurrent applications isolated (each
  // with its own LifecycleManager) and lets each be torn down independently when its application
  // stops, instead of a single static slot that would have to evict (and orphan the resources of)
  // the previous app's client on every switch.
  private static final ConcurrentHashMap<String, ShuffleClient> clients = new ConcurrentHashMap<>();
  private static volatile Map<StorageInfo.Type, FileSystem> hadoopFs;
  private static LongAdder totalReadCounter = new LongAdder();
  private static LongAdder localShuffleReadCounter = new LongAdder();

  private static volatile Optional<
          BiFunction<Integer, byte[], ControlMessages.GetReducerFileGroupResponse>>
      deserializeReducerFileGroupResponseFunction = Optional.empty();

  // for testing
  public static void reset() {
    List<ShuffleClient> toShutdown;
    synchronized (ShuffleClient.class) {
      toShutdown = new ArrayList<>(clients.values());
      clients.clear();
      hadoopFs = null;
    }
    // Shut down outside the lock: shutdown() tears down an RpcEnv and pools and can block.
    for (ShuffleClient client : toShutdown) {
      try {
        client.shutdown();
      } catch (Throwable t) {
        logger.warn("Failed to shutdown shuffle client during reset.", t);
      }
    }
  }

  /**
   * Removes {@code client} from the registry and shuts it down exactly once, reclaiming its RpcEnv,
   * Netty data client factory, push-retry pool and reviveManager. An engine's shuffle-manager calls
   * this from its own stop() so a stopped application's client does not leak until JVM exit, and
   * (unlike {@link #reset()}) without touching other live applications' clients. Keyed by the
   * instance rather than appUniqueId because executor-side managers never populate their
   * appUniqueId field. No-op if the client was already removed (e.g. by a concurrent {@link
   * #reset()}), so the instance is never shut down twice.
   */
  public static void removeInstance(ShuffleClient client) {
    if (client == null) {
      return;
    }
    boolean removed;
    synchronized (ShuffleClient.class) {
      removed = clients.values().removeIf(existing -> existing == client);
    }
    if (removed) {
      client.shutdown();
    }
  }

  protected ShuffleClient() {}

  public static ShuffleClient get(
      String appUniqueId,
      String driverHost,
      int port,
      CelebornConf conf,
      UserIdentifier userIdentifier) {
    return ShuffleClient.get(appUniqueId, driverHost, port, conf, userIdentifier, null);
  }

  public static ShuffleClient get(
      String appUniqueId,
      String driverHost,
      int port,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      byte[] extension) {
    return ShuffleClient.get(
        appUniqueId, driverHost, port, conf, userIdentifier, extension, Optional.empty());
  }

  public static ShuffleClient get(
      String appUniqueId,
      String driverHost,
      int port,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      byte[] extension,
      Optional<CryptoHandler> cryptoHandler) {
    ShuffleClient client = clients.get(appUniqueId);
    if (client == null) {
      synchronized (ShuffleClient.class) {
        client = clients.get(appUniqueId);
        if (client == null) {
          // During Spark task execution a task may be interrupted (e.g. by speculative execution)
          // while this builds the client. Fully set the instance up before publishing it into the
          // registry, and tear a half-built instance down on failure, so a later call rebuilds
          // cleanly instead of returning a client with no LifecycleManagerRef (which would NPE on
          // first use). Because only fully-initialized instances are ever put into the map, the
          // lock-free clients.get() above can never observe a half-built or wrong-app client.
          ShuffleClientImpl instance = new ShuffleClientImpl(appUniqueId, conf, userIdentifier);
          try {
            instance.setupLifecycleManagerRef(driverHost, port);
            instance.setExtension(extension);
            instance.setupCryptoHandler(cryptoHandler);
          } catch (RuntimeException | Error e) {
            instance.shutdown();
            throw e;
          }
          clients.put(appUniqueId, instance);
          client = instance;
        }
      }
    }
    // Apply the crypto handler even when the client already exists. This handles the case where
    // SparkEnv was transiently unavailable during the first init call (causing an empty handler to
    // be stored), so that encryption is correctly applied on retry. setupCryptoHandler is a
    // volatile write and safe to call without the lock.
    if (cryptoHandler != null && cryptoHandler.isPresent()) {
      client.setupCryptoHandler(cryptoHandler);
    }
    return client;
  }

  public static Map<StorageInfo.Type, FileSystem> getHadoopFs(CelebornConf conf) {
    if (null == hadoopFs) {
      synchronized (ShuffleClient.class) {
        if (null == hadoopFs) {
          try {
            hadoopFs = CelebornHadoopUtils.getHadoopFS(conf);
          } catch (Exception e) {
            logger.error("Celeborn initialize DFS failed.", e);
            hadoopFs = Collections.emptyMap();
          }
        }
      }
    }
    return hadoopFs;
  }

  public static void incrementLocalReadCounter() {
    localShuffleReadCounter.increment();
    totalReadCounter.increment();
  }

  public static void incrementTotalReadCounter() {
    totalReadCounter.increment();
  }

  public static void printReadStats(Logger logger) {
    long totalReadCount = totalReadCounter.longValue();
    long localReadCount = localShuffleReadCounter.longValue();
    logger.info(
        "Current client read {}/{} (local/total) partitions, local read ratio {}",
        localReadCount,
        totalReadCount,
        String.format("%.2f", (localReadCount * 1.0d / totalReadCount) * 100));
  }

  public abstract void setupCryptoHandler(Optional<CryptoHandler> cryptoHandler);

  public abstract void setupLifecycleManagerRef(String host, int port);

  public abstract void setupLifecycleManagerRef(RpcEndpointRef endpointRef);

  /**
   * @param extension Extension for shuffle client, it's a byte array. Used in derived shuffle
   *     client implementation.
   */
  public abstract void setExtension(byte[] extension);

  /**
   * Write data to a specific reduce partition, computing and recording a CRC over the batch before
   * pushing. Prefer this over {@link #pushData} at all writer call sites.
   *
   * @param shuffleId the unique shuffle id of the application
   * @param mapId the map id of the shuffle
   * @param attemptId the attempt id of the map task, i.e. speculative task or task rerun for Apache
   *     Spark
   * @param partitionId the partition id the data belongs to
   * @param data byte array containing data to be pushed
   * @param offset start position of data to be pushed
   * @param length length of data to be pushed
   * @param numMappers the number map tasks in the shuffle
   * @param numPartitions the number of partitions in the shuffle
   * @return bytes pushed
   * @throws IOException
   */
  public int pushDataWithCRC(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    computeBatchCRC(shuffleId, mapId, attemptId, partitionId, data, offset, length);
    return pushData(
        shuffleId, mapId, attemptId, partitionId, data, offset, length, numMappers, numPartitions);
  }

  /**
   * Write data to a specific reduce partition.
   *
   * <p><b>Internal use only.</b> Callers outside the async push pipeline (i.e. {@link
   * org.apache.celeborn.client.write.DataPusher}) should use {@link #pushDataWithCRC} instead,
   * which additionally records a CRC over the batch for end-to-end integrity checking.
   *
   * @param shuffleId the unique shuffle id of the application
   * @param mapId the map id of the shuffle
   * @param attemptId the attempt id of the map task, i.e. speculative task or task rerun for Apache
   *     Spark
   * @param partitionId the partition id the data belongs to
   * @param data byte array containing data to be pushed
   * @param offset start position of data to be pushed
   * @param length length of data to be pushed
   * @param numMappers the number map tasks in the shuffle
   * @param numPartitions the number of partitions in the shuffle
   * @return bytes pushed
   * @throws IOException
   */
  public abstract int pushData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException;

  /**
   * Pre-compute CRC for a batch immediately after assembly in the writer, before the data enters
   * the async push pipeline. This is the sole CRC accumulation path when shuffle integrity check is
   * enabled.
   */
  public abstract void computeBatchCRC(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length);

  /**
   * Merge data into a specific reduce partition, computing and recording a CRC over the batch
   * before merging. Prefer this over {@link #mergeData} at all writer call sites.
   *
   * @param shuffleId the unique shuffle id of the application
   * @param mapId the map id of the shuffle
   * @param attemptId the attempt id of the map task, i.e. speculative task or task rerun for Apache
   *     Spark
   * @param partitionId the partition id the data belongs to
   * @param data byte array containing data to be merged
   * @param offset start position of data to be merged
   * @param length length of data to be merged
   * @param numMappers the number map tasks in the shuffle
   * @param numPartitions the number of partitions in the shuffle
   * @return bytes merged
   * @throws IOException
   */
  public int mergeDataWithCRC(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException {
    computeBatchCRC(shuffleId, mapId, attemptId, partitionId, data, offset, length);
    return mergeData(
        shuffleId, mapId, attemptId, partitionId, data, offset, length, numMappers, numPartitions);
  }

  /**
   * Merge data into a specific reduce partition.
   *
   * <p><b>Internal use only.</b> Callers outside the async push pipeline should use {@link
   * #mergeDataWithCRC} instead, which additionally records a CRC over the batch for end-to-end
   * integrity checking.
   *
   * @param shuffleId the unique shuffle id of the application
   * @param mapId the map id of the shuffle
   * @param attemptId the attempt id of the map task, i.e. speculative task or task rerun for Apache
   *     Spark
   * @param partitionId the partition id the data belongs to
   * @param data byte array containing data to be merged
   * @param offset start position of data to be merged
   * @param length length of data to be merged
   * @param numMappers the number map tasks in the shuffle
   * @param numPartitions the number of partitions in the shuffle
   * @return bytes merged
   * @throws IOException
   */
  public abstract int mergeData(
      int shuffleId,
      int mapId,
      int attemptId,
      int partitionId,
      byte[] data,
      int offset,
      int length,
      int numMappers,
      int numPartitions)
      throws IOException;

  public abstract void pushMergedData(int shuffleId, int mapId, int attemptId) throws IOException;

  // Report partition locations written by the completed map task of ReducePartition Shuffle Type.
  public abstract void mapperEnd(
      int shuffleId, int mapId, int attemptId, int numMappers, int numPartitions)
      throws IOException;

  public abstract void readReducerPartitionEnd(
      int shuffleId, int partitionId, int startMapIndex, int endMapIndex, int crc32, long bytes)
      throws IOException;

  // Report partition locations written by the completed map task of MapPartition Shuffle Type.
  public abstract void mapPartitionMapperEnd(
      int shuffleId, int mapId, int attemptId, int numMappers, int numPartitions, int partitionId)
      throws IOException;

  // Cleanup states of the map task
  public abstract void cleanup(int shuffleId, int mapId, int attemptId);

  public abstract ShuffleClientImpl.ReduceFileGroups updateFileGroup(int shuffleId, int partitionId)
      throws CelebornIOException;

  public ShuffleClientImpl.ReduceFileGroups updateFileGroup(
      int shuffleId, int startPartition, int endPartition) throws CelebornIOException {
    if (startPartition < 0 || endPartition < startPartition) {
      throw new IllegalArgumentException(
          String.format("Invalid reducer file group range [%d, %d)", startPartition, endPartition));
    }

    ShuffleClientImpl.ReduceFileGroups merged =
        new ShuffleClientImpl.ReduceFileGroups(
            new ConcurrentHashMap<>(),
            null,
            ConcurrentHashMap.newKeySet(),
            new ConcurrentHashMap<>());
    for (int partitionId = startPartition; partitionId < endPartition; partitionId++) {
      ShuffleClientImpl.ReduceFileGroups current = updateFileGroup(shuffleId, partitionId);
      if (current.partitionGroups != null) {
        merged.partitionGroups.putAll(current.partitionGroups);
      }
      if (current.partitionIds != null) {
        merged.partitionIds.addAll(current.partitionIds);
      }
      if (current.pushFailedBatches != null) {
        merged.pushFailedBatches.putAll(current.pushFailedBatches);
      }
      if (merged.mapAttempts == null) {
        merged.mapAttempts = current.mapAttempts;
      }
    }
    return merged;
  }

  public abstract boolean isShuffleStageEnd(int shuffleId) throws Exception;

  // Reduce side read partition which is deduplicated by mapperId+mapperAttemptNum+batchId, batchId
  // is a self-incrementing variable hidden in the implementation when sending data.
  /**
   * @param shuffleId the unique shuffle id of the application
   * @param partitionId the partition id to read from
   * @param attemptNumber the attempt id of reduce task, can be safely set to any value
   * @param startMapIndex the index of start map index of interested map range, set to 0 if you want
   *     to read all partition data
   * @param endMapIndex the index of end map index of interested map range, set to
   *     `Integer.MAX_VALUE` if you want to read all partition data
   * @param metricsCallback callback to report metrics
   * @return
   * @throws IOException
   */
  public CelebornInputStream readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      MetricsCallback metricsCallback)
      throws IOException {
    return readPartition(
        shuffleId,
        shuffleId,
        partitionId,
        attemptNumber,
        taskId,
        startMapIndex,
        endMapIndex,
        null,
        null,
        null,
        null,
        null,
        null,
        metricsCallback,
        true);
  }

  public abstract CelebornInputStream readPartition(
      int shuffleId,
      int appShuffleId,
      int partitionId,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      ExceptionMaker exceptionMaker,
      ArrayList<PartitionLocation> locations,
      ArrayList<PbStreamHandler> streamHandlers,
      Map<String, LocationPushFailedBatches> failedBatchSetMap,
      Map<String, Pair<Integer, Integer>> chunksRange,
      int[] mapAttempts,
      MetricsCallback metricsCallback,
      boolean needDecompress)
      throws IOException;

  public abstract boolean cleanupShuffle(int shuffleId);

  public abstract void shutdown();

  public abstract PartitionLocation registerMapPartitionTask(
      int shuffleId, int numMappers, int mapId, int attemptId, int partitionId) throws IOException;

  public abstract ConcurrentHashMap<Integer, PartitionLocation> getPartitionLocation(
      int shuffleId, int numMappers, int numPartitions) throws CelebornIOException;

  public abstract PushState getPushState(String mapKey);

  public abstract Tuple2<Integer, Boolean> getShuffleId(
      int appShuffleId, String appShuffleIdentifier, boolean isWriter, boolean isBarrierStage);

  /**
   * report shuffle data fetch failure to LifecycleManager for special handling, eg, shuffle status
   * cleanup for spark app. It must be a sync call and make sure the cleanup is done, otherwise,
   * incorrect shuffle data can be fetched in re-run tasks
   */
  public abstract boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, long taskId);

  /**
   * Report barrier task failure. When any barrier task fails, all (shuffle) output for that stage
   * attempt is to be discarded, and spark will recompute the entire stage
   */
  public abstract boolean reportBarrierTaskFailure(int appShuffleId, String appShuffleIdentifier);

  public abstract TransportClientFactory getDataClientFactory();

  public abstract void excludeFailedFetchLocation(String hostAndFetchPort, Exception e);

  public static void registerDeserializeReducerFileGroupResponseFunction(
      BiFunction<Integer, byte[], ControlMessages.GetReducerFileGroupResponse> function) {
    if (!deserializeReducerFileGroupResponseFunction.isPresent()) {
      deserializeReducerFileGroupResponseFunction = Optional.ofNullable(function);
    }
  }

  public static ControlMessages.GetReducerFileGroupResponse deserializeReducerFileGroupResponse(
      int shuffleId, byte[] bytes) {
    if (!deserializeReducerFileGroupResponseFunction.isPresent()) {
      // Should never happen
      logger.warn("DeserializeReducerFileGroupResponseFunction is not registered.");
      return null;
    }
    return deserializeReducerFileGroupResponseFunction.get().apply(shuffleId, bytes);
  }
}
