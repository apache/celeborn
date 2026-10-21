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

package org.apache.celeborn.service.deploy.master.clustermeta.ha;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.client.MasterClient;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.ResourceRequest;
import org.apache.celeborn.service.deploy.master.clustermeta.ResourceProtos.Type;

public class HeartbeatAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatAggregator.class);

  private final HARaftServer ratisServer;
  private final long batchIntervalMs;

  private final ReentrantLock pendingLock = new ReentrantLock();
  private Map<String, ResourceProtos.WorkerHeartbeatRequest> workerHeartbeats = new HashMap<>();
  private Map<String, ResourceProtos.AppHeartbeatRequest> appHeartbeats = new HashMap<>();

  // Serializes raft submissions of heartbeat batches and lifecycle requests so that a
  // lifecycle request (e.g. AppLost, WorkerLost, RegisterWorker) is never applied before
  // heartbeats that were accepted earlier.
  private final ReentrantLock submissionLock = new ReentrantLock();

  private final ScheduledExecutorService flushExecutor;

  public HeartbeatAggregator(HARaftServer ratisServer, CelebornConf conf) {
    this.ratisServer = ratisServer;
    this.batchIntervalMs = conf.masterHaHeartbeatBatchIntervalMs();
    this.flushExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-heartbeat-aggregator");
    this.flushExecutor.scheduleWithFixedDelay(
        this::flushSafely, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
    LOG.info("HeartbeatAggregator started, flush interval {} ms.", batchIntervalMs);
  }

  public void offerWorkerHeartbeat(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    pendingLock.lock();
    try {
      workerHeartbeats.put(workerKey(heartbeat), heartbeat);
    } finally {
      pendingLock.unlock();
    }
  }

  public void offerAppHeartbeat(ResourceProtos.AppHeartbeatRequest heartbeat) {
    pendingLock.lock();
    try {
      appHeartbeats.merge(heartbeat.getAppId(), heartbeat, HeartbeatAggregator::mergeAppHeartbeat);
    } finally {
      pendingLock.unlock();
    }
  }

  // App heartbeat counters (totalWritten, fileCount, shuffle/application counts and fallback
  // counts) are deltas reset by the client after each report, so coalescing must accumulate
  // them instead of keeping only the newest one. Time and client gauges are snapshots, for
  // which the newest value wins.
  private static ResourceProtos.AppHeartbeatRequest mergeAppHeartbeat(
      ResourceProtos.AppHeartbeatRequest pending, ResourceProtos.AppHeartbeatRequest latest) {
    return latest
        .toBuilder()
        .setTotalWritten(pending.getTotalWritten() + latest.getTotalWritten())
        .setFileCount(pending.getFileCount() + latest.getFileCount())
        .setShuffleCount(pending.getShuffleCount() + latest.getShuffleCount())
        .setApplicationCount(pending.getApplicationCount() + latest.getApplicationCount())
        .putAllShuffleFallbackCounts(
            mergeCounts(
                pending.getShuffleFallbackCountsMap(), latest.getShuffleFallbackCountsMap()))
        .putAllApplicationFallbackCounts(
            mergeCounts(
                pending.getApplicationFallbackCountsMap(),
                latest.getApplicationFallbackCountsMap()))
        .build();
  }

  private static Map<String, Long> mergeCounts(Map<String, Long> a, Map<String, Long> b) {
    Map<String, Long> merged = new HashMap<>(a);
    b.forEach((key, value) -> merged.merge(key, value, Long::sum));
    return merged;
  }

  private static String workerKey(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    return workerKey(
        heartbeat.getHost(),
        heartbeat.getRpcPort(),
        heartbeat.getPushPort(),
        heartbeat.getFetchPort(),
        heartbeat.getReplicatePort());
  }

  static String workerKey(
      String host, int rpcPort, int pushPort, int fetchPort, int replicatePort) {
    return host + ":" + rpcPort + ":" + pushPort + ":" + fetchPort + ":" + replicatePort;
  }

  public void stop() {
    flushSafely();
    flushExecutor.shutdownNow();
  }

  private void flushSafely() {
    try {
      flush();
    } catch (Throwable t) {
      // Dropped batches self-heal next interval.
      LOG.error("Failed to flush aggregated heartbeats, dropping this batch.", t);
    }
  }

  private void flush() {
    submissionLock.lock();
    try {
      List<ResourceProtos.WorkerHeartbeatRequest> drainedWorkers;
      List<ResourceProtos.AppHeartbeatRequest> drainedApps;
      pendingLock.lock();
      try {
        if (workerHeartbeats.isEmpty() && appHeartbeats.isEmpty()) {
          return;
        }
        drainedWorkers = new ArrayList<>(workerHeartbeats.values());
        workerHeartbeats.clear();
        drainedApps = new ArrayList<>(appHeartbeats.values());
        appHeartbeats.clear();
      } finally {
        pendingLock.unlock();
      }
      submitBatch(drainedWorkers, drainedApps);
    } finally {
      submissionLock.unlock();
    }
  }

  /**
   * Submits a lifecycle request (e.g. AppLost, WorkerLost, RegisterWorker) while preserving its
   * order relative to buffered heartbeats: pending heartbeats of the affected apps and workers are
   * drained and flushed first, and the submissionLock serializes this against in-flight batch
   * flushes, so no earlier accepted heartbeat can be applied after the lifecycle request.
   */
  public void submitWithOrdering(
      Collection<String> appIds, Collection<String> workerKeys, ResourceRequest lifecycleRequest) {
    submissionLock.lock();
    try {
      List<ResourceProtos.WorkerHeartbeatRequest> drainedWorkers = new ArrayList<>();
      List<ResourceProtos.AppHeartbeatRequest> drainedApps = new ArrayList<>();
      pendingLock.lock();
      try {
        for (String appId : appIds) {
          ResourceProtos.AppHeartbeatRequest heartbeat = appHeartbeats.remove(appId);
          if (heartbeat != null) {
            drainedApps.add(heartbeat);
          }
        }
        for (String workerKey : workerKeys) {
          ResourceProtos.WorkerHeartbeatRequest heartbeat = workerHeartbeats.remove(workerKey);
          if (heartbeat != null) {
            drainedWorkers.add(heartbeat);
          }
        }
      } finally {
        pendingLock.unlock();
      }
      if (!drainedWorkers.isEmpty() || !drainedApps.isEmpty()) {
        submitBatch(drainedWorkers, drainedApps);
      }
      ratisServer.submitRequest(lifecycleRequest);
    } finally {
      submissionLock.unlock();
    }
  }

  // Caller must hold submissionLock.
  private void submitBatch(
      List<ResourceProtos.WorkerHeartbeatRequest> drainedWorkers,
      List<ResourceProtos.AppHeartbeatRequest> drainedApps) {
    if (!ratisServer.isLeader()) {
      return;
    }

    ResourceRequest batchRequest =
        ResourceRequest.newBuilder()
            .setCmdType(Type.BatchHeartbeat)
            .setRequestId(MasterClient.genRequestId())
            .setBatchHeartbeatRequest(
                ResourceProtos.BatchHeartbeatRequest.newBuilder()
                    .addAllWorkerHeartbeats(drainedWorkers)
                    .addAllAppHeartbeats(drainedApps)
                    .build())
            .build();
    long startNs = System.nanoTime();
    ratisServer.submitRequest(batchRequest);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    if (elapsedMs > batchIntervalMs) {
      LOG.warn(
          "Submitting aggregated heartbeats ({} worker, {} app) took {} ms; raft commits are "
              + "slower than the flush interval {} ms.",
          drainedWorkers.size(),
          drainedApps.size(),
          elapsedMs,
          batchIntervalMs);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Flushed aggregated heartbeats, {} worker heartbeats, {} app heartbeats.",
          drainedWorkers.size(),
          drainedApps.size());
    }
  }
}
