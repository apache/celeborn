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
      appHeartbeats.put(heartbeat.getAppId(), heartbeat);
    } finally {
      pendingLock.unlock();
    }
  }

  private static String workerKey(ResourceProtos.WorkerHeartbeatRequest heartbeat) {
    return heartbeat.getHost()
        + ":"
        + heartbeat.getRpcPort()
        + ":"
        + heartbeat.getPushPort()
        + ":"
        + heartbeat.getFetchPort()
        + ":"
        + heartbeat.getReplicatePort();
  }

  public void stop() {
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
