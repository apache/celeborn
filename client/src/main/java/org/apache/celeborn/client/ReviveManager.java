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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.ThreadUtils;

class ReviveManager {
  private static final Logger logger = LoggerFactory.getLogger(ReviveManager.class);

  LinkedBlockingQueue<ReviveRequest> requestQueue = new LinkedBlockingQueue<>();
  private final long interval;
  private final int batchSize;
  ShuffleClientImpl shuffleClient;
  private ScheduledExecutorService batchReviveRequestScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-revive-scheduler");
  private ThreadPoolExecutor batchReviveRequestHandler =
      ThreadUtils.newDaemonFixedThreadPool(2, "batch-revive-handler");
  private ThreadPoolExecutor batchReportRequestHandler =
      ThreadUtils.newDaemonFixedThreadPool(2, "batch-report-handler");

  public ReviveManager(ShuffleClientImpl shuffleClient, CelebornConf conf) {
    this.shuffleClient = shuffleClient;
    this.interval = conf.clientPushReviveInterval();
    this.batchSize = conf.clientPushReviveBatchSize();

    batchReviveRequestScheduler.scheduleWithFixedDelay(
        () -> {
          try {
            Map<Integer, Set<ReviveRequest>> urgentMap = new HashMap<>();
            Map<Integer, Set<ReviveRequest>> nonUrgentMap = new HashMap<>();
            do {
              ArrayList<ReviveRequest> batchRequests = new ArrayList<>();
              requestQueue.drainTo(batchRequests, batchSize);
              for (ReviveRequest req : batchRequests) {
                Set<ReviveRequest> set = null;
                if (req.urgent) {
                  set = urgentMap.computeIfAbsent(req.shuffleId, id -> new HashSet<>());
                } else {
                  set = nonUrgentMap.computeIfAbsent(req.shuffleId, id -> new HashSet<>());
                }
                set.add(req);
              }
              if (!urgentMap.isEmpty()) {
                reviveInternal(urgentMap, true);
              }
              if (!nonUrgentMap.isEmpty()) {
                reviveInternal(nonUrgentMap, false);
              }
              // break the loop if remaining requests is less than half of
              // `celeborn.client.push.revive.batchSize`
            } while (requestQueue.size() > batchSize / 2);
          } catch (Throwable e) {
            logger.error("Exception when batchRevive: ", e);
            throw e;
          }
        },
        interval,
        interval,
        TimeUnit.MILLISECONDS);
  }

  public void reviveInternal(Map<Integer, Set<ReviveRequest>> shuffleMap, boolean urgent) {
    for (Map.Entry<Integer, Set<ReviveRequest>> shuffleEntry : shuffleMap.entrySet()) {
      // Call reviveBatch for requests in the same (appId, shuffleId)
      int shuffleId = shuffleEntry.getKey();
      Set<ReviveRequest> requests = shuffleEntry.getValue();
      processRequests(shuffleId, requests, urgent);
    }
  }

  public void processRequests(int shuffleId, Collection<ReviveRequest> requests, boolean urgent) {
    Set<Integer> mapIds = new HashSet<>();
    ArrayList<ReviveRequest> filteredRequests = new ArrayList<>();
    Map<Integer, ReviveRequest> requestsToSend = new HashMap<>();

    // Insert request that is not MapperEnded and with the max epoch
    // into requestsToSend
    Iterator<ReviveRequest> iter = requests.iterator();
    while (iter.hasNext()) {
      ReviveRequest req = iter.next();
      if ((urgent
              && shuffleClient.newerPartitionLocationExists(
                  shuffleId, req.partitionId, req.clientMaxEpoch))
          || shuffleClient.mapperEnded(shuffleId, req.mapId)) {
        req.reviveStatus = StatusCode.SUCCESS.getValue();
      } else {
        filteredRequests.add(req);
        mapIds.add(req.mapId);
        if (!requestsToSend.containsKey(req.partitionId)
            || requestsToSend.get(req.partitionId).clientMaxEpoch < req.clientMaxEpoch) {
          requestsToSend.put(req.partitionId, req);
        }
      }
    }

    ThreadPoolExecutor handler = urgent ? batchReviveRequestHandler : batchReportRequestHandler;
    if (!requestsToSend.isEmpty()) {
      handler.submit(
          () -> {
            try {
              // Call reviveBatch. Return null means Exception caught or
              // SHUFFLE_NOT_REGISTERED
              // Do not use WriterTracerHere because traceInfo is set afterward
              long reviveStartTime = System.nanoTime();
              Map<Integer, Integer> results =
                  shuffleClient.reviveBatch(shuffleId, mapIds, requestsToSend.values(), urgent);
              long reviveCostTime = System.nanoTime() - reviveStartTime;
              if (results == null) {
                for (ReviveRequest req : filteredRequests) {
                  req.reviveStatus = StatusCode.REVIVE_FAILED.getValue();
                }
              } else {
                for (ReviveRequest req : filteredRequests) {
                  if (shuffleClient.mapperEnded(shuffleId, req.mapId)) {
                    req.reviveStatus = StatusCode.SUCCESS.getValue();
                  } else {
                    req.reviveStatus = results.get(req.partitionId);
                  }
                }
              }
            } catch (Throwable e) {
              logger.error("Exception when processRequests: ", e);
              throw e;
            }
          });
    }
  }

  public void addRequest(ReviveRequest request) {
    shuffleClient.excludeWorkerByCause(request.cause, request.loc);
    // This sync is necessary to ensure the add action is atomic
    try {
      requestQueue.put(request);
      logger.debug("Add urgent request: {}", request);
    } catch (InterruptedException e) {
      logger.error("Exception when put into requests!", e);
    }
  }

  public void close() {
    ThreadUtils.shutdown(batchReviveRequestScheduler);
  }
}
