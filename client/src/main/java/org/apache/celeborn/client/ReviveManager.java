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

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.ReviveRequest;
import org.apache.celeborn.common.protocol.message.StatusCode;
import org.apache.celeborn.common.util.ThreadUtils;

class ReviveManager {
  private static final Logger logger = LoggerFactory.getLogger(ShuffleClientImpl.class);

  LinkedBlockingQueue<ReviveRequest> requests = new LinkedBlockingQueue<>();
  private final long interval;
  private final int batchSize;
  ShuffleClientImpl shuffleClient;
  private ScheduledExecutorService batchReviveRequestScheduler =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-revive-scheduler");

  public ReviveManager(ShuffleClientImpl shuffleClient, CelebornConf conf) {
    this.shuffleClient = shuffleClient;
    this.interval = conf.clientPushReviveInterval();
    this.batchSize = conf.clientPushReviveBatchSize();

    batchReviveRequestScheduler.scheduleAtFixedRate(
        () -> {
          Map<Integer, Set<ReviveRequest>> shuffleMap = new HashMap<>();
          int count = 0;
          while (!requests.isEmpty()) {
            ReviveRequest request = requests.poll();
            while (request != null && count < batchSize) {
              Set<ReviveRequest> set =
                  shuffleMap.computeIfAbsent(request.shuffleId, id -> new HashSet<>());
              set.add(request);
              count++;
              request = requests.poll();
            }
            if (request != null) {
              Set<ReviveRequest> set =
                  shuffleMap.computeIfAbsent(request.shuffleId, id -> new HashSet<>());
              set.add(request);
              count++;
            }
            for (Map.Entry<Integer, Set<ReviveRequest>> shuffleEntry : shuffleMap.entrySet()) {
              // Call reviveBatch for requests in the same (appId, shuffleId)
              int shuffleId = shuffleEntry.getKey();
              Set<ReviveRequest> requests = shuffleEntry.getValue();
              Set<Integer> mapIds = new HashSet<>();
              ArrayList<ReviveRequest> filteredRequests = new ArrayList<>();
              Map<Integer, ReviveRequest> requestsToSend = new HashMap<>();

              // Insert request that is not MapperEnded and with the max epoch
              // into requestsToSend
              Iterator<ReviveRequest> iter = requests.iterator();
              while (iter.hasNext()) {
                ReviveRequest req = iter.next();
                if (shuffleClient.checkRevivedLocation(shuffleId, req.partitionId, req.epoch, false)
                    || shuffleClient.mapperEnded(shuffleId, req.mapId)) {
                  req.reviveStatus = StatusCode.SUCCESS.getValue();
                } else {
                  filteredRequests.add(req);
                  mapIds.add(req.mapId);
                  PartitionLocation loc = req.loc;
                  if (!requestsToSend.containsKey(req.partitionId)
                      || requestsToSend.get(req.partitionId).epoch < req.epoch) {
                    requestsToSend.put(req.partitionId, req);
                  }
                }
              }

              if (!requestsToSend.isEmpty()) {
                // Call reviveBatch. Return null means Exception caught or
                // SHUFFLE_NOT_REGISTERED
                Map<Integer, Integer> results =
                    shuffleClient.reviveBatch(shuffleId, mapIds, requestsToSend.values());
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
              }
            }
          }
        },
        interval,
        interval,
        TimeUnit.MILLISECONDS);
  }

  public void addRequest(ReviveRequest request) {
    shuffleClient.excludeWorkerByCause(request.cause, request.loc);
    // This sync is necessary to ensure the add action is atomic
    try {
      requests.put(request);
    } catch (InterruptedException e) {
      logger.error("Exception when put into requests!", e);
    }
  }
}
