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

package org.apache.celeborn.client.write;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.apache.celeborn.common.protocol.message.StatusCode;

/*
 * This class is for track in flight request and limit request.
 * */
public class InFlightRequestTracker {
  private static final Logger logger = LoggerFactory.getLogger(InFlightRequestTracker.class);

  private final long waitInflightTimeoutMs;
  private final long pushTimeoutMs;
  private final long delta;
  private final PushState pushState;

  private final AtomicInteger batchId = new AtomicInteger();
  private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, BatchInfo>>
      inflightBatchesPerAddress = new ConcurrentHashMap<>();

  public InFlightRequestTracker(CelebornConf conf, PushState pushState) {
    this.waitInflightTimeoutMs = conf.pushLimitInFlightTimeoutMs();
    this.pushTimeoutMs = conf.pushDataTimeoutMs();
    this.delta = conf.pushLimitInFlightSleepDeltaMs();
    this.pushState = pushState;
  }

  public void addBatch(int batchId, String hostAndPushPort) {
    ConcurrentHashMap<Integer, BatchInfo> batchIdSetPerPair =
        inflightBatchesPerAddress.computeIfAbsent(hostAndPushPort, id -> new ConcurrentHashMap<>());
    batchIdSetPerPair.computeIfAbsent(batchId, id -> new BatchInfo());
  }

  public void removeBatch(int batchId, String hostAndPushPort) {
    ConcurrentHashMap<Integer, BatchInfo> batchIdMap =
        inflightBatchesPerAddress.get(hostAndPushPort);
    BatchInfo info = batchIdMap.remove(batchId);
    if (info != null && info.channelFuture != null) {
      info.channelFuture.cancel(true);
    }
    if (batchIdMap.size() == 0) {
      inflightBatchesPerAddress.remove(hostAndPushPort);
    }
  }

  public ConcurrentHashMap<Integer, BatchInfo> getBatchIdSetByAddressPair(String hostAndPort) {
    return inflightBatchesPerAddress.computeIfAbsent(
        hostAndPort, pair -> new ConcurrentHashMap<>());
  }

  public boolean limitMaxInFlight(String hostAndPushPort, int maxInFlight) throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    ConcurrentHashMap<Integer, BatchInfo> batchIdMap = getBatchIdSetByAddressPair(hostAndPushPort);
    long times = waitInflightTimeoutMs / delta;
    try {
      while (times > 0) {
        if (batchIdMap.size() <= maxInFlight) {
          break;
        }
        if (pushState.exception.get() != null) {
          throw pushState.exception.get();
        }
        failExpiredBatch();
        Thread.sleep(delta);
        times--;
      }
    } catch (InterruptedException e) {
      pushState.exception.set(new IOException(e));
    }

    if (times <= 0) {
      logger.warn(
          "After waiting for {} ms, "
              + "there are still {} batches in flight "
              + "for hostAndPushPort {}, "
              + "which exceeds the limit {}.",
          waitInflightTimeoutMs,
          batchIdMap.size(),
          hostAndPushPort,
          maxInFlight);
    }

    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    return times <= 0;
  }

  public boolean limitZeroInFlight() throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }
    long times = waitInflightTimeoutMs / delta;

    int inFlightSize = 0;
    try {
      while (times > 0) {
        Optional<Integer> inFlightSizeOptional =
            inflightBatchesPerAddress.values().stream().map(Map::size).reduce(Integer::sum);
        inFlightSize = inFlightSizeOptional.orElse(0);
        if (inFlightSize == 0) {
          break;
        }
        if (pushState.exception.get() != null) {
          throw pushState.exception.get();
        }
        failExpiredBatch();
        Thread.sleep(delta);
        times--;
      }
    } catch (InterruptedException e) {
      pushState.exception.set(new IOException(e));
    }

    if (times <= 0) {
      logger.error(
          "After waiting for {} ms, there are still {} batches in flight, expect 0 batches",
          waitInflightTimeoutMs,
          inFlightSize);
    }

    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    return times <= 0;
  }

  public boolean reachLimit(String hostAndPushPort, int maxInFlight) throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    ConcurrentHashMap<Integer, BatchInfo> batchIdMap = getBatchIdSetByAddressPair(hostAndPushPort);
    return batchIdMap.size() > maxInFlight;
  }

  protected int nextBatchId() {
    return batchId.incrementAndGet();
  }

  public synchronized void failExpiredBatch() {
    long currentTime = System.currentTimeMillis();
    inflightBatchesPerAddress
        .values()
        .forEach(
            batchIdMap ->
                batchIdMap
                    .values()
                    .forEach(
                        info -> {
                          if (currentTime - info.pushTime > pushTimeoutMs) {
                            if (info.callback != null) {
                              info.channelFuture.cancel(true);
                              info.callback.onFailure(
                                  new IOException(StatusCode.PUSH_DATA_TIMEOUT_MASTER.getMessage()));
                              info.channelFuture = null;
                              info.callback = null;
                            }
                          }
                        }));
  }

  public void cleanup() {
    if (!inflightBatchesPerAddress.isEmpty()) {
      logger.debug("Cancel all {} futures.", inflightBatchesPerAddress.size());
      inflightBatchesPerAddress
          .values()
          .forEach(
              batchIdMap ->
                  batchIdMap
                      .values()
                      .forEach(
                          info -> {
                            if (info.channelFuture != null) {
                              info.channelFuture.cancel(true);
                            }
                          }));
      inflightBatchesPerAddress.clear();
    }
  }

  static class BatchInfo {
    ChannelFuture channelFuture;
    long pushTime;
    RpcResponseCallback callback;
  }
}
