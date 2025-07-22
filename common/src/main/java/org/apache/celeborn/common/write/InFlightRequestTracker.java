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

package org.apache.celeborn.common.write;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.util.JavaUtils;

/*
 * This class is for track in flight request and limit request.
 * */
public class InFlightRequestTracker {
  private static final Logger logger = LoggerFactory.getLogger(InFlightRequestTracker.class);

  private final long waitInflightTimeoutMs;
  private final long delta;
  private final PushState pushState;
  private final PushStrategy pushStrategy;

  private final AtomicInteger batchId = new AtomicInteger();
  private final ConcurrentHashMap<String, Set<Integer>> inflightBatchesPerAddress =
      JavaUtils.newConcurrentHashMap();
  private ConcurrentHashMap<String, LongAdder> inflightBytesSizePerAddress = null;
  private ConcurrentHashMap<Integer, Integer> inflightBatchBytesSizes = null;

  private final int maxInFlightReqsTotal;
  private final boolean maxInFlightBytesSizeEnabled;
  private Long maxInFlightBytesSizeTotal = null;
  private Long maxInFlightBytesSizePerWorker = null;
  private final LongAdder totalInflightReqs = new LongAdder();
  private LongAdder totalInflightBytes = null;

  private volatile boolean cleaned = false;

  public InFlightRequestTracker(CelebornConf conf, PushState pushState) {
    this.waitInflightTimeoutMs = conf.clientPushLimitInFlightTimeoutMs();
    this.delta = conf.clientPushLimitInFlightSleepDeltaMs();
    this.pushState = pushState;
    this.pushStrategy = PushStrategy.getStrategy(conf);
    this.maxInFlightReqsTotal = conf.clientPushMaxReqsInFlightTotal();
    this.maxInFlightBytesSizeEnabled = conf.clientPushMaxBytesSizeInFlightEnabled();
    if (this.maxInFlightBytesSizeEnabled) {
      this.inflightBytesSizePerAddress = JavaUtils.newConcurrentHashMap();
      this.inflightBatchBytesSizes = JavaUtils.newConcurrentHashMap();
      this.maxInFlightBytesSizeTotal = conf.clientPushMaxBytesSizeInFlightTotal();
      this.maxInFlightBytesSizePerWorker = conf.clientPushMaxBytesSizeInFlightPerWorker();
      this.totalInflightBytes = new LongAdder();
    }
  }

  public void addBatch(int batchId, int batchBytesSize, String hostAndPushPort) {
    Set<Integer> batchIdSetPerPair =
        inflightBatchesPerAddress.computeIfAbsent(
            hostAndPushPort, id -> ConcurrentHashMap.newKeySet());
    batchIdSetPerPair.add(batchId);
    totalInflightReqs.increment();
    if (maxInFlightBytesSizeEnabled) {
      LongAdder bytesSizePerPair =
          inflightBytesSizePerAddress.computeIfAbsent(hostAndPushPort, id -> new LongAdder());
      bytesSizePerPair.add(batchBytesSize);
      inflightBatchBytesSizes.put(batchId, batchBytesSize);
      totalInflightBytes.add(batchBytesSize);
    }
  }

  public void removeBatch(int batchId, String hostAndPushPort) {
    Set<Integer> batchIdSet = inflightBatchesPerAddress.get(hostAndPushPort);
    if (batchIdSet != null) {
      batchIdSet.remove(batchId);
    } else {
      logger.info("Batches of {} in flight is null.", hostAndPushPort);
    }
    totalInflightReqs.decrement();
    if (maxInFlightBytesSizeEnabled) {
      int inflightBatchBytesSize =
          -Optional.ofNullable(inflightBatchBytesSizes.remove(batchId)).orElse(0);
      LongAdder inflightBytesSize = inflightBytesSizePerAddress.get(hostAndPushPort);
      if (inflightBytesSize != null) {
        inflightBytesSize.add(inflightBatchBytesSize);
      }
      totalInflightBytes.add(inflightBatchBytesSize);
    }
  }

  public void onSuccess(String hostAndPushPort) {
    pushStrategy.onSuccess(hostAndPushPort);
  }

  public void onCongestControl(String hostAndPushPort) {
    pushStrategy.onCongestControl(hostAndPushPort);
  }

  public Set<Integer> getBatchIdSetByAddressPair(String hostAndPort) {
    return inflightBatchesPerAddress.computeIfAbsent(
        hostAndPort, pair -> ConcurrentHashMap.newKeySet());
  }

  public LongAdder getBatchBytesSizeByAddressPair(String hostAndPort) {
    return maxInFlightBytesSizeEnabled
        ? inflightBytesSizePerAddress.computeIfAbsent(hostAndPort, id -> new LongAdder())
        : new LongAdder();
  }

  public boolean limitMaxInFlight(String hostAndPushPort) throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    pushStrategy.limitPushSpeed(pushState, hostAndPushPort);
    int currentMaxReqsInFlight = pushStrategy.getCurrentMaxReqsInFlight(hostAndPushPort);

    Set<Integer> batchIdSet = getBatchIdSetByAddressPair(hostAndPushPort);
    LongAdder batchBytesSize = getBatchBytesSizeByAddressPair(hostAndPushPort);
    long times = waitInflightTimeoutMs / delta;
    try {
      while (times > 0) {
        if (cleaned) {
          // MapEnd cleans up push state, which does not exceed the max requests in flight limit.
          return false;
        } else {
          if ((totalInflightReqs.sum() <= maxInFlightReqsTotal
                  && batchIdSet.size() <= currentMaxReqsInFlight)
              || (maxInFlightBytesSizeEnabled
                  && totalInflightBytes.sum() <= maxInFlightBytesSizeTotal
                  && batchBytesSize.sum() <= maxInFlightBytesSizePerWorker)) {
            break;
          }
          if (pushState.exception.get() != null) {
            throw pushState.exception.get();
          }
          Thread.sleep(delta);
          times--;
        }
      }
    } catch (InterruptedException e) {
      pushState.exception.set(new CelebornIOException(e));
    }

    if (times <= 0) {
      if (totalInflightReqs.sum() > maxInFlightReqsTotal
          || batchIdSet.size() > currentMaxReqsInFlight) {
        logger.warn(
            "After waiting for {} ms, "
                + "there are still {} requests in flight (limit: {}): "
                + "{} batches for hostAndPushPort {}, "
                + "which exceeds the current limit {}.",
            waitInflightTimeoutMs,
            totalInflightReqs.sum(),
            maxInFlightReqsTotal,
            batchIdSet.size(),
            hostAndPushPort,
            currentMaxReqsInFlight);
      }
      if (maxInFlightBytesSizeEnabled
          && (totalInflightBytes.sum() > maxInFlightBytesSizeTotal
              || batchBytesSize.sum() > maxInFlightBytesSizePerWorker)) {
        logger.warn(
            "After waiting for {} ms, "
                + "there are still {} bytes in flight (limit: {}): "
                + "{} bytes for hostAndPushPort {}, "
                + "which exceeds the current limit {}.",
            waitInflightTimeoutMs,
            totalInflightBytes.sum(),
            maxInFlightBytesSizeTotal,
            batchBytesSize.sum(),
            hostAndPushPort,
            maxInFlightBytesSizePerWorker);
      }
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

    try {
      while (times > 0) {
        if (cleaned) {
          // MapEnd cleans up push state, which does not exceed the zero requests in flight limit.
          return false;
        } else {
          if (totalInflightReqs.sum() == 0
              && (!maxInFlightBytesSizeEnabled || totalInflightBytes.sum() == 0)) {
            break;
          }
          if (pushState.exception.get() != null) {
            throw pushState.exception.get();
          }
          Thread.sleep(delta);
          times--;
        }
      }
    } catch (InterruptedException e) {
      pushState.exception.set(new CelebornIOException(e));
    }

    if (times <= 0) {
      if (totalInflightReqs.sum() > 0) {
        logger.error(
            "After waiting for {} ms, "
                + "there are still {} requests in flight: {}, "
                + "which exceeds the current limit 0.",
            waitInflightTimeoutMs,
            totalInflightReqs.sum(),
            inflightBatchesPerAddress.entrySet().stream()
                .filter(c -> !c.getValue().isEmpty())
                .map(c -> c.getValue().size() + " batches for hostAndPushPort " + c.getKey())
                .collect(Collectors.joining(", ", "[", "]")));
      }
      if (maxInFlightBytesSizeEnabled && totalInflightBytes.sum() > 0) {
        logger.error(
            "After waiting for {} ms, "
                + "there are still {} bytes in flight: {}, "
                + "which exceeds the current limit 0.",
            waitInflightTimeoutMs,
            totalInflightBytes.sum(),
            inflightBytesSizePerAddress.entrySet().stream()
                .filter(c -> c.getValue().sum() != 0)
                .map(c -> c.getValue().sum() + " bytes for hostAndPushPort " + c.getKey())
                .collect(Collectors.joining(", ", "[", "]")));
      }
    }

    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    return times <= 0;
  }

  public int remainingAllowPushes(String hostAndPushPort) {
    return pushStrategy.getCurrentMaxReqsInFlight(hostAndPushPort)
        - getBatchIdSetByAddressPair(hostAndPushPort).size();
  }

  protected int nextBatchId() {
    return batchId.incrementAndGet();
  }

  public void cleanup() {
    logger.info(
        "Cleanup {} requests and {} batches in flight.",
        totalInflightReqs.sum(),
        inflightBatchesPerAddress.values().stream().mapToInt(Set::size).sum());
    cleaned = true;
    inflightBatchesPerAddress.clear();
    pushStrategy.clear();
    if (maxInFlightBytesSizeEnabled) {
      logger.info("Cleanup {} bytes in flight.", totalInflightBytes.sum());
      inflightBytesSizePerAddress.clear();
      inflightBatchBytesSizes.clear();
    }
  }
}
