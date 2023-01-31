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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;

public class SlowStartPushStrategy extends PushStrategy {

  private static final Logger logger = LoggerFactory.getLogger(SlowStartPushStrategy.class);
  private int maxInFlight;
  private final AtomicInteger currentMaxReqsInFlight;

  // Indicate the number of congested times even after the in flight requests reduced to 1
  private final AtomicInteger continueCongestedNumber;
  private int congestionAvoidanceFlag = 0;

  private final long maxSleepMills;

  public SlowStartPushStrategy(CelebornConf conf) {
    super(conf);
    this.currentMaxReqsInFlight = new AtomicInteger(0);
    this.continueCongestedNumber = new AtomicInteger(0);
    this.maxInFlight = conf.pushMaxReqsInFlight();
    this.maxSleepMills = conf.pushSlowStartMaxSleepMills();
  }

  /**
   * If `pushDataSlowStart` is enabled, will increase `currentMaxReqsInFlight` gradually to meet the
   * max push speed.
   *
   * <p>1. slow start period: every RTT period, `currentMaxReqsInFlight` is doubled.
   *
   * <p>2. congestion avoidance: every RTT period, `currentMaxReqsInFlight` plus 1.
   *
   * <p>Note that here we define one RTT period: one batch(currentMaxReqsInFlight) of push data
   * requests.
   */
  @Override
  public void onSuccess() {
    synchronized (currentMaxReqsInFlight) {
      continueCongestedNumber.set(0);
      if (currentMaxReqsInFlight.get() >= maxInFlight) {
        // Congestion avoidance
        congestionAvoidanceFlag++;
        if (congestionAvoidanceFlag >= currentMaxReqsInFlight.get()) {
          currentMaxReqsInFlight.incrementAndGet();
          congestionAvoidanceFlag = 0;
        }
      } else {
        // Slow start
        currentMaxReqsInFlight.incrementAndGet();
      }
    }
  }

  @Override
  public void onCongestControl() {
    synchronized (currentMaxReqsInFlight) {
      if (currentMaxReqsInFlight.get() <= 1) {
        currentMaxReqsInFlight.set(1);
        continueCongestedNumber.incrementAndGet();
      } else {
        currentMaxReqsInFlight.updateAndGet(pre -> pre / 2);
      }
      maxInFlight = currentMaxReqsInFlight.get();
      congestionAvoidanceFlag = 0;
    }
  }

  protected long getSleepTime(int currentMaxReqs) {
    if (currentMaxReqs >= conf.pushMaxReqsInFlight()) {
      return 0;
    }

    long sleepInterval = 500 - 60L * currentMaxReqs;

    if (currentMaxReqs == 1) {
      return Math.min(sleepInterval + continueCongestedNumber.get() * 1000L, maxSleepMills);
    }

    return Math.max(sleepInterval, 0);
  }

  @Override
  public void limitPushSpeed(String mapKey, PushState pushState, String hostAndPushPort)
      throws IOException {
    if (pushState.exception.get() != null) {
      throw pushState.exception.get();
    }

    long sleepInterval = getSleepTime(currentMaxReqsInFlight.get());
    if (sleepInterval > 0L) {
      try {
        logger.debug("Will sleep {} ms to control the push speed.", sleepInterval);
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        pushState.exception.set(new IOException(e));
      }
    }

    awaitInFlightRequestsMatched(mapKey, pushState, hostAndPushPort, currentMaxReqsInFlight.get());
  }

  @VisibleForTesting
  protected int getCurrentMaxReqsInFlight() {
    return currentMaxReqsInFlight.get();
  }
}
