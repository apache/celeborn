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

package org.apache.celeborn.common.network.server.ratelimit;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.identity.UserIdentifier;

public class RateLimitController {

  private static final Logger logger = LoggerFactory.getLogger(RateLimitController.class);
  private static volatile RateLimitController _INSTANCE = null;

  private final int sampleTimeWindowSeconds;
  private final long highWatermark;
  private final long lowWatermark;
  private final long userInactiveTimeMills;

  private final AtomicBoolean overHighWatermark = new AtomicBoolean(false);

  private final BufferStatusHub totalBufferStatusHub;

  private final ConcurrentHashMap<UserIdentifier, UserBufferInfo> userBufferStatuses;

  private final ScheduledExecutorService removeUserExecutorService;

  private RateLimitController(
      int sampleTimeWindowSeconds,
      long highWatermark,
      long lowWatermark,
      long userInactiveTimeMills,
      long checkInterval) {
    assert (highWatermark > lowWatermark);

    this.sampleTimeWindowSeconds = sampleTimeWindowSeconds;
    this.highWatermark = highWatermark;
    this.lowWatermark = lowWatermark;
    this.userInactiveTimeMills = userInactiveTimeMills;
    this.totalBufferStatusHub = new BufferStatusHub(sampleTimeWindowSeconds);
    this.userBufferStatuses = new ConcurrentHashMap<>();

    this.removeUserExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "remove-inactive-user");
              thread.setDaemon(true);
              return thread;
            });

    this.removeUserExecutorService.scheduleWithFixedDelay(
        this::removeInactiveUsers, 0, checkInterval, TimeUnit.SECONDS);
  }

  public static RateLimitController initialize(
      int sampleTimeWindowSeconds,
      long highWatermark,
      long lowWatermark,
      long userInactiveTimeMills,
      long checkInterval) {
    if (_INSTANCE == null) {
      _INSTANCE =
          new RateLimitController(
              sampleTimeWindowSeconds,
              highWatermark,
              lowWatermark,
              userInactiveTimeMills,
              checkInterval);
    }

    return _INSTANCE;
  }

  public static RateLimitController instance() {
    return _INSTANCE;
  }

  private static class UserBufferInfo {
    long timestamp;
    final BufferStatusHub bufferStatusHub;

    public UserBufferInfo(long timestamp, BufferStatusHub bufferStatusHub) {
      this.timestamp = timestamp;
      this.bufferStatusHub = bufferStatusHub;
    }

    synchronized void updateInfo(long timestamp, BufferStatusHub.BufferStatusNode node) {
      this.timestamp = timestamp;
      this.bufferStatusHub.add(node);
    }

    public long getTimestamp() {
      return timestamp;
    }

    public BufferStatusHub getBufferStatusHub() {
      return bufferStatusHub;
    }
  }

  /**
   * 1. If the total pending bytes is over high watermark, will congest users who produce speed is
   * higher than the potential average consume speed. 2. Will stop congest these uses until the
   * pending bytes lower to low watermark. 3. If the pending bytes doesn't exceed the high
   * watermark, will allow all users to try to get max throughout capacity.
   */
  public boolean isUserCongested(UserIdentifier userIdentifier) {
    if (userBufferStatuses.size() == 0) {
      return false;
    }

    BufferStatusHub.BufferStatusNode totalBufferStatus = totalBufferStatusHub.sum();
    long pendingConsumed = totalBufferStatus.bytesPendingConsumed();

    // The potential consume speed in average
    long avgConsumeSpeed =
        totalBufferStatus.bytesConsumed()
            / ((long) sampleTimeWindowSeconds * userBufferStatuses.size());

    if (pendingConsumed > highWatermark) {
      overHighWatermark.set(true);
    }

    if (overHighWatermark.get()) {
      if (pendingConsumed < lowWatermark) {
        overHighWatermark.set(false);
        return false;
      }

      UserBufferInfo userBufferInfo = userBufferStatuses.get(userIdentifier);
      if (userBufferInfo != null) {
        BufferStatusHub.BufferStatusNode userBufferStatus =
            userBufferInfo.getBufferStatusHub().sum();

        // If the user produce speed is higher that the avg consume speed, will congest it
        long userProduceSpeed = userBufferStatus.bytesAdded() / sampleTimeWindowSeconds;
        return userProduceSpeed > avgConsumeSpeed;
      }
    }

    return false;
  }

  public void incrementBytes(UserIdentifier userIdentifier, int numBytes) {
    long currentTimeMillis = System.currentTimeMillis();
    UserBufferInfo userBufferInfo =
        userBufferStatuses.computeIfAbsent(
            userIdentifier,
            user -> {
              logger.info("New user {} comes, initializing its rate status", user);
              BufferStatusHub bufferStatusHub = new BufferStatusHub(sampleTimeWindowSeconds);
              return new UserBufferInfo(currentTimeMillis, bufferStatusHub);
            });

    BufferStatusHub.BufferStatusNode node = new BufferStatusHub.BufferStatusNode(numBytes, 0);
    totalBufferStatusHub.add(node);
    userBufferInfo.updateInfo(currentTimeMillis, node);
  }

  public void decrementBytes(UserIdentifier userIdentifier, int numBytes) {
    UserBufferInfo userBufferInfo = userBufferStatuses.get(userIdentifier);

    if (userBufferInfo == null) {
      logger.warn(
          "The user {} is already in inactive status, while we're still trying "
              + "to update it, will simply ignore to decrement bytes here",
          userIdentifier);
      return;
    }

    long currentTimeMillis = System.currentTimeMillis();
    BufferStatusHub.BufferStatusNode node = new BufferStatusHub.BufferStatusNode(0, numBytes);
    totalBufferStatusHub.add(node);
    userBufferInfo.updateInfo(currentTimeMillis, node);
  }

  private void removeInactiveUsers() {
    try {
      long currentTimeMillis = System.currentTimeMillis();

      Iterator<Map.Entry<UserIdentifier, UserBufferInfo>> iterator =
          userBufferStatuses.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<UserIdentifier, UserBufferInfo> next = iterator.next();
        UserIdentifier userIdentifier = next.getKey();
        UserBufferInfo userBufferInfo = next.getValue();
        if (currentTimeMillis - userBufferInfo.getTimestamp() >= userInactiveTimeMills) {
          userBufferStatuses.remove(userIdentifier);
          logger.info(
              String.format(
                  "User: %s has been expired, remove it from rate limit list", userIdentifier));
        }
      }
    } catch (Exception e) {
      logger.error("Error occurs when removing inactive users, ", e);
    }
  }

  public static void destroy() {
    if (_INSTANCE != null) {
      _INSTANCE.removeUserExecutorService.shutdownNow();
      _INSTANCE.userBufferStatuses.clear();
      _INSTANCE.totalBufferStatusHub.clear();
      _INSTANCE = null;
    }
  }
}
