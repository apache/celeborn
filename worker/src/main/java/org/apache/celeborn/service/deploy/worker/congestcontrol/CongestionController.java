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

package org.apache.celeborn.service.deploy.worker.congestcontrol;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.quota.UserTrafficQuota;
import org.apache.celeborn.common.quota.WorkerTrafficQuota;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.ThreadUtils;
import org.apache.celeborn.server.common.service.config.ConfigService;
import org.apache.celeborn.service.deploy.worker.WorkerSource;
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager;

public class CongestionController {

  private static final Logger logger = LoggerFactory.getLogger(CongestionController.class);
  private static volatile CongestionController _INSTANCE = null;
  private final WorkerSource workerSource;

  private final int sampleTimeWindowSeconds;
  private final long userInactiveTimeMills;

  private final AtomicBoolean overHighWatermark = new AtomicBoolean(false);

  private final BufferStatusHub consumedBufferStatusHub;

  private final BufferStatusHub producedBufferStatusHub;

  private final ConcurrentHashMap<UserIdentifier, UserBufferInfo> userBufferStatuses;

  private final ScheduledExecutorService removeUserExecutorService;

  private final ScheduledExecutorService checkService;

  private final ConcurrentHashMap<UserIdentifier, UserCongestionControlContext>
      userCongestionContextMap;

  private final ConfigService configService;

  private final UserTrafficQuota defaultUserQuota;

  private volatile WorkerTrafficQuota workerTrafficQuota;

  protected CongestionController(
      WorkerSource workerSource,
      int sampleTimeWindowSeconds,
      CelebornConf conf,
      ConfigService configService) {

    this.workerSource = workerSource;
    this.sampleTimeWindowSeconds = sampleTimeWindowSeconds;
    this.userInactiveTimeMills = conf.workerCongestionControlUserInactiveIntervalMs();
    this.consumedBufferStatusHub = new BufferStatusHub(sampleTimeWindowSeconds);
    this.producedBufferStatusHub = new BufferStatusHub(sampleTimeWindowSeconds);
    this.userBufferStatuses = JavaUtils.newConcurrentHashMap();
    this.userCongestionContextMap = JavaUtils.newConcurrentHashMap();

    defaultUserQuota =
        new UserTrafficQuota(
            conf.workerCongestionControlUserProduceSpeedHighWatermark(),
            conf.workerCongestionControlUserProduceSpeedLowWatermark());

    workerTrafficQuota =
        new WorkerTrafficQuota(
            conf.workerCongestionControlDiskBufferHighWatermark(),
            conf.workerCongestionControlDiskBufferLowWatermark(),
            conf.workerCongestionControlWorkerProduceSpeedHighWatermark(),
            conf.workerCongestionControlWorkerProduceSpeedLowWatermark());

    this.removeUserExecutorService =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor(
            "worker-congestion-controller-inactive-user-remover");

    this.removeUserExecutorService.scheduleWithFixedDelay(
        this::removeInactiveUsers, 0, userInactiveTimeMills, TimeUnit.MILLISECONDS);

    this.checkService =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-congestion-controller-checker");

    this.checkService.scheduleWithFixedDelay(
        this::checkCongestion,
        0,
        conf.workerCongestionControlCheckIntervalMs(),
        TimeUnit.MILLISECONDS);

    this.workerSource.addGauge(
        WorkerSource.POTENTIAL_CONSUME_SPEED(), this::getPotentialConsumeSpeed);

    this.workerSource.addGauge(
        WorkerSource.WORKER_CONSUME_SPEED(), consumedBufferStatusHub::avgBytesPerSec);

    this.configService = configService;

    if (configService != null) {
      updateQuota();
      configService.registerListenerOnConfigUpdate(this::updateQuota);
    }
  }

  public static synchronized CongestionController initialize(
      WorkerSource workSource,
      int sampleTimeWindowSeconds,
      CelebornConf conf,
      ConfigService configService) {
    _INSTANCE = new CongestionController(workSource, sampleTimeWindowSeconds, conf, configService);
    return _INSTANCE;
  }

  public static CongestionController instance() {
    return _INSTANCE;
  }

  /**
   * 1. If the total pending bytes is over high watermark, will congest users who produce speed is
   * higher than the potential average consume speed.
   *
   * <p>2. Will stop congest these uses until the pending bytes lower to low watermark.
   *
   * <p>3. If the pending bytes doesn't exceed the high watermark, will allow all users to try to
   * get max throughout capacity.
   */
  public boolean isUserCongested(UserCongestionControlContext userCongestionControlContext) {
    if (userBufferStatuses.isEmpty()) {
      return false;
    }

    UserIdentifier userIdentifier = userCongestionControlContext.getUserIdentifier();
    long userProduceSpeed = getUserProduceSpeed(userCongestionControlContext.getUserBufferInfo());
    UserTrafficQuota userTrafficQuota = userCongestionControlContext.getUserTrafficQuota();
    // If the user produce speed is higher that the avg consume speed, will congest it
    if (overHighWatermark.get()) {
      long avgConsumeSpeed = getPotentialProduceSpeed();
      if (userProduceSpeed > avgConsumeSpeed) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "The user {}, produceSpeed is {}, while consumeSpeed is {}, need to congest it.",
              userIdentifier,
              userProduceSpeed,
              avgConsumeSpeed);
        }
        return true;
      }
    }

    if (userProduceSpeed > userTrafficQuota.userProduceSpeedHighWatermark()) {
      userCongestionControlContext.onCongestionControl();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The user {}, produceSpeed is {}, while userProduceSpeedHighWatermark is {}, need to congest it.",
            userIdentifier,
            userProduceSpeed,
            userTrafficQuota.userProduceSpeedHighWatermark());
      }
    } else if (userCongestionControlContext.inCongestionControl()
        && userProduceSpeed < userTrafficQuota.userProduceSpeedLowWatermark()) {
      userCongestionControlContext.offCongestionControl();
    }
    return userCongestionControlContext.inCongestionControl();
  }

  public UserBufferInfo getUserBuffer(UserIdentifier userIdentifier) {
    return userBufferStatuses.computeIfAbsent(
        userIdentifier,
        user -> {
          logger.info("New user {} comes, initializing its rate status", user);
          BufferStatusHub bufferStatusHub = new BufferStatusHub(sampleTimeWindowSeconds);
          UserBufferInfo userInfo = new UserBufferInfo(System.currentTimeMillis(), bufferStatusHub);
          return userInfo;
        });
  }

  public void consumeBytes(int numBytes) {
    long currentTimeMillis = System.currentTimeMillis();
    BufferStatusHub.BufferStatusNode node = new BufferStatusHub.BufferStatusNode(numBytes);
    consumedBufferStatusHub.add(currentTimeMillis, node);
  }

  public long getTotalPendingBytes() {
    return MemoryManager.instance().getMemoryUsage();
  }

  public void trimMemoryUsage() {
    MemoryManager.instance().trimAllListeners();
  }

  /**
   * Get avg consumed bytes in a configured time window, and divide with the number of active users
   * to determine the potential consume speed.
   */
  public long getPotentialConsumeSpeed() {
    if (userBufferStatuses.size() == 0) {
      return 0;
    }

    return consumedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
  }

  public long getPotentialProduceSpeed() {
    if (userBufferStatuses.size() == 0) {
      return 0;
    }

    return producedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
  }

  /** Get the avg user produce speed, the unit is bytes/sec. */
  private long getUserProduceSpeed(UserBufferInfo userBufferInfo) {
    if (userBufferInfo != null) {
      return userBufferInfo.getBufferStatusHub().avgBytesPerSec();
    }

    return 0L;
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
          userCongestionContextMap.remove(userIdentifier);
          workerSource.removeGauge(WorkerSource.USER_PRODUCE_SPEED(), userIdentifier.toMap());
          logger.info("User {} has been expired, remove from rate limit list", userIdentifier);
        }
      }
    } catch (Exception e) {
      logger.error("Error occurs when removing inactive users", e);
    }
  }

  protected void checkCongestion() {
    try {
      long pendingConsume = getTotalPendingBytes();
      long workerProduceSpeed = producedBufferStatusHub.avgBytesPerSec();
      if (pendingConsume < workerTrafficQuota.diskBufferLowWatermark()
          && workerProduceSpeed < workerTrafficQuota.workerProduceSpeedLowWatermark()) {
        if (overHighWatermark.compareAndSet(true, false)) {
          logger.info(
              "Pending consume and produce speed is lower than low watermark, exit congestion control");
        }
        return;
      } else if ((pendingConsume > workerTrafficQuota.diskBufferHighWatermark()
              || workerProduceSpeed > workerTrafficQuota.workerProduceSpeedHighWatermark())
          && overHighWatermark.compareAndSet(false, true)) {
        logger.info(
            "Pending consume or produce speed is higher than high watermark, need congestion control");
      }
      if (overHighWatermark.get()) {
        trimMemoryUsage();
      }
    } catch (Exception e) {
      logger.error("Congestion check error", e);
    }
  }

  public Boolean isOverHighWatermark() {
    return overHighWatermark.get();
  }

  public void close() {
    logger.info("Closing {}", this.getClass().getSimpleName());
    ThreadUtils.shutdown(this.removeUserExecutorService);
    ThreadUtils.shutdown(this.checkService);
    this.userBufferStatuses.clear();
    this.consumedBufferStatusHub.clear();
    this.producedBufferStatusHub.clear();
  }

  @VisibleForTesting
  public void shutDownCheckService() {
    ThreadUtils.shutdown(this.checkService);
  }

  public static synchronized void destroy() {
    if (_INSTANCE != null) {
      _INSTANCE.close();
      _INSTANCE = null;
    }
  }

  public BufferStatusHub getProducedBufferStatusHub() {
    return producedBufferStatusHub;
  }

  public UserCongestionControlContext getUserCongestionContext(UserIdentifier userIdentifier) {
    return userCongestionContextMap.computeIfAbsent(
        userIdentifier,
        user -> {
          UserBufferInfo userBufferInfo = getUserBuffer(userIdentifier);
          UserTrafficQuota userTrafficQuota;
          if (configService == null) {
            userTrafficQuota = defaultUserQuota;
          } else {
            userTrafficQuota =
                configService
                    .getTenantUserConfigFromCache(userIdentifier.tenantId(), userIdentifier.name())
                    .getUserTrafficQuota();
          }
          return new UserCongestionControlContext(
              userTrafficQuota,
              producedBufferStatusHub,
              userBufferInfo,
              workerSource,
              userIdentifier);
        });
  }

  public ConcurrentHashMap<UserIdentifier, UserCongestionControlContext>
      getUserCongestionContextMap() {
    return userCongestionContextMap;
  }

  public BufferStatusHub getConsumedBufferStatusHub() {
    return consumedBufferStatusHub;
  }

  private void updateQuota() {
    workerTrafficQuota = configService.getSystemConfigFromCache().getWorkerTrafficQuota();
    for (Map.Entry<UserIdentifier, UserCongestionControlContext> entry :
        userCongestionContextMap.entrySet()) {
      UserIdentifier user = entry.getKey();
      UserCongestionControlContext context = entry.getValue();
      context.updateUserTrafficQuota(
          configService
              .getTenantUserConfigFromCache(user.tenantId(), user.name())
              .getUserTrafficQuota());
    }
  }
}
