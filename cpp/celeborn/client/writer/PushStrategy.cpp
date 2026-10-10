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

#include "celeborn/client/writer/PushStrategy.h"

#include <algorithm>
#include <thread>

namespace celeborn {
namespace client {

std::unique_ptr<PushStrategy> PushStrategy::create(
    const conf::CelebornConf& conf) {
  auto strategyName = conf.clientPushLimitStrategy();
  // The Java config entry uppercases the configured value.
  std::transform(
      strategyName.begin(),
      strategyName.end(),
      strategyName.begin(),
      ::toupper);
  if (strategyName == conf::CelebornConf::kSimplePushStrategy) {
    return std::make_unique<SimplePushStrategy>(conf);
  } else if (strategyName == conf::CelebornConf::kSlowStartPushStrategy) {
    return std::make_unique<SlowStartPushStrategy>(conf);
  } else {
    CELEBORN_FAIL("unsupported pushStrategy: " + strategyName);
  }
}

void SlowStartPushStrategy::CongestControlContext::increaseCurrentMaxReqs() {
  std::lock_guard<std::mutex> lock(mutex_);
  continueCongestedNumber_.store(0);
  if (currentMaxReqsInFlight_.load() >= reqsInFlightBlockThreshold_) {
    // Congestion avoidance
    congestionAvoidanceFlag_++;
    if (congestionAvoidanceFlag_ >= currentMaxReqsInFlight_.load()) {
      currentMaxReqsInFlight_.fetch_add(1);
      congestionAvoidanceFlag_ = 0;
    }
  } else {
    // Slow start
    currentMaxReqsInFlight_.fetch_add(1);
  }
}

void SlowStartPushStrategy::CongestControlContext::decreaseCurrentMaxReqs() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (currentMaxReqsInFlight_.load() <= 1) {
    currentMaxReqsInFlight_.store(1);
    continueCongestedNumber_.fetch_add(1);
  } else {
    currentMaxReqsInFlight_.store(currentMaxReqsInFlight_.load() / 2);
  }
  reqsInFlightBlockThreshold_ = currentMaxReqsInFlight_.load();
  congestionAvoidanceFlag_ = 0;
}

std::shared_ptr<SlowStartPushStrategy::CongestControlContext>
SlowStartPushStrategy::getCongestControlContextByAddress(
    const std::string& hostAndPushPort) {
  return congestControlInfoPerAddress_.computeIfAbsent(hostAndPushPort, [&]() {
    return std::make_shared<CongestControlContext>(maxInFlightPerWorker_);
  });
}

void SlowStartPushStrategy::onSuccess(const std::string& hostAndPushPort) {
  getCongestControlContextByAddress(hostAndPushPort)->increaseCurrentMaxReqs();
}

void SlowStartPushStrategy::onCongestControl(
    const std::string& hostAndPushPort) {
  getCongestControlContextByAddress(hostAndPushPort)->decreaseCurrentMaxReqs();
}

long SlowStartPushStrategy::getSleepTime(CongestControlContext& context) const {
  int currentMaxReqs = context.getCurrentMaxReqsInFlight();
  if (currentMaxReqs >= maxInFlightPerWorker_) {
    return 0;
  }

  long sleepInterval = initialSleepMills_ - 60L * currentMaxReqs;

  if (currentMaxReqs == 1) {
    return std::min(
        sleepInterval + context.getContinueCongestedNumber() * 1000L,
        maxSleepMills_);
  }

  return std::max(sleepInterval, 0L);
}

void SlowStartPushStrategy::limitPushSpeed(
    PushState& pushState,
    const std::string& hostAndPushPort) {
  auto exceptionMsg = pushState.getExceptionMsg();
  if (exceptionMsg.has_value()) {
    CELEBORN_FAIL(exceptionMsg.value());
  }
  auto congestControlContext =
      getCongestControlContextByAddress(hostAndPushPort);
  long sleepInterval = getSleepTime(*congestControlContext);
  if (sleepInterval > 0L) {
    VLOG(1) << "Will sleep " << sleepInterval
            << " ms to control the push speed to " << hostAndPushPort << ".";
    std::this_thread::sleep_for(utils::MS(sleepInterval));
  }
}

int SlowStartPushStrategy::getCurrentMaxReqsInFlight(
    const std::string& hostAndPushPort) {
  return getCongestControlContextByAddress(hostAndPushPort)
      ->getCurrentMaxReqsInFlight();
}

void SlowStartPushStrategy::clear() {
  congestControlInfoPerAddress_.clear();
}

} // namespace client
} // namespace celeborn
