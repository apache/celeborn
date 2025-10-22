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

#include "celeborn/client/writer/PushState.h"

namespace celeborn {
namespace client {

PushState::PushState(const conf::CelebornConf& conf)
    : waitInflightTimeoutMs_(conf.clientPushLimitInFlightTimeoutMs()),
      deltaMs_(conf.clientPushLimitInFlightSleepDeltaMs()),
      pushStrategy_(PushStrategy::create(conf)),
      maxInFlightReqsTotal_(conf.clientPushMaxReqsInFlightTotal()) {}

int PushState::nextBatchId() {
  return currBatchId_.fetch_add(1);
}

void PushState::addBatch(int batchId, const std::string& hostAndPushPort) {
  auto batchIdSet = inflightBatchesPerAddress_.computeIfAbsent(
      hostAndPushPort,
      [&]() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
  if (batchIdSet->insert(batchId)) {
    totalInflightReqs_.fetch_add(1);
  } else {
    VLOG(1) << "BatchIdSet already has batchId " << batchId << ", ignored";
  }
}

void PushState::onSuccess(const std::string& hostAndPushPort) {
  pushStrategy_->onSuccess(hostAndPushPort);
}

void PushState::onCongestControl(const std::string& hostAndPushPort) {
  pushStrategy_->onCongestControl(hostAndPushPort);
}

void PushState::removeBatch(int batchId, const std::string& hostAndPushPort) {
  auto batchIdSetOptional = inflightBatchesPerAddress_.get(hostAndPushPort);
  if (batchIdSetOptional.has_value()) {
    auto batchIdSet = batchIdSetOptional.value();
    if (batchIdSet->erase(batchId)) {
      totalInflightReqs_.fetch_sub(1);
    } else {
      VLOG(1) << "BatchIdSet has already removed batchId " << batchId
              << ", ignored";
    }
  } else {
    LOG(WARNING) << "BatchIdSet of " << hostAndPushPort << " doesn't exist.";
  }
}

bool PushState::limitMaxInFlight(const std::string& hostAndPushPort) {
  throwIfExceptionExists();

  pushStrategy_->limitPushSpeed(*this, hostAndPushPort);
  int currentMaxReqsInFlight =
      pushStrategy_->getCurrentMaxReqsInFlight(hostAndPushPort);

  auto batchIdSet = inflightBatchesPerAddress_.computeIfAbsent(
      hostAndPushPort,
      [&]() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
  long times = waitInflightTimeoutMs_ / deltaMs_;
  for (; times > 0; times--) {
    if (totalInflightReqs_ <= maxInFlightReqsTotal_ &&
        batchIdSet->size() <= currentMaxReqsInFlight) {
      break;
    }
    throwIfExceptionExists();
    std::this_thread::sleep_for(utils::MS(deltaMs_));
  }

  if (times <= 0) {
    LOG(WARNING) << "After waiting for " << waitInflightTimeoutMs_
                 << " ms, there are still " << batchIdSet->size()
                 << " batches in flight for hostAndPushPort " << hostAndPushPort
                 << ", which exceeds the current limit "
                 << currentMaxReqsInFlight;
  }
  throwIfExceptionExists();
  return times <= 0;
}

bool PushState::limitZeroInFlight() {
  throwIfExceptionExists();

  long times = waitInflightTimeoutMs_ / deltaMs_;
  for (; times > 0; times--) {
    if (totalInflightReqs_ <= 0) {
      break;
    }
    throwIfExceptionExists();
    std::this_thread::sleep_for(utils::MS(deltaMs_));
  }

  if (times <= 0) {
    // TODO: log with more detailed address msg.
    LOG(ERROR) << "After waiting for " << waitInflightTimeoutMs_
               << " ms, there are still " << totalInflightReqs_
               << " in flight  which exceeds the current limit 0.";
  }
  throwIfExceptionExists();
  return times <= 0;
}

bool PushState::exceptionExists() const {
  auto exp = exception_.rlock();
  return (bool)(*exp);
}

void PushState::setException(std::unique_ptr<std::exception> exception) {
  auto exp = exception_.wlock();
  if (!(*exp)) {
    *exp = std::move(exception);
  }
}

std::optional<std::string> PushState::getExceptionMsg() const {
  auto exp = exception_.rlock();
  if (*exp) {
    return (*exp)->what();
  }
  return std::nullopt;
}

void PushState::cleanup() {
  inflightBatchesPerAddress_.clear();
  totalInflightReqs_ = 0;
  pushStrategy_->clear();
}

void PushState::throwIfExceptionExists() {
  auto exp = exception_.rlock();
  if (*exp) {
    CELEBORN_FAIL((*exp)->what());
  }
}

} // namespace client
} // namespace celeborn
