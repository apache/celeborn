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
      maxInFlightReqsTotal_(conf.clientPushMaxReqsInFlightTotal()),
      maxInFlightBytesSizeEnabled_(
          conf.clientPushMaxBytesSizeInFlightEnabled()),
      maxInFlightBytesSizeTotal_(conf.clientPushMaxBytesSizeInFlightTotal()),
      maxInFlightBytesSizePerWorker_(
          conf.clientPushMaxBytesSizeInFlightPerWorker()) {
  if (maxInFlightBytesSizeEnabled_) {
    inflightBytesSizePerAddress_.emplace();
    inflightBatchBytesSizes_.emplace();
  }
}

int PushState::nextBatchId() {
  return currBatchId_.fetch_add(1);
}

void PushState::addBatch(
    int batchId,
    int batchBytesSize,
    const std::string& hostAndPushPort) {
  auto batchIdSet = inflightBatchesPerAddress_.computeIfAbsent(
      hostAndPushPort,
      [&]() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
  batchIdSet->insert(batchId);
  totalInflightReqs_.fetch_add(1);

  if (maxInFlightBytesSizeEnabled_) {
    auto bytesSizePerAddress = inflightBytesSizePerAddress_->computeIfAbsent(
        hostAndPushPort,
        [&]() { return std::make_shared<std::atomic<long>>(0); });
    bytesSizePerAddress->fetch_add(batchBytesSize);
    inflightBatchBytesSizes_->set(batchId, batchBytesSize);
    totalInflightBytes_.fetch_add(batchBytesSize);
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
    batchIdSet->erase(batchId);
  } else {
    LOG(WARNING) << "BatchIdSet of " << hostAndPushPort << " doesn't exist.";
  }

  totalInflightReqs_.fetch_sub(1);

  if (maxInFlightBytesSizeEnabled_) {
    auto inflightBatchBytesSize = inflightBatchBytesSizes_->get(batchId);
    inflightBatchBytesSizes_->erase(batchId);
    if (inflightBatchBytesSize.has_value()) {
      auto inflightBytesSize =
          inflightBytesSizePerAddress_->get(hostAndPushPort);
      if (inflightBytesSize.has_value()) {
        inflightBytesSize.value()->fetch_sub(inflightBatchBytesSize.value());
      }
      totalInflightBytes_.fetch_sub(inflightBatchBytesSize.value());
    }
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
  std::shared_ptr<std::atomic<long>> batchBytesSize = nullptr;
  if (maxInFlightBytesSizeEnabled_) {
    batchBytesSize = inflightBytesSizePerAddress_->computeIfAbsent(
        hostAndPushPort,
        [&]() { return std::make_shared<std::atomic<long>>(0); });
  }
  long times = waitInflightTimeoutMs_ / deltaMs_;
  for (; times > 0; times--) {
    if (cleaned_) {
      return false;
    }

    bool reqCountWithinLimits =
        (totalInflightReqs_ <= maxInFlightReqsTotal_ &&
         static_cast<int>(batchIdSet->size()) <= currentMaxReqsInFlight);
    bool byteSizeWithinLimits = false;

    if (maxInFlightBytesSizeEnabled_ && batchBytesSize) {
      byteSizeWithinLimits =
          (totalInflightBytes_.load() <= maxInFlightBytesSizeTotal_ &&
           batchBytesSize->load() <= maxInFlightBytesSizePerWorker_);
    }

    if (reqCountWithinLimits ||
        (maxInFlightBytesSizeEnabled_ && byteSizeWithinLimits)) {
      break;
    }

    throwIfExceptionExists();
    std::this_thread::sleep_for(utils::MS(deltaMs_));
  }

  if (times <= 0) {
    if (totalInflightReqs_ > maxInFlightReqsTotal_ ||
        static_cast<int>(batchIdSet->size()) > currentMaxReqsInFlight) {
      LOG(WARNING) << "After waiting for " << waitInflightTimeoutMs_
                   << " ms, there are still " << totalInflightReqs_
                   << " requests in flight (limit: " << maxInFlightReqsTotal_
                   << "): " << batchIdSet->size()
                   << " batches in flight for hostAndPushPort "
                   << hostAndPushPort << ", which exceeds the current limit "
                   << currentMaxReqsInFlight;
    }
    if (maxInFlightBytesSizeEnabled_ && batchBytesSize) {
      if (totalInflightBytes_.load() > maxInFlightBytesSizeTotal_ ||
          batchBytesSize->load() > maxInFlightBytesSizePerWorker_) {
        LOG(WARNING) << "After waiting for " << waitInflightTimeoutMs_
                     << " ms, there are still " << totalInflightBytes_.load()
                     << " bytes in flight (limit: "
                     << maxInFlightBytesSizeTotal_
                     << "): " << batchBytesSize->load()
                     << " bytes for hostAndPushPort " << hostAndPushPort
                     << ", which exceeds the current limit "
                     << maxInFlightBytesSizePerWorker_;
      }
    }
  }
  throwIfExceptionExists();
  return times <= 0;
}

bool PushState::limitZeroInFlight() {
  throwIfExceptionExists();

  long times = waitInflightTimeoutMs_ / deltaMs_;
  for (; times > 0; times--) {
    if (cleaned_) {
      return false;
    }
    if (totalInflightReqs_ <= 0) {
      break;
    }
    throwIfExceptionExists();
    std::this_thread::sleep_for(utils::MS(deltaMs_));
  }

  if (times <= 0) {
    std::string addressInfos;
    inflightBatchesPerAddress_.forEach(
        [&](const std::string& address,
            const std::shared_ptr<utils::ConcurrentHashSet<int>>&
                inflightBatches) {
          if (inflightBatches->size() <= 0) {
            return;
          }
          if (!addressInfos.empty()) {
            addressInfos += ", ";
          }
          addressInfos += fmt::format(
              "{} batches for hostAndPushPort {}",
              inflightBatches->size(),
              address);
        });
    LOG(ERROR) << "After waiting for " << waitInflightTimeoutMs_
               << " ms, there are still " << totalInflightReqs_
               << " in flight: [" << addressInfos
               << "] which exceeds the current limit 0.";
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
  LOG(INFO) << "Cleanup " << totalInflightReqs_.load()
            << " requests in flight.";
  cleaned_ = true;
  inflightBatchesPerAddress_.clear();
  totalInflightReqs_ = 0;
  pushStrategy_->clear();

  if (maxInFlightBytesSizeEnabled_) {
    LOG(INFO) << "Cleanup " << totalInflightBytes_.load()
              << " bytes in flight.";
    inflightBytesSizePerAddress_->clear();
    inflightBatchBytesSizes_->clear();
    totalInflightBytes_ = 0;
  }
}

void PushState::throwIfExceptionExists() {
  auto exp = exception_.rlock();
  if (*exp) {
    CELEBORN_FAIL((*exp)->what());
  }
}

} // namespace client
} // namespace celeborn
