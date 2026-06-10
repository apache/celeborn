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

#include "celeborn/client/writer/PushDataCallback.h"
#include "celeborn/conf/CelebornConf.h"

namespace celeborn {
namespace client {

std::shared_ptr<PushDataCallback> PushDataCallback::create(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> databody,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    std::shared_ptr<const protocol::PartitionLocation> latestLocation) {
  return std::shared_ptr<PushDataCallback>(new PushDataCallback(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      numMappers,
      numPartitions,
      mapKey,
      batchId,
      std::move(databody),
      pushState,
      weakClient,
      remainingReviveTimes,
      latestLocation));
}

PushDataCallback::PushDataCallback(
    int shuffleId,
    int mapId,
    int attemptId,
    int partitionId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> databody,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes,
    std::shared_ptr<const protocol::PartitionLocation> latestLocation)
    : shuffleId_(shuffleId),
      mapId_(mapId),
      attemptId_(attemptId),
      partitionId_(partitionId),
      numMappers_(numMappers),
      numPartitions_(numPartitions),
      mapKey_(mapKey),
      batchId_(batchId),
      batchBytesSize_(databody ? static_cast<int>(databody->size()) : 0),
      databody_(std::move(databody)),
      pushState_(pushState),
      weakClient_(weakClient),
      remainingReviveTimes_(remainingReviveTimes),
      latestLocation_(latestLocation) {}

void PushDataCallback::releaseInFlightBatch() {
  const auto hostAndPushPort = latestLocation_->hostAndPushPort();
  pushState_->onSuccess(hostAndPushPort);
  pushState_->removeBatch(batchId_, hostAndPushPort);
}

void PushDataCallback::releaseInFlightBatchOnCongestion() {
  const auto hostAndPushPort = latestLocation_->hostAndPushPort();
  pushState_->onCongestControl(hostAndPushPort);
  pushState_->removeBatch(batchId_, hostAndPushPort);
}

void PushDataCallback::onSuccess(
    std::unique_ptr<memory::ReadOnlyByteBuffer> response) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushDataCallbackOnSuccess, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " partition " << partitionId_ << " batch " << batchId_
                 << ".";
    return;
  }
  if (response->remainingSize() <= 0) {
    releaseInFlightBatch();
    return;
  }
  protocol::StatusCode reason =
      static_cast<protocol::StatusCode>(response->read<uint8_t>());
  switch (reason) {
    case protocol::StatusCode::MAP_ENDED: {
      auto mapperEndSet = sharedClient->mapperEndSets().computeIfAbsent(
          shuffleId_,
          []() { return std::make_shared<utils::ConcurrentHashSet<int>>(); });
      mapperEndSet->insert(mapId_);
      // The push succeeded, so release the in-flight batch too.
      releaseInFlightBatch();
      break;
    }
    case protocol::StatusCode::SOFT_SPLIT: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " soft split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      if (!ShuffleClientImpl::newerPartitionLocationExists(
              sharedClient->getPartitionLocationMap(shuffleId_).value(),
              partitionId_,
              latestLocation_->epoch)) {
        auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
            shuffleId_,
            mapId_,
            attemptId_,
            partitionId_,
            latestLocation_->epoch,
            latestLocation_,
            protocol::StatusCode::SOFT_SPLIT);
        sharedClient->addRequestToReviveManager(reviveRequest);
      }
      releaseInFlightBatch();
      break;
    }
    case protocol::StatusCode::HARD_SPLIT: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " hard split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      if (sharedClient->dataPushFailureTrackingEnabled_ &&
          sharedClient->pushReplicateEnabled_) {
        pushState_->recordFailedBatch(
            latestLocation_->uniqueId(), mapId_, attemptId_, batchId_);
      }
      reviveAndRetryPushData(*sharedClient, protocol::StatusCode::HARD_SPLIT);
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " primary congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      releaseInFlightBatchOnCongestion();
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " replicate congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      releaseInFlightBatchOnCongestion();
      break;
    }
    default: {
      // Treated as success (e.g. StageEnd), matching the Java client's final
      // else branch.
      LOG(WARNING) << "unhandled PushData success protocol::StatusCode: "
                   << reason;
      releaseInFlightBatch();
    }
  }
}

void PushDataCallback::onFailure(std::unique_ptr<std::exception> exception) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushDataCallbackOnFailure, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " partition " << partitionId_ << " batch " << batchId_
                 << ".";
    return;
  }
  const std::string errorMsg = exception ? exception->what() : "";

  // Record the failed batch for the adaptive skewed-partition read
  // optimization, like the Java client (regardless of cause when enabled).
  if (sharedClient->dataPushFailureTrackingEnabled_) {
    pushState_->recordFailedBatch(
        latestLocation_->uniqueId(), mapId_, attemptId_, batchId_);
  }

  if (pushState_->exceptionExists()) {
    return;
  }

  // Classify the failure; with no revive attempts left this sets the terminal
  // exception and stops here (like the Java client).
  const auto causeOpt = ShuffleClientImpl::classifyPushFailure(
      errorMsg, remainingReviveTimes_, *pushState_);
  if (!causeOpt) {
    return;
  }
  const protocol::StatusCode cause = *causeOpt;

  // Logged only on the retry path, like the Java client (suppressed once
  // revive attempts are exhausted).
  LOG(ERROR) << "Push data to " << latestLocation_->hostAndPushPort()
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " partition " << partitionId_
             << " batch " << batchId_ << ", remain revive times "
             << remainingReviveTimes_ << ", errorMsg " << errorMsg;

  if (sharedClient->mapperEnded(shuffleId_, mapId_)) {
    pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
    LOG(INFO) << "Push data to " << latestLocation_->hostAndPushPort()
              << " failed but mapper already ended for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_
              << ", remain revive times " << remainingReviveTimes_ << ".";
    return;
  }
  remainingReviveTimes_--;
  reviveAndRetryPushData(*sharedClient, cause);
}

void PushDataCallback::updateLatestLocation(
    std::shared_ptr<const protocol::PartitionLocation> latestLocation) {
  pushState_->addBatch(
      batchId_, batchBytesSize_, latestLocation->hostAndPushPort());
  pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
  latestLocation_ = latestLocation;
}

void PushDataCallback::reviveAndRetryPushData(
    ShuffleClientImpl& shuffleClient,
    protocol::StatusCode cause) {
  auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
      shuffleId_,
      mapId_,
      attemptId_,
      partitionId_,
      latestLocation_->epoch,
      latestLocation_,
      cause);
  VLOG(1) << "addRequest to reviveManager, shuffleId "
          << reviveRequest->shuffleId << " mapId " << reviveRequest->mapId
          << " attemptId " << reviveRequest->attemptId << " partitionId "
          << reviveRequest->partitionId << " batchId " << batchId_ << " epoch "
          << reviveRequest->epoch;
  shuffleClient.addRequestToReviveManager(reviveRequest);
  long dueTimeMs = utils::currentTimeMillis() +
      shuffleClient.conf_->clientRpcRequestPartitionLocationRpcAskTimeout() /
          utils::MS(1);
  shuffleClient.addPushDataRetryTask(
      [weakClient = this->weakClient_,
       shuffleId = this->shuffleId_,
       body = this->databody_->clone(),
       batchId = this->batchId_,
       callback = shared_from_this(),
       pushState = this->pushState_,
       reviveRequest,
       remainingReviveTimes = this->remainingReviveTimes_,
       dueTimeMs]() {
        auto sharedClient = weakClient.lock();
        if (!sharedClient) {
          LOG(WARNING) << "ShuffleClientImpl has expired when "
                          "PushDataFailureCallback, ignored, shuffleId "
                       << shuffleId;
          return;
        }
        sharedClient->submitRetryPushData(
            shuffleId,
            body->clone(),
            batchId,
            callback,
            pushState,
            reviveRequest,
            remainingReviveTimes,
            dueTimeMs);
      });
}

} // namespace client
} // namespace celeborn
