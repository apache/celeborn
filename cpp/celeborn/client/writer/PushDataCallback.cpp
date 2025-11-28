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
      databody_(std::move(databody)),
      pushState_(pushState),
      weakClient_(weakClient),
      remainingReviveTimes_(remainingReviveTimes),
      latestLocation_(latestLocation) {}

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
    pushState_->onSuccess(latestLocation_->hostAndPushPort());
    pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
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
      pushState_->onSuccess(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    case protocol::StatusCode::HARD_SPLIT: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " hard split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      reviveAndRetryPushData(*sharedClient, protocol::StatusCode::HARD_SPLIT);
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " primary congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      pushState_->onCongestControl(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push data to " << latestLocation_->hostAndPushPort()
              << " replicate congestion required for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_ << " partition "
              << partitionId_ << " batch " << batchId_ << ".";
      pushState_->onCongestControl(latestLocation_->hostAndPushPort());
      pushState_->removeBatch(batchId_, latestLocation_->hostAndPushPort());
      break;
    }
    default: {
      // This is treated as success.
      LOG(WARNING) << "unhandled PushData success protocol::StatusCode: "
                   << reason;
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
  if (pushState_->exceptionExists()) {
    return;
  }

  LOG(ERROR) << "Push data to " << latestLocation_->hostAndPushPort()
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " partition " << partitionId_
             << " batch " << batchId_ << ", remain revive times "
             << remainingReviveTimes_;

  if (remainingReviveTimes_ <= 0) {
    // TODO: set more specific exception.
    pushState_->setException(std::move(exception));
    return;
  }

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
  // TODO: we use PRIMARY exception as the dummy value here, but the cause
  //  should be extracted from error msg. Especially, the cause should tell if
  //  the exception is from PRIMARY or REPLICATE.
  protocol::StatusCode cause =
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;
  reviveAndRetryPushData(*sharedClient, cause);
}

void PushDataCallback::updateLatestLocation(
    std::shared_ptr<const protocol::PartitionLocation> latestLocation) {
  pushState_->addBatch(batchId_, latestLocation->hostAndPushPort());
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
