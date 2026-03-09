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

#include "celeborn/client/writer/PushMergedDataCallback.h"
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/protocol/TransportMessage.h"

namespace celeborn {
namespace client {

std::shared_ptr<PushMergedDataCallback> PushMergedDataCallback::create(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    const std::string& hostAndPushPort,
    int groupedBatchId,
    std::vector<DataBatch> batches,
    std::vector<int> partitionIds,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes) {
  return std::shared_ptr<PushMergedDataCallback>(new PushMergedDataCallback(
      shuffleId,
      mapId,
      attemptId,
      numMappers,
      numPartitions,
      mapKey,
      hostAndPushPort,
      groupedBatchId,
      std::move(batches),
      std::move(partitionIds),
      pushState,
      weakClient,
      remainingReviveTimes));
}

PushMergedDataCallback::PushMergedDataCallback(
    int shuffleId,
    int mapId,
    int attemptId,
    int numMappers,
    int numPartitions,
    const std::string& mapKey,
    const std::string& hostAndPushPort,
    int groupedBatchId,
    std::vector<DataBatch> batches,
    std::vector<int> partitionIds,
    std::shared_ptr<PushState> pushState,
    std::weak_ptr<ShuffleClientImpl> weakClient,
    int remainingReviveTimes)
    : shuffleId_(shuffleId),
      mapId_(mapId),
      attemptId_(attemptId),
      numMappers_(numMappers),
      numPartitions_(numPartitions),
      mapKey_(mapKey),
      hostAndPushPort_(hostAndPushPort),
      groupedBatchId_(groupedBatchId),
      batches_(std::move(batches)),
      partitionIds_(std::move(partitionIds)),
      pushState_(pushState),
      weakClient_(weakClient),
      remainingReviveTimes_(remainingReviveTimes) {}

void PushMergedDataCallback::onSuccess(
    std::unique_ptr<memory::ReadOnlyByteBuffer> response) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushMergedDataCallbackOnSuccess, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " groupedBatch " << groupedBatchId_ << ".";
    return;
  }

  if (response->remainingSize() <= 0) {
    pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
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
      pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
      break;
    }
    case protocol::StatusCode::HARD_SPLIT:
    case protocol::StatusCode::SOFT_SPLIT: {
      VLOG(1) << "Push merged data to " << hostAndPushPort_
              << " split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatch "
              << groupedBatchId_ << ".";

      if (response->remainingSize() > 0) {
        // Parse PbPushMergedDataSplitPartitionInfo from TransportMessage
        auto transportMsg = std::make_unique<protocol::TransportMessage>(
            response->readToReadOnlyBuffer(response->remainingSize()));
        PbPushMergedDataSplitPartitionInfo partitionInfo;
        if (!partitionInfo.ParseFromString(transportMsg->payload())) {
          pushState_->setException(std::make_unique<std::runtime_error>(
              "Failed to parse PbPushMergedDataSplitPartitionInfo"));
          return;
        }

        for (int i = 0; i < partitionInfo.splitpartitionindexes_size(); i++) {
          int partitionIndex = partitionInfo.splitpartitionindexes(i);
          int statusCode = partitionInfo.statuscodes(i);

          if (statusCode ==
              static_cast<int>(protocol::StatusCode::SOFT_SPLIT)) {
            int partitionId = partitionIds_[partitionIndex];
            if (!ShuffleClientImpl::newerPartitionLocationExists(
                    sharedClient->getPartitionLocationMap(shuffleId_).value(),
                    partitionId,
                    batches_[partitionIndex].loc->epoch)) {
              auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
                  shuffleId_,
                  mapId_,
                  attemptId_,
                  partitionId,
                  batches_[partitionIndex].loc->epoch,
                  batches_[partitionIndex].loc,
                  protocol::StatusCode::SOFT_SPLIT);
              sharedClient->addRequestToReviveManager(reviveRequest);
            }
          }
        }

        // For any HARD_SPLIT partitions, need to resubmit
        std::vector<DataBatch> batchesToRetry;
        std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
        for (int i = 0; i < partitionInfo.splitpartitionindexes_size(); i++) {
          int partitionIndex = partitionInfo.splitpartitionindexes(i);
          int statusCode = partitionInfo.statuscodes(i);
          if (statusCode ==
              static_cast<int>(protocol::StatusCode::HARD_SPLIT)) {
            int partitionId = partitionIds_[partitionIndex];
            auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
                shuffleId_,
                mapId_,
                attemptId_,
                partitionId,
                batches_[partitionIndex].loc->epoch,
                batches_[partitionIndex].loc,
                protocol::StatusCode::HARD_SPLIT);
            sharedClient->addRequestToReviveManager(reviveRequest);
            reviveRequests.push_back(reviveRequest);
            batchesToRetry.push_back(std::move(batches_[partitionIndex]));
          }
        }

        if (!batchesToRetry.empty()) {
          long dueTimeMs = utils::currentTimeMillis() +
              sharedClient->conf_
                      ->clientRpcRequestPartitionLocationRpcAskTimeout() /
                  utils::MS(1);
          sharedClient->submitRetryPushMergedData(
              shuffleId_,
              mapId_,
              attemptId_,
              numMappers_,
              numPartitions_,
              mapKey_,
              std::move(batchesToRetry),
              std::move(reviveRequests),
              groupedBatchId_,
              pushState_,
              remainingReviveTimes_,
              dueTimeMs);
        } else {
          pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
        }
      } else {
        // Old worker without per-partition split info: revive all batches
        std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
        for (size_t i = 0; i < batches_.size(); i++) {
          auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
              shuffleId_,
              mapId_,
              attemptId_,
              partitionIds_[i],
              batches_[i].loc->epoch,
              batches_[i].loc,
              reason);
          sharedClient->addRequestToReviveManager(reviveRequest);
          reviveRequests.push_back(reviveRequest);
        }

        long dueTimeMs = utils::currentTimeMillis() +
            sharedClient->conf_
                    ->clientRpcRequestPartitionLocationRpcAskTimeout() /
                utils::MS(1);
        sharedClient->submitRetryPushMergedData(
            shuffleId_,
            mapId_,
            attemptId_,
            numMappers_,
            numPartitions_,
            mapKey_,
            std::move(batches_),
            std::move(reviveRequests),
            groupedBatchId_,
            pushState_,
            remainingReviveTimes_,
            dueTimeMs);
      }
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: {
      VLOG(1) << "Push merged data to " << hostAndPushPort_
              << " primary congestion for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatch "
              << groupedBatchId_ << ".";
      pushState_->onCongestControl(hostAndPushPort_);
      pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push merged data to " << hostAndPushPort_
              << " replicate congestion for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatch "
              << groupedBatchId_ << ".";
      pushState_->onCongestControl(hostAndPushPort_);
      pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
      break;
    }
    default: {
      LOG(WARNING) << "unhandled PushMergedData success StatusCode: " << reason;
      pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
    }
  }
}

void PushMergedDataCallback::onFailure(
    std::unique_ptr<std::exception> exception) {
  auto sharedClient = weakClient_.lock();
  if (!sharedClient) {
    LOG(WARNING) << "ShuffleClientImpl has expired when "
                    "PushMergedDataCallbackOnFailure, ignored, shuffle "
                 << shuffleId_ << " map " << mapId_ << " attempt " << attemptId_
                 << " groupedBatch " << groupedBatchId_ << ".";
    return;
  }
  if (pushState_->exceptionExists()) {
    return;
  }

  LOG(ERROR) << "Push merged data to " << hostAndPushPort_
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " groupedBatch " << groupedBatchId_
             << ", remain revive times " << remainingReviveTimes_;

  if (remainingReviveTimes_ <= 0) {
    pushState_->setException(std::move(exception));
    return;
  }

  if (sharedClient->mapperEnded(shuffleId_, mapId_)) {
    pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
    LOG(INFO) << "Push merged data to " << hostAndPushPort_
              << " failed but mapper already ended for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_
              << " groupedBatch " << groupedBatchId_ << ".";
    return;
  }

  protocol::StatusCode cause =
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY;

  std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
  for (size_t i = 0; i < batches_.size(); i++) {
    auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
        shuffleId_,
        mapId_,
        attemptId_,
        partitionIds_[i],
        batches_[i].loc->epoch,
        batches_[i].loc,
        cause);
    sharedClient->addRequestToReviveManager(reviveRequest);
    reviveRequests.push_back(reviveRequest);
  }

  long dueTimeMs = utils::currentTimeMillis() +
      sharedClient->conf_->clientRpcRequestPartitionLocationRpcAskTimeout() /
          utils::MS(1);
  sharedClient->submitRetryPushMergedData(
      shuffleId_,
      mapId_,
      attemptId_,
      numMappers_,
      numPartitions_,
      mapKey_,
      std::move(batches_),
      std::move(reviveRequests),
      groupedBatchId_,
      pushState_,
      remainingReviveTimes_ - 1,
      dueTimeMs);
}

} // namespace client
} // namespace celeborn