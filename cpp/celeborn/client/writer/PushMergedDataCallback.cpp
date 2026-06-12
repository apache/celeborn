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
#include "celeborn/utils/Exceptions.h"

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

void PushMergedDataCallback::releaseInFlightBatch() {
  pushState_->onSuccess(hostAndPushPort_);
  pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
}

void PushMergedDataCallback::releaseInFlightBatchOnCongestion() {
  pushState_->onCongestControl(hostAndPushPort_);
  pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
}

void PushMergedDataCallback::maybeRecordResubmittedBatch(
    const ShuffleClientImpl& client,
    const DataBatch& batch) {
  if (client.dataPushFailureTrackingEnabled_ && client.pushReplicateEnabled_) {
    pushState_->recordFailedBatch(
        batch.loc->uniqueId(), mapId_, attemptId_, batch.batchId);
  }
}

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
      releaseInFlightBatch();
      break;
    }
    case protocol::StatusCode::HARD_SPLIT:
    case protocol::StatusCode::SOFT_SPLIT: {
      VLOG(1) << "Push merged data to " << hostAndPushPort_
              << " split required for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatch "
              << groupedBatchId_ << ".";

      if (response->remainingSize() > 0) {
        try {
          // Parse PbPushMergedDataSplitPartitionInfo from TransportMessage
          auto transportMsg = std::make_unique<protocol::TransportMessage>(
              response->readToReadOnlyBuffer(response->remainingSize()));
          PbPushMergedDataSplitPartitionInfo partitionInfo;
          if (!partitionInfo.ParseFromString(transportMsg->payload())) {
            pushState_->setException(std::make_unique<std::runtime_error>(
                "Failed to parse PbPushMergedDataSplitPartitionInfo"));
            return;
          }

          CELEBORN_CHECK_EQ(
              partitionInfo.statuscodes_size(),
              partitionInfo.splitpartitionindexes_size(),
              "Mismatched sizes: statuscodes {} vs splitpartitionindexes {}",
              partitionInfo.statuscodes_size(),
              partitionInfo.splitpartitionindexes_size());
          const int numBatches = static_cast<int>(batches_.size());
          for (int i = 0; i < partitionInfo.splitpartitionindexes_size(); i++) {
            int partitionIndex = partitionInfo.splitpartitionindexes(i);
            CELEBORN_CHECK_GE(partitionIndex, 0);
            CELEBORN_CHECK_LT(
                partitionIndex,
                numBatches,
                "Partition index {} out of range [0, {})",
                partitionIndex,
                numBatches);
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

          // Any partition that is not SOFT_SPLIT needs to be resubmitted
          // (mirrors Java's else branch), reviving with HARD_SPLIT.
          std::vector<DataBatch> batchesToRetry;
          std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
          for (int i = 0; i < partitionInfo.splitpartitionindexes_size(); i++) {
            int partitionIndex = partitionInfo.splitpartitionindexes(i);
            CELEBORN_DCHECK_GE(partitionIndex, 0);
            CELEBORN_DCHECK_LT(partitionIndex, numBatches);
            int statusCode = partitionInfo.statuscodes(i);
            if (statusCode !=
                static_cast<int>(protocol::StatusCode::SOFT_SPLIT)) {
              int partitionId = partitionIds_[partitionIndex];
              // Record before the batch is moved into batchesToRetry.
              maybeRecordResubmittedBatch(
                  *sharedClient, batches_[partitionIndex]);
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
                protocol::StatusCode::HARD_SPLIT,
                groupedBatchId_,
                pushState_,
                remainingReviveTimes_,
                dueTimeMs);
          } else {
            releaseInFlightBatch();
          }
        } catch (const std::exception& e) {
          // A malformed/incompatible response must not escape onSuccess and
          // leave the batch in flight; fail terminally (like the Java client).
          pushState_->setException(std::make_unique<std::runtime_error>(
              std::string("parse pushMergedData response failed: ") +
              e.what()));
          return;
        }
      } else {
        // Old worker without per-partition split info: resubmit ALL batches,
        // reviving with HARD_SPLIT (like the Java client) whether the bare
        // status byte was SOFT_SPLIT or HARD_SPLIT.
        std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests;
        for (size_t i = 0; i < batches_.size(); i++) {
          maybeRecordResubmittedBatch(*sharedClient, batches_[i]);
          auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
              shuffleId_,
              mapId_,
              attemptId_,
              partitionIds_[i],
              batches_[i].loc->epoch,
              batches_[i].loc,
              protocol::StatusCode::HARD_SPLIT);
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
            protocol::StatusCode::HARD_SPLIT,
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
      releaseInFlightBatchOnCongestion();
      break;
    }
    case protocol::StatusCode::PUSH_DATA_SUCCESS_REPLICA_CONGESTED: {
      VLOG(1) << "Push merged data to " << hostAndPushPort_
              << " replicate congestion for shuffle " << shuffleId_ << " map "
              << mapId_ << " attempt " << attemptId_ << " groupedBatch "
              << groupedBatchId_ << ".";
      releaseInFlightBatchOnCongestion();
      break;
    }
    case protocol::StatusCode::SUCCESS: {
      releaseInFlightBatch();
      break;
    }
    default: {
      LOG(WARNING) << "unhandled PushMergedData success StatusCode: " << reason;
      releaseInFlightBatch();
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
  const std::string errorMsg = exception ? exception->what() : "";

  // Record the failed batches for the adaptive skewed-partition read
  // optimization, like the Java client (regardless of cause when enabled).
  if (sharedClient->dataPushFailureTrackingEnabled_) {
    for (size_t i = 0; i < batches_.size(); i++) {
      pushState_->recordFailedBatch(
          batches_[i].loc->uniqueId(), mapId_, attemptId_, batches_[i].batchId);
    }
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
  LOG(ERROR) << "Push merged data to " << hostAndPushPort_
             << " failed for shuffle " << shuffleId_ << " map " << mapId_
             << " attempt " << attemptId_ << " groupedBatch " << groupedBatchId_
             << ", remain revive times " << remainingReviveTimes_
             << ", errorMsg " << errorMsg;

  if (sharedClient->mapperEnded(shuffleId_, mapId_)) {
    pushState_->removeBatch(groupedBatchId_, hostAndPushPort_);
    LOG(INFO) << "Push merged data to " << hostAndPushPort_
              << " failed but mapper already ended for shuffle " << shuffleId_
              << " map " << mapId_ << " attempt " << attemptId_
              << " groupedBatch " << groupedBatchId_ << ".";
    return;
  }

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
      cause,
      groupedBatchId_,
      pushState_,
      remainingReviveTimes_ - 1,
      dueTimeMs);
}

} // namespace client
} // namespace celeborn