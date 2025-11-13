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

#include "celeborn/client/writer/ReviveManager.h"

namespace celeborn {
namespace client {

folly::FunctionScheduler ReviveManager::globalExecutor_ =
    folly::FunctionScheduler();

std::shared_ptr<ReviveManager> ReviveManager::create(
    const std::string& name,
    const conf::CelebornConf& conf,
    std::weak_ptr<ShuffleClientImpl> weakClient) {
  auto reviveManager =
      std::shared_ptr<ReviveManager>(new ReviveManager(name, conf, weakClient));
  reviveManager->start();
  return std::move(reviveManager);
}

ReviveManager::ReviveManager(
    const std::string& name,
    const conf::CelebornConf& conf,
    std::weak_ptr<ShuffleClientImpl> weakClient)
    : name_(name),
      batchSize_(conf.clientPushReviveBatchSize()),
      interval_(conf.clientPushReviveInterval()),
      weakClient_(weakClient) {}

ReviveManager::~ReviveManager() {
  globalExecutor_.cancelFunction(name_);
}

void ReviveManager::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return;
  }
  globalExecutor_.start();
  auto task = [weak_this = weak_from_this(), batchSize = batchSize_]() {
    try {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      bool continueFlag = true;
      do {
        std::unordered_map<
            int,
            std::unique_ptr<std::unordered_set<PtrReviveRequest>>>
            shuffleMap;
        std::vector<PtrReviveRequest> requests;
        shared_this->requestQueue_.withLock([&](auto& queue) {
          for (int i = 0; i < batchSize && !queue.empty(); i++) {
            requests.push_back(queue.front());
            queue.pop();
          }
        });
        if (requests.empty()) {
          break;
        }
        for (auto& request : requests) {
          auto& set = shuffleMap[request->shuffleId];
          if (!set) {
            set = std::make_unique<std::unordered_set<PtrReviveRequest>>();
          }
          set->insert(request);
        }
        auto shuffleClient = shared_this->weakClient_.lock();
        if (!shuffleClient) {
          return;
        }
        for (auto& [shuffleId, requestSet] : shuffleMap) {
          std::unordered_set<int> mapIds;
          std::vector<PtrReviveRequest> filteredRequests;
          std::unordered_map<int, PtrReviveRequest> requestsToSend;

          auto locationMapOptional =
              shuffleClient->getPartitionLocationMap(shuffleId);
          CELEBORN_CHECK(locationMapOptional.has_value());
          auto locationMap = locationMapOptional.value();
          for (auto& request : *requestSet) {
            if (shuffleClient->newerPartitionLocationExists(
                    locationMap, request->partitionId, request->epoch) ||
                shuffleClient->mapperEnded(shuffleId, request->mapId)) {
              request->reviveStatus = protocol::StatusCode::SUCCESS;
            } else {
              filteredRequests.push_back(request);
              mapIds.insert(request->mapId);
              if (auto iter = requestsToSend.find(request->partitionId);
                  iter == requestsToSend.end() ||
                  iter->second->epoch < request->epoch) {
                requestsToSend[request->partitionId] = request;
              }
            }
          }

          if (requestsToSend.empty()) {
            continue;
          }
          if (auto resultOptional =
                  shuffleClient->reviveBatch(shuffleId, mapIds, requestsToSend);
              resultOptional.has_value()) {
            auto result = resultOptional.value();
            for (auto& request : filteredRequests) {
              if (shuffleClient->mapperEnded(shuffleId, request->mapId)) {
                request->reviveStatus = protocol::StatusCode::SUCCESS;
              } else {
                request->reviveStatus = result[request->partitionId];
              }
            }
          } else {
            for (auto& request : filteredRequests) {
              request->reviveStatus = protocol::StatusCode::REVIVE_FAILED;
            }
          }
        }
        continueFlag =
            (shared_this->requestQueue_.lock()->size() > batchSize / 2);
      } while (continueFlag);
    } catch (std::exception& e) {
      LOG(ERROR) << "ReviveManager error occurred: " << e.what();
    }
  };
  startFunction(task);
}

void ReviveManager::startFunction(std::function<void()> task) {
  try {
    globalExecutor_.addFunction(task, interval_, name_, interval_);
  } catch (std::exception& e) {
    LOG(ERROR) << "startFunction failed, current function name " << name_
               << ", retry again...";
    name_ += "-";
    name_ += std::to_string(rand() % 10000);
    startFunction(task);
  }
}

void ReviveManager::addRequest(PtrReviveRequest request) {
  requestQueue_.withLock([&](auto& queue) { queue.push(std::move(request)); });
}

} // namespace client
} // namespace celeborn
