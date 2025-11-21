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

#pragma once

#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/client/writer/PushDataCallback.h"
#include "celeborn/client/writer/PushState.h"
#include "celeborn/client/writer/ReviveManager.h"
#include "celeborn/network/NettyRpcEndpointRef.h"

namespace celeborn {
namespace client {
class ShuffleClient {
 public:
  virtual void setupLifecycleManagerRef(std::string& host, int port) = 0;

  virtual void setupLifecycleManagerRef(
      std::shared_ptr<network::NettyRpcEndpointRef>& lifecycleManagerRef) = 0;

  virtual void updateReducerFileGroup(int shuffleId) = 0;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) = 0;

  virtual std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) = 0;

  virtual bool cleanupShuffle(int shuffleId) = 0;

  virtual void shutdown() = 0;
};

class ReviveManager;
class PushDataCallback;

class ShuffleClientImpl
    : public ShuffleClient,
      public std::enable_shared_from_this<ShuffleClientImpl> {
 public:
  friend class ReviveManager;
  friend class PushDataCallback;

  using PtrReviveRequest = std::shared_ptr<protocol::ReviveRequest>;
  using PartitionLocationMap = utils::ConcurrentHashMap<
      int,
      std::shared_ptr<const protocol::PartitionLocation>>;
  using PtrPartitionLocationMap = std::shared_ptr<PartitionLocationMap>;

  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<ShuffleClientImpl> create(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::shared_ptr<network::TransportClientFactory>& clientFactory);

  void setupLifecycleManagerRef(std::string& host, int port) override;

  void setupLifecycleManagerRef(std::shared_ptr<network::NettyRpcEndpointRef>&
                                    lifecycleManagerRef) override;

  std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex) override;

  std::unique_ptr<CelebornInputStream> readPartition(
      int shuffleId,
      int partitionId,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression) override;

  void updateReducerFileGroup(int shuffleId) override;

  bool cleanupShuffle(int shuffleId) override;

  void shutdown() override {}

 protected:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  ShuffleClientImpl(
      const std::string& appUniqueId,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::shared_ptr<network::TransportClientFactory>& clientFactory);

  // TODO: currently this function serves as a stub. will be updated in future
  //  commits.
  virtual void submitRetryPushData(
      int shuffleId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body,
      int batchId,
      std::shared_ptr<PushDataCallback> pushDataCallback,
      std::shared_ptr<PushState> pushState,
      PtrReviveRequest request,
      int remainReviveTimes,
      long dueTimeMs) {}

  // TODO: currently this function serves as a stub. will be updated in future
  //  commits.
  virtual bool mapperEnded(int shuffleId, int mapId) {
    return true;
  }

  // TODO: currently this function serves as a stub. will be updated in future
  //  commits.
  virtual void addRequestToReviveManager(
      std::shared_ptr<protocol::ReviveRequest> reviveRequest) {}

  // TODO: currently this function serves as a stub. will be updated in future
  //  commits.
  virtual std::optional<std::unordered_map<int, int>> reviveBatch(
      int shuffleId,
      const std::unordered_set<int>& mapIds,
      const std::unordered_map<int, PtrReviveRequest>& requests) {
    return std::nullopt;
  }

  virtual std::optional<PtrPartitionLocationMap> getPartitionLocationMap(
      int shuffleId) {
    return partitionLocationMaps_.get(shuffleId);
  }

  virtual utils::
      ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>&
      mapperEndSets() {
    return mapperEndSets_;
  }

  virtual void addPushDataRetryTask(folly::Func&& task) {
    pushDataRetryPool_->add(std::move(task));
  }

 private:
  // TODO: no support for WAIT as it is not used.
  static bool newerPartitionLocationExists(
      std::shared_ptr<utils::ConcurrentHashMap<
          int,
          std::shared_ptr<const protocol::PartitionLocation>>> locationMap,
      int partitionId,
      int epoch);

  std::shared_ptr<protocol::GetReducerFileGroupResponse>
  getReducerFileGroupInfo(int shuffleId);

  const std::string appUniqueId_;
  std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<network::NettyRpcEndpointRef> lifecycleManagerRef_;
  std::shared_ptr<network::TransportClientFactory> clientFactory_;
  std::shared_ptr<folly::IOExecutor> pushDataRetryPool_;
  std::shared_ptr<ReviveManager> reviveManager_;
  std::mutex mutex_;
  utils::ConcurrentHashMap<
      int,
      std::shared_ptr<protocol::GetReducerFileGroupResponse>>
      reducerFileGroupInfos_;
  utils::ConcurrentHashMap<int, PtrPartitionLocationMap> partitionLocationMaps_;
  utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>
      mapperEndSets_;
};
} // namespace client
} // namespace celeborn
