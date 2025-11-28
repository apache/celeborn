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

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/writer/PushState.h"

namespace celeborn {
namespace client {

class ShuffleClientImpl;

class PushDataCallback : public network::RpcResponseCallback,
                         public std::enable_shared_from_this<PushDataCallback> {
 public:
  // Only allow construction from create() method to ensure that functionality
  // of std::shared_from_this works properly.
  static std::shared_ptr<PushDataCallback> create(
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
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

  void onSuccess(std::unique_ptr<memory::ReadOnlyByteBuffer> response) override;

  void onFailure(std::unique_ptr<std::exception> exception) override;

  // The location of a PushDataCallback might be updated if a revive is
  // involved, and the location must be updated by calling
  // updateLatestLocation() to make sure the location is properly updated.
  void updateLatestLocation(
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

 private:
  // The constructor is hidden to ensure that functionality of
  // std::shared_from_this works properly.
  PushDataCallback(
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
      std::shared_ptr<const protocol::PartitionLocation> latestLocation);

  void reviveAndRetryPushData(
      ShuffleClientImpl& shuffleClient,
      protocol::StatusCode cause);

  const int shuffleId_;
  const int mapId_;
  const int attemptId_;
  const int partitionId_;
  const int numMappers_;
  const int numPartitions_;
  const std::string mapKey_;
  const int batchId_;
  const std::unique_ptr<memory::ReadOnlyByteBuffer> databody_;
  const std::shared_ptr<PushState> pushState_;
  const std::weak_ptr<ShuffleClientImpl> weakClient_;
  int remainingReviveTimes_;
  std::shared_ptr<const protocol::PartitionLocation> latestLocation_;
};

} // namespace client
} // namespace celeborn
