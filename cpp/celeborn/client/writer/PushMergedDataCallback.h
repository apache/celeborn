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
#include "celeborn/client/writer/DataBatches.h"
#include "celeborn/client/writer/PushState.h"

namespace celeborn {
namespace client {

class ShuffleClientImpl;

class PushMergedDataCallback
    : public network::RpcResponseCallback,
      public std::enable_shared_from_this<PushMergedDataCallback> {
 public:
  static std::shared_ptr<PushMergedDataCallback> create(
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
      int remainingReviveTimes);

  void onSuccess(std::unique_ptr<memory::ReadOnlyByteBuffer> response) override;

  void onFailure(std::unique_ptr<std::exception> exception) override;

 private:
  PushMergedDataCallback(
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
      int remainingReviveTimes);

  const int shuffleId_;
  const int mapId_;
  const int attemptId_;
  const int numMappers_;
  const int numPartitions_;
  const std::string mapKey_;
  const std::string hostAndPushPort_;
  const int groupedBatchId_;
  std::vector<DataBatch> batches_;
  const std::vector<int> partitionIds_;
  const std::shared_ptr<PushState> pushState_;
  const std::weak_ptr<ShuffleClientImpl> weakClient_;
  int remainingReviveTimes_;
};

} // namespace client
} // namespace celeborn
