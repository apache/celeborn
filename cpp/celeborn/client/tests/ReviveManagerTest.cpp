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

#include <gtest/gtest.h>

#include "celeborn/client/writer/ReviveManager.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
class MockShuffleClient : public ShuffleClientImpl {
 public:
  friend class ReviveManager;

  static std::shared_ptr<MockShuffleClient> create() {
    return std::shared_ptr<MockShuffleClient>(new MockShuffleClient());
  }

  virtual ~MockShuffleClient() = default;

  bool mapperEnded(int shuffleId, int mapId) override {
    return onMapperEnded_(shuffleId, mapId);
  }

  void setOnMapperEnded(std::function<bool(int, int)>&& onMapperEnded) {
    onMapperEnded_ = onMapperEnded;
  }

  std::optional<std::unordered_map<int, int>> reviveBatch(
      int shuffleId,
      const std::unordered_set<int>& mapIds,
      const std::unordered_map<int, PtrReviveRequest>& requests) override {
    return onReviveBatch_(shuffleId, mapIds, requests);
  }

  void setOnReviveBatch(
      std::function<std::optional<std::unordered_map<int, int>>(
          int,
          const std::unordered_set<int>&,
          const std::unordered_map<int, PtrReviveRequest>&)>&& onReviveBatch) {
    onReviveBatch_ = onReviveBatch;
  }

  std::optional<PtrPartitionLocationMap> getPartitionLocationMap(
      int shuffleId) override {
    return onGetPartitionLocationMap_(shuffleId);
  }

  void setOnGetPartitionLocationMap(
      std::function<std::optional<PtrPartitionLocationMap>(int)>&&
          onGetPartitionLocationMap) {
    onGetPartitionLocationMap_ = onGetPartitionLocationMap;
  }

 private:
  MockShuffleClient()
      : ShuffleClientImpl(
            "mock",
            std::make_shared<conf::CelebornConf>(),
            nullptr) {}
  std::function<bool(int, int)> onMapperEnded_ = [](int, int) { return false; };
  std::function<std::optional<std::unordered_map<int, int>>(
      int,
      const std::unordered_set<int>&,
      const std::unordered_map<int, PtrReviveRequest>&)>
      onReviveBatch_ = [](int,
                          const std::unordered_set<int>&,
                          const std::unordered_map<int, PtrReviveRequest>&) {
        return std::nullopt;
      };
  std::function<std::optional<PtrPartitionLocationMap>(int)>
      onGetPartitionLocationMap_ =
          [](int) -> std::optional<PtrPartitionLocationMap> {
    return {std::make_shared<PartitionLocationMap>()};
  };
};
} // namespace

class ReviveManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    mockShuffleClient_ = MockShuffleClient::create();
    auto conf = conf::CelebornConf();
    conf.registerProperty(
        conf::CelebornConf::kClientPushReviveInterval,
        fmt::format("{}ms", pushReviveIntervalMs_));
    reviveManager_ = ReviveManager::create(
        "test", conf, mockShuffleClient_->weak_from_this());
  }

  std::shared_ptr<MockShuffleClient> mockShuffleClient_;
  std::shared_ptr<ReviveManager> reviveManager_;

  static constexpr int pushReviveIntervalMs_ = 100;
};

TEST_F(ReviveManagerTest, successOnMapperEnded) {
  int mapperEndedCalledTimes = 0;
  const int testShuffleId = 1000;
  const int testMapId = 1001;
  const int testAttemptId = 1002;
  const int testPartitionId = 1003;
  const int testEpoch = 1004;
  auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testEpoch,
      nullptr,
      static_cast<protocol::StatusCode>(0));
  mockShuffleClient_->setOnMapperEnded(
      [=, &mapperEndedCalledTimes](int shuffleId, int mapId) mutable {
        EXPECT_EQ(testShuffleId, shuffleId);
        EXPECT_EQ(testMapId, mapId);
        mapperEndedCalledTimes++;
        return true;
      });

  EXPECT_EQ(
      reviveRequest->reviveStatus, protocol::StatusCode::REVIVE_INITIALIZED);
  reviveManager_->addRequest(reviveRequest);
  std::this_thread::sleep_for(
      std::chrono::milliseconds(pushReviveIntervalMs_ * 3));

  EXPECT_GT(mapperEndedCalledTimes, 0);
  EXPECT_EQ(reviveRequest->reviveStatus, protocol::StatusCode::SUCCESS);
}

TEST_F(ReviveManagerTest, successOnReviveSuccess) {
  int reviveBatchCalledTimes = 0;
  const int testShuffleId = 1000;
  const int testMapId = 1001;
  const int testAttemptId = 1002;
  const int testPartitionId = 1003;
  const int testEpoch = 1004;
  auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testEpoch,
      nullptr,
      static_cast<protocol::StatusCode>(0));
  mockShuffleClient_->setOnReviveBatch(
      [=, &reviveBatchCalledTimes](
          int shuffleId,
          const std::unordered_set<int>& mapIds,
          const std::unordered_map<int, ShuffleClientImpl::PtrReviveRequest>&
              requestsToSend) mutable
      -> std::optional<std::unordered_map<int, int>> {
        EXPECT_EQ(testShuffleId, shuffleId);
        EXPECT_GT(mapIds.count(testMapId), 0);
        EXPECT_GT(requestsToSend.count(testPartitionId), 0);
        auto request = requestsToSend.find(testPartitionId)->second;
        EXPECT_EQ(request->shuffleId, testShuffleId);
        EXPECT_EQ(request->mapId, testMapId);
        EXPECT_EQ(request->attemptId, testAttemptId);
        EXPECT_EQ(request->partitionId, testPartitionId);
        EXPECT_EQ(request->epoch, testEpoch);

        std::unordered_map<int, int> result;
        result[request->partitionId] = protocol::StatusCode::SUCCESS;
        ++reviveBatchCalledTimes;
        return {result};
      });

  EXPECT_EQ(
      reviveRequest->reviveStatus, protocol::StatusCode::REVIVE_INITIALIZED);
  reviveManager_->addRequest(reviveRequest);
  std::this_thread::sleep_for(
      std::chrono::milliseconds(pushReviveIntervalMs_ * 3));

  EXPECT_GT(reviveBatchCalledTimes, 0);
  EXPECT_EQ(reviveRequest->reviveStatus, protocol::StatusCode::SUCCESS);
}

TEST_F(ReviveManagerTest, failureOnReviveFailure) {
  int reviveBatchCalledTimes = 0;
  const int testShuffleId = 1000;
  const int testMapId = 1001;
  const int testAttemptId = 1002;
  const int testPartitionId = 1003;
  const int testEpoch = 1004;
  auto reviveRequest = std::make_shared<protocol::ReviveRequest>(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testEpoch,
      nullptr,
      static_cast<protocol::StatusCode>(0));
  mockShuffleClient_->setOnReviveBatch(
      [=, &reviveBatchCalledTimes](
          int shuffleId,
          const std::unordered_set<int>& mapIds,
          const std::unordered_map<int, ShuffleClientImpl::PtrReviveRequest>&
              requestsToSend) mutable
      -> std::optional<std::unordered_map<int, int>> {
        EXPECT_EQ(testShuffleId, shuffleId);
        EXPECT_GT(mapIds.count(testMapId), 0);
        EXPECT_GT(requestsToSend.count(testPartitionId), 0);
        auto request = requestsToSend.find(testPartitionId)->second;
        EXPECT_EQ(request->shuffleId, testShuffleId);
        EXPECT_EQ(request->mapId, testMapId);
        EXPECT_EQ(request->attemptId, testAttemptId);
        EXPECT_EQ(request->partitionId, testPartitionId);
        EXPECT_EQ(request->epoch, testEpoch);

        ++reviveBatchCalledTimes;
        return std::nullopt;
      });

  EXPECT_EQ(
      reviveRequest->reviveStatus, protocol::StatusCode::REVIVE_INITIALIZED);
  reviveManager_->addRequest(reviveRequest);
  std::this_thread::sleep_for(
      std::chrono::milliseconds(pushReviveIntervalMs_ * 3));

  EXPECT_GT(reviveBatchCalledTimes, 0);
  EXPECT_EQ(reviveRequest->reviveStatus, protocol::StatusCode::REVIVE_FAILED);
}
