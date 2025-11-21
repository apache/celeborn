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

#include "celeborn/client/writer/PushDataCallback.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
class MockShuffleClient : public ShuffleClientImpl {
 public:
  friend class PushDataCallback;

  using FuncOnSubmitRetryPushData = std::function<void(
      int,
      std::unique_ptr<memory::ReadOnlyByteBuffer>,
      int,
      std::shared_ptr<PushDataCallback>,
      std::shared_ptr<PushState>,
      PtrReviveRequest,
      int,
      long)>;
  using FuncOnAddRequestToReviveManager =
      std::function<void(std::shared_ptr<protocol::ReviveRequest>)>;
  using FuncOnAddPushDataRetryTask = std::function<void(folly::Func&&)>;

  static std::shared_ptr<MockShuffleClient> create() {
    return std::shared_ptr<MockShuffleClient>(new MockShuffleClient());
  }

  virtual ~MockShuffleClient() = default;

  bool mapperEnded(int shuffleId, int mapId) override {
    return false;
  }

  std::optional<PtrPartitionLocationMap> getPartitionLocationMap(
      int shuffleId) override {
    return {std::make_shared<PartitionLocationMap>()};
  }

  void submitRetryPushData(
      int shuffleId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body,
      int batchId,
      std::shared_ptr<PushDataCallback> pushDataCallback,
      std::shared_ptr<PushState> pushState,
      PtrReviveRequest request,
      int remainReviveTimes,
      long dueTimeMs) override {
    onSubmitRetryPushData_(
        shuffleId,
        std::move(body),
        batchId,
        pushDataCallback,
        pushState,
        request,
        remainReviveTimes,
        dueTimeMs);
  }

  void setOnSubmitRetryPushData(FuncOnSubmitRetryPushData&& func) {
    onSubmitRetryPushData_ = func;
  }

  void addRequestToReviveManager(
      std::shared_ptr<protocol::ReviveRequest> reviveRequest) override {
    onAddRequestToReviveManager_(reviveRequest);
  }

  void setOnAddRequestToReviveManager(FuncOnAddRequestToReviveManager&& func) {
    onAddRequestToReviveManager_ = func;
  }

  utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>&
  mapperEndSets() override {
    return ShuffleClientImpl::mapperEndSets();
  }

  void addPushDataRetryTask(folly::Func&& task) override {
    onAddPushDataRetryTask_(std::move(task));
  }

  void setOnAddPushDataRetryTask(FuncOnAddPushDataRetryTask&& func) {
    onAddPushDataRetryTask_ = func;
  }

 private:
  MockShuffleClient()
      : ShuffleClientImpl(
            "mock",
            std::make_shared<conf::CelebornConf>(),
            nullptr) {}

  FuncOnSubmitRetryPushData onSubmitRetryPushData_ =
      [](int,
         std::unique_ptr<memory::ReadOnlyByteBuffer>,
         int,
         std::shared_ptr<PushDataCallback>,
         std::shared_ptr<PushState>,
         PtrReviveRequest,
         int,
         long) { CELEBORN_UNREACHABLE("not expected to call this"); };
  FuncOnAddRequestToReviveManager onAddRequestToReviveManager_ =
      [](std::shared_ptr<protocol::ReviveRequest>) {
        CELEBORN_UNREACHABLE("not expected to call this");
      };
  FuncOnAddPushDataRetryTask onAddPushDataRetryTask_ = [](folly::Func&&) {
    CELEBORN_UNREACHABLE("not expected to call this");
  };
};

std::unique_ptr<memory::ReadOnlyByteBuffer> createReadOnlyByteBuffer(
    uint8_t code) {
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(1);
  writeBuffer->write<uint8_t>(code);
  return std::move(memory::ByteBuffer::toReadOnly(std::move(writeBuffer)));
}
} // namespace

const int testPushReviveIntervalMs = 100;
const int testShuffleId = 1000;
const int testMapId = 1001;
const int testAttemptId = 1002;
const int testPartitionId = 1003;
const int testNumMappers = 1004;
const int testNumPartitions = 1005;
const std::string testMapKey = "test-map-key";
const int testBatchId = 1006;
const auto testLastestLocation =
    std::make_shared<const protocol::PartitionLocation>();

TEST(PushDataCallbackTest, onSuccessAndNoOperation) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  auto mockClient = MockShuffleClient::create();
  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      1,
      testLastestLocation);

  auto response = memory::ReadOnlyByteBuffer::createEmptyBuffer();
  pushDataCallback->onSuccess(std::move(response));
}

TEST(PushDataCallbackTest, onSuccessAndMapEnd) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  auto mockClient = MockShuffleClient::create();
  EXPECT_EQ(mockClient->mapperEndSets().size(), 0);
  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      1,
      testLastestLocation);

  auto response = createReadOnlyByteBuffer(protocol::StatusCode::MAP_ENDED);
  pushDataCallback->onSuccess(std::move(response));
  auto& mapperEndSets = mockClient->mapperEndSets();
  EXPECT_TRUE(mapperEndSets.containsKey(testShuffleId));
  auto mapperEndSet = mapperEndSets.get(testShuffleId).value();
  EXPECT_TRUE(mapperEndSet->contains(testMapId));
}

TEST(PushDataCallbackTest, onSuccessAndSoftSplit) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  auto mockClient = MockShuffleClient::create();

  int addRequestCalledTimes = 0;
  mockClient->setOnAddRequestToReviveManager(
      [=, &addRequestCalledTimes](
          std::shared_ptr<protocol::ReviveRequest> request) mutable {
        EXPECT_EQ(request->shuffleId, testShuffleId);
        EXPECT_EQ(request->mapId, testMapId);
        EXPECT_EQ(request->attemptId, testAttemptId);
        EXPECT_EQ(request->partitionId, testPartitionId);
        EXPECT_EQ(request->cause, protocol::StatusCode::SOFT_SPLIT);

        addRequestCalledTimes++;
      });

  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      1,
      testLastestLocation);

  auto response = createReadOnlyByteBuffer(protocol::StatusCode::SOFT_SPLIT);
  pushDataCallback->onSuccess(std::move(response));
  EXPECT_GT(addRequestCalledTimes, 0);
}

TEST(PushDataCallbackTest, onSuccessAndHardSplit) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  const int testRemainingReviveTimes = 1;
  auto mockClient = MockShuffleClient::create();

  int addRequestCalledTimes = 0;
  mockClient->setOnAddRequestToReviveManager(
      [=, &addRequestCalledTimes](
          std::shared_ptr<protocol::ReviveRequest> request) mutable {
        EXPECT_EQ(request->shuffleId, testShuffleId);
        EXPECT_EQ(request->mapId, testMapId);
        EXPECT_EQ(request->attemptId, testAttemptId);
        EXPECT_EQ(request->partitionId, testPartitionId);
        EXPECT_EQ(request->cause, protocol::StatusCode::HARD_SPLIT);

        addRequestCalledTimes++;
      });
  int addRetryTaskCalledTimes = 0;
  mockClient->setOnAddPushDataRetryTask(
      [&addRetryTaskCalledTimes](folly::Func&& task) {
        addRetryTaskCalledTimes++;
        task();
      });
  int submitRetryCalledTimes = 0;
  mockClient->setOnSubmitRetryPushData(
      [=, &submitRetryCalledTimes](
          int shuffleId,
          std::unique_ptr<memory::ReadOnlyByteBuffer> body,
          int batchId,
          std::shared_ptr<PushDataCallback> pushDataCallback,
          std::shared_ptr<PushState> pushState,
          ShuffleClientImpl::PtrReviveRequest request,
          int remainReviveTimes,
          long dueTimeMs) mutable {
        EXPECT_EQ(shuffleId, testShuffleId);
        EXPECT_EQ(batchId, testBatchId);
        EXPECT_EQ(remainReviveTimes, testRemainingReviveTimes);

        submitRetryCalledTimes++;
      });

  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      testRemainingReviveTimes,
      testLastestLocation);

  auto response = createReadOnlyByteBuffer(protocol::StatusCode::HARD_SPLIT);
  pushDataCallback->onSuccess(std::move(response));
  EXPECT_GT(addRequestCalledTimes, 0);
  EXPECT_GT(addRetryTaskCalledTimes, 0);
  EXPECT_GT(submitRetryCalledTimes, 0);
}

TEST(PushDataCallbackTest, onFailureAndRevive) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  const int testRemainingReviveTimes = 1;
  auto mockClient = MockShuffleClient::create();

  int addRequestCalledTimes = 0;
  mockClient->setOnAddRequestToReviveManager(
      [=, &addRequestCalledTimes](
          std::shared_ptr<protocol::ReviveRequest> request) mutable {
        EXPECT_EQ(request->shuffleId, testShuffleId);
        EXPECT_EQ(request->mapId, testMapId);
        EXPECT_EQ(request->attemptId, testAttemptId);
        EXPECT_EQ(request->partitionId, testPartitionId);

        addRequestCalledTimes++;
      });
  int addRetryTaskCalledTimes = 0;
  mockClient->setOnAddPushDataRetryTask(
      [&addRetryTaskCalledTimes](folly::Func&& task) {
        addRetryTaskCalledTimes++;
        task();
      });
  int submitRetryCalledTimes = 0;
  mockClient->setOnSubmitRetryPushData(
      [=, &submitRetryCalledTimes](
          int shuffleId,
          std::unique_ptr<memory::ReadOnlyByteBuffer> body,
          int batchId,
          std::shared_ptr<PushDataCallback> pushDataCallback,
          std::shared_ptr<PushState> pushState,
          ShuffleClientImpl::PtrReviveRequest request,
          int remainReviveTimes,
          long dueTimeMs) mutable {
        EXPECT_EQ(shuffleId, testShuffleId);
        EXPECT_EQ(batchId, testBatchId);
        EXPECT_EQ(remainReviveTimes, testRemainingReviveTimes - 1);

        submitRetryCalledTimes++;
      });

  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      testRemainingReviveTimes,
      testLastestLocation);

  auto exception = std::make_unique<std::runtime_error>("test");
  pushDataCallback->onFailure(std::move(exception));
  EXPECT_GT(addRequestCalledTimes, 0);
  EXPECT_GT(addRetryTaskCalledTimes, 0);
  EXPECT_GT(submitRetryCalledTimes, 0);
}

TEST(PushDataCallbackTest, onFailureAndNoRevive) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  const int testRemainingReviveTimes = 0;
  auto mockClient = MockShuffleClient::create();

  auto pushDataCallback = PushDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testPartitionId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testBatchId,
      memory::ReadOnlyByteBuffer::createEmptyBuffer(),
      pushState,
      mockClient->weak_from_this(),
      testRemainingReviveTimes,
      testLastestLocation);

  auto exception = std::make_unique<std::runtime_error>("test");
  pushDataCallback->onFailure(std::move(exception));
  EXPECT_TRUE(pushState->exceptionExists());
}
