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

#include "celeborn/client/tests/ShuffleClientTestUtils.h"
#include "celeborn/client/writer/PushDataCallback.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
class MockShuffleClient : public TestShuffleClientBase {
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

  static std::shared_ptr<MockShuffleClient> create(
      std::shared_ptr<const conf::CelebornConf> conf =
          std::make_shared<conf::CelebornConf>()) {
    return std::shared_ptr<MockShuffleClient>(
        new MockShuffleClient(std::move(conf)));
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
  explicit MockShuffleClient(std::shared_ptr<const conf::CelebornConf> conf)
      : TestShuffleClientBase(std::move(conf)) {}

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

  auto response = createStatusResponse(protocol::StatusCode::MAP_ENDED);
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

  auto response = createStatusResponse(protocol::StatusCode::SOFT_SPLIT);
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

  auto response = createStatusResponse(protocol::StatusCode::HARD_SPLIT);
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

TEST(PushDataCallbackTest, onFailureCauseFromMessage) {
  const auto celebornConf = conf::CelebornConf();
  auto pushState = std::make_shared<PushState>(celebornConf);
  const int testRemainingReviveTimes = 1;
  auto mockClient = MockShuffleClient::create();

  // Capture the cause propagated into the revive request; it must be derived
  // from the failure message rather than a hardcoded value.
  protocol::StatusCode capturedCause = protocol::StatusCode::SUCCESS;
  mockClient->setOnAddRequestToReviveManager(
      [&capturedCause](std::shared_ptr<protocol::ReviveRequest> request) {
        capturedCause = request->cause;
      });
  mockClient->setOnAddPushDataRetryTask([](folly::Func&& task) { task(); });
  mockClient->setOnSubmitRetryPushData(
      [](int,
         std::unique_ptr<memory::ReadOnlyByteBuffer>,
         int,
         std::shared_ptr<PushDataCallback>,
         std::shared_ptr<PushState>,
         ShuffleClientImpl::PtrReviveRequest,
         int,
         long) {});

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

  auto exception = std::make_unique<std::runtime_error>(
      "PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY: connection refused");
  pushDataCallback->onFailure(std::move(exception));
  EXPECT_EQ(
      capturedCause,
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
}

namespace {
std::shared_ptr<PushDataCallback> createCallback(
    std::shared_ptr<PushState> pushState,
    std::shared_ptr<MockShuffleClient> mockClient) {
  return PushDataCallback::create(
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
}
} // namespace

// MAP_ENDED is a successful push: the mapper-end is recorded AND the in-flight
// batch must be released (mirrors the Java client). Otherwise a trailing
// limitZeroInFlight at mapperEnd would never reach zero.
TEST(PushDataCallbackTest, onSuccessMapEndedReleasesInflight) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  const auto host = testLastestLocation->hostAndPushPort();

  // Simulate pushData registering the batch as in-flight before send.
  pushState->addBatch(testBatchId, /*batchBytesSize=*/16, host);
  auto pushDataCallback = createCallback(pushState, mockClient);

  pushDataCallback->onSuccess(
      createStatusResponse(protocol::StatusCode::MAP_ENDED));

  // Mapper end is recorded.
  ASSERT_TRUE(mockClient->mapperEndSets().containsKey(testShuffleId));
  EXPECT_TRUE(mockClient->mapperEndSets()
                  .get(testShuffleId)
                  .value()
                  ->contains(testMapId));
  // In-flight returned to zero (false == reached zero without timing out).
  EXPECT_FALSE(pushState->limitZeroInFlight());
}

// An unhandled success status (e.g. STAGE_ENDED) is treated as success and must
// also release the in-flight batch, like the Java client's final else branch.
TEST(PushDataCallbackTest, onSuccessUnknownStatusReleasesInflight) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  const auto host = testLastestLocation->hostAndPushPort();

  pushState->addBatch(testBatchId, /*batchBytesSize=*/16, host);
  auto pushDataCallback = createCallback(pushState, mockClient);

  pushDataCallback->onSuccess(
      createStatusResponse(protocol::StatusCode::STAGE_ENDED));

  EXPECT_FALSE(pushState->limitZeroInFlight());
}

namespace {
std::shared_ptr<const conf::CelebornConf> makeTrackingConf(
    bool replicateEnabled) {
  auto conf = std::make_shared<conf::CelebornConf>();
  conf->registerProperty(
      conf::CelebornConf::kClientAdaptiveOptimizeSkewedPartitionReadEnabled,
      "true");
  conf->registerProperty(
      conf::CelebornConf::kClientPushReplicateEnabled,
      replicateEnabled ? "true" : "false");
  return conf;
}

std::shared_ptr<const protocol::PartitionLocation> makeTrackedLocation() {
  auto loc = std::make_shared<protocol::PartitionLocation>();
  loc->id = 7;
  loc->epoch = 2;
  loc->host = "track-host";
  loc->pushPort = 9100;
  loc->mode = protocol::PartitionLocation::PRIMARY;
  return loc;
}

bool failedBatchRecorded(
    const protocol::PartitionPushFailedBatches& failedBatches,
    const std::string& partitionUniqueId,
    int mapId,
    int attemptId,
    int batchId) {
  auto locationIt = failedBatches.find(partitionUniqueId);
  if (locationIt == failedBatches.end()) {
    return false;
  }
  auto attemptIt =
      locationIt->second.find(utils::makeAttemptKey(mapId, attemptId));
  return attemptIt != locationIt->second.end() &&
      attemptIt->second.count(batchId) > 0;
}
} // namespace

// With failure tracking enabled, onFailure records the batch (regardless of
// cause) so the reader of a skewed partition can skip the possible duplicate.
TEST(PushDataCallbackTest, onFailureRecordsFailedBatchWhenTrackingEnabled) {
  auto conf = makeTrackingConf(/*replicateEnabled=*/false);
  auto pushState = std::make_shared<PushState>(*conf);
  auto mockClient = MockShuffleClient::create(conf);
  auto location = makeTrackedLocation();

  // remainingReviveTimes = 0: records, then surfaces the terminal exception.
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
      /*remainingReviveTimes=*/0,
      location);

  pushDataCallback->onFailure(std::make_unique<std::runtime_error>("boom"));

  EXPECT_TRUE(failedBatchRecorded(
      pushState->getFailedBatches(),
      location->uniqueId(),
      testMapId,
      testAttemptId,
      testBatchId));
}

// HARD_SPLIT records the resubmitted batch only when replication is also
// enabled (the gating the Java client applies), since without a replica the
// original worker cannot have committed a duplicate.
TEST(
    PushDataCallbackTest,
    onSuccessHardSplitRecordsFailedBatchGatedOnReplicate) {
  for (bool replicateEnabled : {true, false}) {
    auto conf = makeTrackingConf(replicateEnabled);
    auto pushState = std::make_shared<PushState>(*conf);
    auto mockClient = MockShuffleClient::create(conf);
    auto location = makeTrackedLocation();
    mockClient->setOnAddRequestToReviveManager(
        [](std::shared_ptr<protocol::ReviveRequest>) {});
    mockClient->setOnAddPushDataRetryTask([](folly::Func&&) {});

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
        /*remainingReviveTimes=*/1,
        location);

    pushDataCallback->onSuccess(
        createStatusResponse(protocol::StatusCode::HARD_SPLIT));

    EXPECT_EQ(
        failedBatchRecorded(
            pushState->getFailedBatches(),
            location->uniqueId(),
            testMapId,
            testAttemptId,
            testBatchId),
        replicateEnabled)
        << "replicateEnabled: " << replicateEnabled;
  }
}
