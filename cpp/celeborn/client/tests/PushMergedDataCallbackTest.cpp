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
#include "celeborn/client/writer/PushMergedDataCallback.h"
#include "celeborn/proto/TransportMessagesCpp.pb.h"
#include "celeborn/protocol/TransportMessage.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
// Mock that overrides the few ShuffleClientImpl hooks PushMergedDataCallback
// reaches, capturing the arguments passed to the revive/retry paths so tests
// can assert on them.
class MockShuffleClient : public TestShuffleClientBase {
 public:
  static std::shared_ptr<MockShuffleClient> create(
      std::shared_ptr<const conf::CelebornConf> conf =
          std::make_shared<conf::CelebornConf>()) {
    return std::shared_ptr<MockShuffleClient>(
        new MockShuffleClient(std::move(conf)));
  }

  virtual ~MockShuffleClient() = default;

  bool mapperEnded(int shuffleId, int mapId) override {
    return mapperEnded_;
  }

  void setMapperEnded(bool value) {
    mapperEnded_ = value;
  }

  std::optional<PtrPartitionLocationMap> getPartitionLocationMap(
      int shuffleId) override {
    return {std::make_shared<PartitionLocationMap>()};
  }

  utils::ConcurrentHashMap<int, std::shared_ptr<utils::ConcurrentHashSet<int>>>&
  mapperEndSets() override {
    return ShuffleClientImpl::mapperEndSets();
  }

  void addRequestToReviveManager(
      std::shared_ptr<protocol::ReviveRequest> reviveRequest) override {
    reviveCauses.push_back(reviveRequest->cause);
  }

  void submitRetryPushMergedData(
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      const std::string& mapKey,
      std::vector<DataBatch> batches,
      std::vector<std::shared_ptr<protocol::ReviveRequest>> reviveRequests,
      protocol::StatusCode cause,
      int oldGroupedBatchId,
      std::shared_ptr<PushState> pushState,
      int remainReviveTimes,
      long reviveResponseDueTimeMs) override {
    submitRetryCalled = true;
    capturedCause = cause;
    capturedBatchesSize = static_cast<int>(batches.size());
    capturedRemainReviveTimes = remainReviveTimes;
  }

  // Captured state for assertions.
  std::vector<protocol::StatusCode> reviveCauses;
  bool submitRetryCalled = false;
  protocol::StatusCode capturedCause = protocol::StatusCode::SUCCESS;
  int capturedBatchesSize = 0;
  int capturedRemainReviveTimes = 0;

 private:
  explicit MockShuffleClient(std::shared_ptr<const conf::CelebornConf> conf)
      : TestShuffleClientBase(std::move(conf)) {}

  bool mapperEnded_ = false;
};

// Builds a merged-data split response: a single status byte followed by a
// TransportMessage-wrapped PbPushMergedDataSplitPartitionInfo, matching what a
// worker that supports per-partition split info returns. `splits` maps a batch
// index to its per-partition status (SOFT_SPLIT or HARD_SPLIT).
std::unique_ptr<memory::ReadOnlyByteBuffer> createSplitResponse(
    uint8_t statusByte,
    const std::vector<std::pair<int, protocol::StatusCode>>& splits) {
  PbPushMergedDataSplitPartitionInfo pb;
  for (const auto& split : splits) {
    pb.add_splitpartitionindexes(split.first);
    pb.add_statuscodes(static_cast<int>(split.second));
  }
  // Any valid MessageType works; onSuccess only reads the payload.
  protocol::TransportMessage transportMessage(
      MAPPER_END, pb.SerializeAsString());
  auto transportBytes = transportMessage.toReadOnlyByteBuffer()->readToString();

  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(1 + transportBytes.size());
  writeBuffer->write<uint8_t>(statusByte);
  writeBuffer->writeFromString(transportBytes);
  return memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
}

// Builds a split response whose trailing TransportMessage is truncated, so
// parsing it throws (the declared payload length no longer matches the bytes).
std::unique_ptr<memory::ReadOnlyByteBuffer> createMalformedSplitResponse(
    uint8_t statusByte) {
  PbPushMergedDataSplitPartitionInfo pb;
  pb.add_splitpartitionindexes(0);
  pb.add_statuscodes(static_cast<int>(protocol::StatusCode::HARD_SPLIT));
  protocol::TransportMessage transportMessage(
      MAPPER_END, pb.SerializeAsString());
  auto transportBytes = transportMessage.toReadOnlyByteBuffer()->readToString();
  // Drop the last payload byte to corrupt the framing.
  transportBytes.pop_back();

  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(1 + transportBytes.size());
  writeBuffer->write<uint8_t>(statusByte);
  writeBuffer->writeFromString(transportBytes);
  return memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
}

std::vector<DataBatch> makeBatches(int n) {
  std::vector<DataBatch> batches;
  for (int i = 0; i < n; i++) {
    auto loc = std::make_shared<protocol::PartitionLocation>();
    loc->id = i;
    loc->epoch = 0;
    loc->host = "batch-host";
    loc->pushPort = 9200;
    loc->mode = protocol::PartitionLocation::PRIMARY;
    batches.emplace_back(
        std::move(loc),
        /*batchId=*/i,
        memory::ReadOnlyByteBuffer::createEmptyBuffer());
  }
  return batches;
}

std::vector<int> makePartitionIds(int n) {
  std::vector<int> ids;
  for (int i = 0; i < n; i++) {
    ids.push_back(i);
  }
  return ids;
}

const int testShuffleId = 1000;
const int testMapId = 1001;
const int testAttemptId = 1002;
const int testNumMappers = 1004;
const int testNumPartitions = 1005;
const int testGroupedBatchId = 1006;
const std::string testMapKey = "test-map-key";
const std::string testHostAndPushPort = "test-host:1001";

std::shared_ptr<PushMergedDataCallback> createCallback(
    std::shared_ptr<PushState> pushState,
    std::shared_ptr<MockShuffleClient> mockClient,
    int numBatches,
    int remainingReviveTimes) {
  return PushMergedDataCallback::create(
      testShuffleId,
      testMapId,
      testAttemptId,
      testNumMappers,
      testNumPartitions,
      testMapKey,
      testHostAndPushPort,
      testGroupedBatchId,
      makeBatches(numBatches),
      makePartitionIds(numBatches),
      pushState,
      mockClient->weak_from_this(),
      remainingReviveTimes);
}
} // namespace

TEST(PushMergedDataCallbackTest, onSuccessEmptyReleasesInflight) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  pushState->addBatch(
      testGroupedBatchId, /*batchBytesSize=*/16, testHostAndPushPort);
  auto callback = createCallback(pushState, mockClient, /*numBatches=*/1, 1);

  callback->onSuccess(memory::ReadOnlyByteBuffer::createEmptyBuffer());

  EXPECT_FALSE(mockClient->submitRetryCalled);
  EXPECT_FALSE(pushState->limitZeroInFlight());
}

TEST(PushMergedDataCallbackTest, onSuccessMapEndedReleasesInflight) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  pushState->addBatch(
      testGroupedBatchId, /*batchBytesSize=*/16, testHostAndPushPort);
  auto callback = createCallback(pushState, mockClient, /*numBatches=*/1, 1);

  callback->onSuccess(createStatusResponse(protocol::StatusCode::MAP_ENDED));

  ASSERT_TRUE(mockClient->mapperEndSets().containsKey(testShuffleId));
  EXPECT_TRUE(mockClient->mapperEndSets()
                  .get(testShuffleId)
                  .value()
                  ->contains(testMapId));
  EXPECT_FALSE(pushState->limitZeroInFlight());
}

TEST(PushMergedDataCallbackTest, onSuccessCongestionReleasesInflight) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  pushState->addBatch(
      testGroupedBatchId, /*batchBytesSize=*/16, testHostAndPushPort);
  auto callback = createCallback(pushState, mockClient, /*numBatches=*/1, 1);

  callback->onSuccess(createStatusResponse(
      protocol::StatusCode::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED));

  EXPECT_FALSE(mockClient->submitRetryCalled);
  EXPECT_FALSE(pushState->limitZeroInFlight());
}

// Old worker returns a bare split status byte without per-partition info: all
// batches must be resubmitted and revived with HARD_SPLIT (never the raw
// SOFT_SPLIT byte), matching the Java client.
TEST(PushMergedDataCallbackTest, onSuccessOldWorkerSplitRevivesWithHardSplit) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  const int numBatches = 2;
  const int remainingReviveTimes = 3;
  auto callback =
      createCallback(pushState, mockClient, numBatches, remainingReviveTimes);

  // Bare SOFT_SPLIT byte (no trailing PbPushMergedDataSplitPartitionInfo).
  callback->onSuccess(createStatusResponse(protocol::StatusCode::SOFT_SPLIT));

  ASSERT_TRUE(mockClient->submitRetryCalled);
  EXPECT_EQ(mockClient->capturedCause, protocol::StatusCode::HARD_SPLIT);
  EXPECT_EQ(mockClient->capturedBatchesSize, numBatches);
  // Split retries do not consume a revive attempt.
  EXPECT_EQ(mockClient->capturedRemainReviveTimes, remainingReviveTimes);
  ASSERT_EQ(mockClient->reviveCauses.size(), static_cast<size_t>(numBatches));
  for (auto cause : mockClient->reviveCauses) {
    EXPECT_EQ(cause, protocol::StatusCode::HARD_SPLIT);
  }
}

// onFailure derives the cause from the error message and threads it into the
// retry (rather than a hardcoded cause), decrementing the revive budget.
TEST(PushMergedDataCallbackTest, onFailureRevivePropagatesCause) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  const int remainingReviveTimes = 2;
  auto callback = createCallback(
      pushState, mockClient, /*numBatches=*/1, remainingReviveTimes);

  callback->onFailure(std::make_unique<std::runtime_error>(
      "PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY: connection refused"));

  ASSERT_TRUE(mockClient->submitRetryCalled);
  EXPECT_EQ(
      mockClient->capturedCause,
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
  EXPECT_EQ(mockClient->capturedRemainReviveTimes, remainingReviveTimes - 1);
  ASSERT_EQ(mockClient->reviveCauses.size(), 1u);
  EXPECT_EQ(
      mockClient->reviveCauses[0],
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
}

// Out of revive attempts: a terminal exception annotated with the normalized
// cause is recorded, and no retry is submitted.
TEST(PushMergedDataCallbackTest, onFailureNoReviveSetsExceptionWithCause) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  auto callback =
      createCallback(pushState, mockClient, /*numBatches=*/1, /*remain=*/0);

  callback->onFailure(std::make_unique<std::runtime_error>("boom"));

  EXPECT_FALSE(mockClient->submitRetryCalled);
  ASSERT_TRUE(pushState->exceptionExists());
  auto msg = pushState->getExceptionMsg();
  ASSERT_TRUE(msg.has_value());
  // Unknown messages classify to the primary non-critical cause.
  EXPECT_NE(
      msg->find("PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY"),
      std::string::npos);
}

// New worker returns per-partition split info: only the HARD_SPLIT partition is
// resubmitted (revived with HARD_SPLIT); a non-split partition is left alone.
TEST(PushMergedDataCallbackTest, onSuccessPerPartitionHardSplitRetries) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  const int numBatches = 2;
  const int remainingReviveTimes = 3;
  auto callback =
      createCallback(pushState, mockClient, numBatches, remainingReviveTimes);

  // Status byte + PB marking only batch index 0 as HARD_SPLIT.
  callback->onSuccess(createSplitResponse(
      protocol::StatusCode::HARD_SPLIT,
      {{0, protocol::StatusCode::HARD_SPLIT}}));

  ASSERT_TRUE(mockClient->submitRetryCalled);
  EXPECT_EQ(mockClient->capturedCause, protocol::StatusCode::HARD_SPLIT);
  // Only the single hard-split partition is resubmitted.
  EXPECT_EQ(mockClient->capturedBatchesSize, 1);
  EXPECT_EQ(mockClient->capturedRemainReviveTimes, remainingReviveTimes);
  ASSERT_EQ(mockClient->reviveCauses.size(), 1u);
  EXPECT_EQ(mockClient->reviveCauses[0], protocol::StatusCode::HARD_SPLIT);
}

// A malformed (truncated) split-info payload must not escape onSuccess; it
// becomes a terminal push exception instead of leaving the batch in flight.
TEST(PushMergedDataCallbackTest, onSuccessMalformedSplitInfoSetsException) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  auto callback =
      createCallback(pushState, mockClient, /*numBatches=*/1, /*remain=*/3);

  EXPECT_NO_THROW(callback->onSuccess(createMalformedSplitResponse(
      static_cast<uint8_t>(protocol::StatusCode::HARD_SPLIT))));

  EXPECT_FALSE(mockClient->submitRetryCalled);
  ASSERT_TRUE(pushState->exceptionExists());
  auto msg = pushState->getExceptionMsg();
  ASSERT_TRUE(msg.has_value());
  EXPECT_NE(
      msg->find("parse pushMergedData response failed"), std::string::npos);
}

// Per-partition SOFT_SPLIT only adds a revive request (no resubmit) and
// releases the in-flight grouped batch.
TEST(PushMergedDataCallbackTest, onSuccessPerPartitionSoftSplitRevivesOnly) {
  auto pushState = makeShortTimeoutPushState();
  auto mockClient = MockShuffleClient::create();
  pushState->addBatch(
      testGroupedBatchId, /*batchBytesSize=*/16, testHostAndPushPort);
  auto callback =
      createCallback(pushState, mockClient, /*numBatches=*/2, /*remain=*/3);

  callback->onSuccess(createSplitResponse(
      protocol::StatusCode::SOFT_SPLIT,
      {{0, protocol::StatusCode::SOFT_SPLIT}}));

  EXPECT_FALSE(mockClient->submitRetryCalled);
  ASSERT_EQ(mockClient->reviveCauses.size(), 1u);
  EXPECT_EQ(mockClient->reviveCauses[0], protocol::StatusCode::SOFT_SPLIT);
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

// makeBatches builds locations with id == batch index and epoch 0, so the
// batch at index i is recorded under partitionUniqueId "<i>-0".
bool failedBatchRecorded(
    const protocol::PartitionPushFailedBatches& failedBatches,
    int batchIndex) {
  auto locationIt = failedBatches.find(fmt::format("{}-{}", batchIndex, 0));
  if (locationIt == failedBatches.end()) {
    return false;
  }
  auto attemptIt =
      locationIt->second.find(utils::makeAttemptKey(testMapId, testAttemptId));
  return attemptIt != locationIt->second.end() &&
      attemptIt->second.count(batchIndex) > 0;
}
} // namespace

// Old worker bare HARD_SPLIT (no per-partition info): every resubmitted batch
// must be recorded when failure tracking and replication are enabled, exactly
// like the per-partition branch, so the skewed-partition reader can dedup.
TEST(PushMergedDataCallbackTest, onSuccessOldWorkerSplitRecordsFailedBatches) {
  auto conf = makeTrackingConf(/*replicateEnabled=*/true);
  auto pushState = std::make_shared<PushState>(*conf);
  auto mockClient = MockShuffleClient::create(conf);
  const int numBatches = 2;
  auto callback =
      createCallback(pushState, mockClient, numBatches, /*remain=*/3);

  callback->onSuccess(createStatusResponse(protocol::StatusCode::HARD_SPLIT));

  ASSERT_TRUE(mockClient->submitRetryCalled);
  const auto failedBatches = pushState->getFailedBatches();
  for (int i = 0; i < numBatches; i++) {
    EXPECT_TRUE(failedBatchRecorded(failedBatches, i)) << "batch " << i;
  }
}

// Per-partition split info: only the HARD_SPLIT batch is recorded, and only
// when replication is enabled (the gating the Java client applies).
TEST(PushMergedDataCallbackTest, onSuccessPerPartitionHardSplitRecordsGated) {
  for (bool replicateEnabled : {true, false}) {
    auto conf = makeTrackingConf(replicateEnabled);
    auto pushState = std::make_shared<PushState>(*conf);
    auto mockClient = MockShuffleClient::create(conf);
    auto callback =
        createCallback(pushState, mockClient, /*numBatches=*/2, /*remain=*/3);

    callback->onSuccess(createSplitResponse(
        protocol::StatusCode::HARD_SPLIT,
        {{0, protocol::StatusCode::HARD_SPLIT}}));

    const auto failedBatches = pushState->getFailedBatches();
    EXPECT_EQ(failedBatchRecorded(failedBatches, 0), replicateEnabled)
        << "replicateEnabled: " << replicateEnabled;
    // The non-split batch is never recorded.
    EXPECT_FALSE(failedBatchRecorded(failedBatches, 1));
  }
}

// onFailure records every batch of the merged push (regardless of cause and
// without the replication gate) when failure tracking is enabled.
TEST(
    PushMergedDataCallbackTest,
    onFailureRecordsAllBatchesWhenTrackingEnabled) {
  auto conf = makeTrackingConf(/*replicateEnabled=*/false);
  auto pushState = std::make_shared<PushState>(*conf);
  auto mockClient = MockShuffleClient::create(conf);
  const int numBatches = 2;
  // remainingReviveTimes = 0: records, then surfaces the terminal exception.
  auto callback =
      createCallback(pushState, mockClient, numBatches, /*remain=*/0);

  callback->onFailure(std::make_unique<std::runtime_error>("boom"));

  EXPECT_TRUE(pushState->exceptionExists());
  const auto failedBatches = pushState->getFailedBatches();
  for (int i = 0; i < numBatches; i++) {
    EXPECT_TRUE(failedBatchRecorded(failedBatches, i)) << "batch " << i;
  }
}
