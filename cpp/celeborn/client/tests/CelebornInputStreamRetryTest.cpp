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
#include <system_error>

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/conf/CelebornConf.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::network;
using namespace celeborn::protocol;
using namespace celeborn::conf;
using namespace celeborn::memory;

namespace {
using MS = std::chrono::milliseconds;

class StubShuffleClient : public ShuffleClient {
 public:
  explicit StubShuffleClient(
      std::shared_ptr<CelebornConf> conf,
      std::shared_ptr<ShuffleClient::FetchExcludedWorkers> excludedWorkers)
      : conf_(std::move(conf)), excludedWorkers_(std::move(excludedWorkers)) {}

  void setupLifecycleManagerRef(std::string&, int) override {}
  void setupLifecycleManagerRef(
      std::shared_ptr<network::NettyRpcEndpointRef>&) override {}
  int pushData(int, int, int, int, const uint8_t*, size_t, size_t, int, int)
      override {
    return 0;
  }
  void mapperEnd(int, int, int, int) override {}
  void cleanup(int, int, int) override {}
  void updateReducerFileGroup(int) override {}
  std::unique_ptr<CelebornInputStream> readPartition(int, int, int, int, int)
      override {
    return nullptr;
  }
  std::unique_ptr<CelebornInputStream>
  readPartition(int, int, int, int, int, bool) override {
    return nullptr;
  }
  bool cleanupShuffle(int) override {
    return true;
  }
  void shutdown() override {}

  void excludeFailedFetchLocation(
      const std::string& hostAndFetchPort,
      const std::exception& e) override {
    if (conf_->clientPushReplicateEnabled() &&
        conf_->clientFetchExcludeWorkerOnFailureEnabled() &&
        utils::isCriticalCauseForFetch(e)) {
      auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count();
      excludedWorkers_->set(hostAndFetchPort, now);
    }
  }

 private:
  std::shared_ptr<CelebornConf> conf_;
  std::shared_ptr<ShuffleClient::FetchExcludedWorkers> excludedWorkers_;
};

// A TransportClient that always throws a configurable exception
// from sendRpcRequestSync, allowing us to exercise retry logic.
class FailingTransportClient : public TransportClient {
 public:
  explicit FailingTransportClient(std::exception_ptr exceptionToThrow)
      : TransportClient(nullptr, nullptr, MS(100)),
        exceptionToThrow_(std::move(exceptionToThrow)) {}

  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout)
      override {
    std::rethrow_exception(exceptionToThrow_);
  }

  void sendRpcRequestWithoutResponse(const RpcRequest& request) override {}

  void fetchChunkAsync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      FetchChunkSuccessCallback onSuccess,
      FetchChunkFailureCallback onFailure) override {}

 private:
  std::exception_ptr exceptionToThrow_;
};

class TrackingTransportClientFactory : public TransportClientFactory {
 public:
  explicit TrackingTransportClientFactory(
      std::shared_ptr<FailingTransportClient> client)
      : TransportClientFactory(std::make_shared<CelebornConf>()),
        client_(std::move(client)) {}

  std::shared_ptr<TransportClient> createClient(
      const std::string& host,
      uint16_t port) override {
    hosts_.push_back(host);
    return client_;
  }

  const std::vector<std::string>& hosts() const {
    return hosts_;
  }

 private:
  std::shared_ptr<FailingTransportClient> client_;
  std::vector<std::string> hosts_;
};

std::shared_ptr<PartitionLocation> makeLocationWithPeer() {
  auto primary = std::make_shared<PartitionLocation>();
  primary->id = 0;
  primary->epoch = 0;
  primary->host = "primary-host";
  primary->pushPort = 1001;
  primary->fetchPort = 1002;
  primary->replicatePort = 1003;
  primary->mode = PartitionLocation::PRIMARY;
  primary->storageInfo = std::make_unique<StorageInfo>();
  primary->storageInfo->type = StorageInfo::HDD;

  auto replica = std::make_unique<PartitionLocation>();
  replica->id = 0;
  replica->epoch = 0;
  replica->host = "replica-host";
  replica->pushPort = 2001;
  replica->fetchPort = 2002;
  replica->replicatePort = 2003;
  replica->mode = PartitionLocation::REPLICA;
  replica->storageInfo = std::make_unique<StorageInfo>();
  replica->storageInfo->type = StorageInfo::HDD;

  primary->replicaPeer = std::move(replica);
  return primary;
}

std::shared_ptr<PartitionLocation> makeLocationWithoutPeer() {
  auto location = std::make_shared<PartitionLocation>();
  location->id = 0;
  location->epoch = 0;
  location->host = "solo-host";
  location->pushPort = 3001;
  location->fetchPort = 3002;
  location->replicatePort = 3003;
  location->mode = PartitionLocation::PRIMARY;
  location->storageInfo = std::make_unique<StorageInfo>();
  location->storageInfo->type = StorageInfo::HDD;
  return location;
}

std::shared_ptr<CelebornConf> makeTestConf(bool replicateEnabled = true) {
  auto conf = std::make_shared<CelebornConf>();
  conf->registerProperty(CelebornConf::kNetworkIoRetryWait, "1ms");
  conf->registerProperty(
      CelebornConf::kClientFetchMaxRetriesForEachReplica, "2");
  conf->registerProperty(
      CelebornConf::kClientPushReplicateEnabled,
      replicateEnabled ? "true" : "false");
  conf->registerProperty(
      CelebornConf::kClientFetchExcludeWorkerOnFailureEnabled, "true");
  return conf;
}
// Creates a valid PbStreamHandler RpcResponse for WorkerPartitionReader
// construction. Each copy is safe to use independently (RpcResponse clones
// the body on copy).
RpcResponse makeStreamHandlerResponse(int numChunks = 1) {
  PbStreamHandler pb;
  pb.set_streamid(100);
  pb.set_numchunks(numChunks);
  for (int i = 0; i < numChunks; i++) {
    pb.add_chunkoffsets(i);
  }
  pb.set_fullpath("test-fullpath");
  TransportMessage transportMessage(STREAM_HANDLER, pb.SerializeAsString());
  return RpcResponse(1111, transportMessage.toReadOnlyByteBuffer());
}

// Creates a chunk buffer
std::unique_ptr<ReadOnlyByteBuffer> makeChunkBuffer(
    int mapId,
    int attemptId,
    int batchId,
    const std::string& payload) {
  const size_t totalSize = 4 * sizeof(int) + payload.size();
  auto buffer = ByteBuffer::createWriteOnly(totalSize, false);
  buffer->writeLE<int>(mapId);
  buffer->writeLE<int>(attemptId);
  buffer->writeLE<int>(batchId);
  buffer->writeLE<int>(static_cast<int>(payload.size()));
  buffer->writeFromString(payload);
  return ByteBuffer::toReadOnly(std::move(buffer));
}

// A TransportClient whose sendRpcRequestSync always returns a valid
// stream handler, and whose fetchChunkAsync walks through a pre-configured
// sequence of success/failure behaviors. Used to exercise the getNextChunk()
// retry loop independently of reader-creation failures.
class SequencedMockTransportClient : public TransportClient {
 public:
  SequencedMockTransportClient()
      : TransportClient(nullptr, nullptr, MS(100)),
        streamHandlerResponse_(makeStreamHandlerResponse()) {}

  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout)
      override {
    return streamHandlerResponse_;
  }

  void sendRpcRequestWithoutResponse(const RpcRequest& request) override {}

  void fetchChunkAsync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      FetchChunkSuccessCallback onSuccess,
      FetchChunkFailureCallback onFailure) override {
    auto idx = fetchCallIdx_++;
    if (idx < fetchBehaviors_.size()) {
      fetchBehaviors_[idx](streamChunkSlice, onSuccess, onFailure);
    }
  }

  using FetchBehavior = std::function<void(
      const StreamChunkSlice&,
      FetchChunkSuccessCallback,
      FetchChunkFailureCallback)>;

  void addFetchSuccess(std::unique_ptr<ReadOnlyByteBuffer> chunk) {
    auto iobuf = std::shared_ptr<folly::IOBuf>(chunk->getData());
    fetchBehaviors_.push_back([iobuf](
                                  const StreamChunkSlice& slice,
                                  FetchChunkSuccessCallback onSuccess,
                                  FetchChunkFailureCallback) {
      onSuccess(slice, ByteBuffer::createReadOnly(iobuf->clone(), false));
    });
  }

  void addFetchFailure(const std::string& errorMessage) {
    fetchBehaviors_.push_back([errorMessage](
                                  const StreamChunkSlice& slice,
                                  FetchChunkSuccessCallback,
                                  FetchChunkFailureCallback onFailure) {
      onFailure(slice, std::make_unique<std::runtime_error>(errorMessage));
    });
  }

 private:
  RpcResponse streamHandlerResponse_;
  std::vector<FetchBehavior> fetchBehaviors_;
  size_t fetchCallIdx_{0};
};

class SequencedMockClientFactory : public TransportClientFactory {
 public:
  explicit SequencedMockClientFactory(
      std::shared_ptr<SequencedMockTransportClient> client)
      : TransportClientFactory(std::make_shared<CelebornConf>()),
        client_(std::move(client)) {}

  std::shared_ptr<TransportClient> createClient(
      const std::string& host,
      uint16_t port) override {
    hosts_.push_back(host);
    return client_;
  }

  const std::vector<std::string>& hosts() const {
    return hosts_;
  }

 private:
  std::shared_ptr<SequencedMockTransportClient> client_;
  std::vector<std::string> hosts_;
};
} // namespace

// Verifies that createReaderWithRetry exhausts all retries and throws.
TEST(CelebornInputStreamRetryTest, allRetriesExhaustedThrows) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::system_error(
          std::make_error_code(std::errc::connection_refused))));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  auto conf = makeTestConf();
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  EXPECT_THROW(
      CelebornInputStream(
          "test-shuffle-key",
          conf,
          factory,
          std::move(locations),
          attempts,
          0,
          0,
          100,
          false,
          excludedWorkers,
          &shuffleClient),
      std::exception);
}

// Verifies that on failure, the retry logic switches from primary to replica.
TEST(CelebornInputStreamRetryTest, switchesToPeerOnFailure) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::system_error(
          std::make_error_code(std::errc::connection_refused))));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  auto conf = makeTestConf();
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  try {
    CelebornInputStream(
        "test-shuffle-key",
        conf,
        factory,
        std::move(locations),
        attempts,
        0,
        0,
        100,
        false,
        excludedWorkers,
        &shuffleClient);
  } catch (...) {
    // Expected to throw after exhausting retries
  }

  auto& hosts = factory->hosts();
  // First attempt on primary, then switches to replica
  ASSERT_GE(hosts.size(), 2u);
  EXPECT_EQ(hosts[0], "primary-host");
  EXPECT_EQ(hosts[1], "replica-host");
}

// Verifies that critical failures cause workers to be added to the
// exclusion list.
TEST(CelebornInputStreamRetryTest, excludesCriticalFailures) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::system_error(
          std::make_error_code(std::errc::connection_refused))));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  auto conf = makeTestConf(true);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  try {
    CelebornInputStream(
        "test-shuffle-key",
        conf,
        factory,
        std::move(locations),
        attempts,
        0,
        0,
        100,
        false,
        excludedWorkers,
        &shuffleClient);
  } catch (...) {
  }

  // Both primary and replica should have been excluded (critical failure)
  EXPECT_TRUE(excludedWorkers->get("primary-host:1002").has_value());
  EXPECT_TRUE(excludedWorkers->get("replica-host:2002").has_value());
}

// Verifies that non-critical failures do NOT cause worker exclusion,
// matching the isCriticalCauseForFetch filtering behavior.
TEST(CelebornInputStreamRetryTest, doesNotExcludeNonCriticalFailures) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::runtime_error("LZ4 decompression failed")));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  auto conf = makeTestConf(true);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  try {
    CelebornInputStream(
        "test-shuffle-key",
        conf,
        factory,
        std::move(locations),
        attempts,
        0,
        0,
        100,
        false,
        excludedWorkers,
        &shuffleClient);
  } catch (...) {
  }

  // Non-critical failure should NOT exclude workers
  EXPECT_FALSE(excludedWorkers->get("primary-host:1002").has_value());
  EXPECT_FALSE(excludedWorkers->get("replica-host:2002").has_value());
}

// Verifies that without a peer, all retries target the same location.
TEST(CelebornInputStreamRetryTest, noPeerRetriesSameLocation) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::system_error(
          std::make_error_code(std::errc::connection_refused))));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  // Replication disabled: maxRetry = 2
  auto conf = makeTestConf(false);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithoutPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  try {
    CelebornInputStream(
        "test-shuffle-key",
        conf,
        factory,
        std::move(locations),
        attempts,
        0,
        0,
        100,
        false,
        excludedWorkers,
        &shuffleClient);
  } catch (...) {
  }

  // All retries should target the same host
  for (const auto& host : factory->hosts()) {
    EXPECT_EQ(host, "solo-host");
  }
  // maxRetry = 2
  EXPECT_EQ(factory->hosts().size(), 2u);
}

// Verifies that with replication enabled, maxRetry is doubled.
TEST(CelebornInputStreamRetryTest, replicationDoublesMaxRetries) {
  auto client = std::make_shared<FailingTransportClient>(
      std::make_exception_ptr(std::system_error(
          std::make_error_code(std::errc::connection_refused))));
  auto factory = std::make_shared<TrackingTransportClientFactory>(client);
  // Replication enabled: maxRetry = 2 * 2 = 4
  auto conf = makeTestConf(true);
  conf->registerProperty(
      CelebornConf::kClientFetchExcludeWorkerOnFailureEnabled, "false");
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  try {
    CelebornInputStream(
        "test-shuffle-key",
        conf,
        factory,
        std::move(locations),
        attempts,
        0,
        0,
        100,
        false,
        excludedWorkers,
        &shuffleClient);
  } catch (...) {
  }

  // With maxRetriesForEachReplica=2 and replication enabled,
  // fetchChunkMaxRetry = 2 * 2 = 4 total attempts
  EXPECT_EQ(factory->hosts().size(), 4u);
}

// getNextChunk() retry tests
// These tests exercise the retry loop inside getNextChunk(), which is
// triggered when a successfully-created reader's next() call fails during
// chunk fetching.

// Verifies that when a chunk fetch fails on the primary, getNextChunk()
// switches to the peer replica and successfully reads data on retry.
TEST(CelebornInputStreamRetryTest, fetchChunkRetrySucceedsWithPeerSwitch) {
  auto mockClient = std::make_shared<SequencedMockTransportClient>();
  mockClient->addFetchFailure("chunk fetch failed");
  const std::string payload = "hello";
  mockClient->addFetchSuccess(makeChunkBuffer(0, 0, 0, payload));

  auto factory = std::make_shared<SequencedMockClientFactory>(mockClient);
  auto conf = makeTestConf(true);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  CelebornInputStream stream(
      "test-shuffle-key",
      conf,
      factory,
      std::move(locations),
      attempts,
      0,
      0,
      100,
      false,
      excludedWorkers,
      &shuffleClient);

  std::vector<uint8_t> buffer(payload.size());
  int bytesRead = stream.read(buffer.data(), 0, payload.size());
  EXPECT_EQ(bytesRead, payload.size());
  EXPECT_EQ(std::string(buffer.begin(), buffer.end()), payload);

  auto& hosts = factory->hosts();
  ASSERT_GE(hosts.size(), 2u);
  EXPECT_EQ(hosts[0], "primary-host");
  EXPECT_EQ(hosts[1], "replica-host");
}

// Verifies that getNextChunk() throws after exhausting all chunk-fetch retries.
TEST(CelebornInputStreamRetryTest, fetchChunkRetryExhaustsAllRetries) {
  auto mockClient = std::make_shared<SequencedMockTransportClient>();
  for (int i = 0; i < 4; i++) {
    mockClient->addFetchFailure("chunk fetch failed");
  }

  auto factory = std::make_shared<SequencedMockClientFactory>(mockClient);
  auto conf = makeTestConf(true);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  EXPECT_THROW(
      CelebornInputStream(
          "test-shuffle-key",
          conf,
          factory,
          std::move(locations),
          attempts,
          0,
          0,
          100,
          false,
          excludedWorkers,
          &shuffleClient),
      std::exception);

  auto& hosts = factory->hosts();
  EXPECT_EQ(hosts.size(), 4u);
  EXPECT_EQ(hosts[0], "primary-host");
  for (size_t i = 1; i < hosts.size(); i++) {
    EXPECT_EQ(hosts[i], "replica-host");
  }
}

// Verifies that without a peer, getNextChunk() retries the same location
// and succeeds on the second attempt.
TEST(CelebornInputStreamRetryTest, fetchChunkRetryNoPeerRetriesSameLocation) {
  auto mockClient = std::make_shared<SequencedMockTransportClient>();
  mockClient->addFetchFailure("chunk fetch failed");
  const std::string payload = "world";
  mockClient->addFetchSuccess(makeChunkBuffer(0, 0, 0, payload));

  auto factory = std::make_shared<SequencedMockClientFactory>(mockClient);
  auto conf = makeTestConf(false);
  auto excludedWorkers =
      std::make_shared<CelebornInputStream::FetchExcludedWorkers>();
  StubShuffleClient shuffleClient(conf, excludedWorkers);

  auto location = makeLocationWithoutPeer();
  std::vector<std::shared_ptr<const PartitionLocation>> locations;
  locations.push_back(std::move(location));
  std::vector<int> attempts = {0};

  CelebornInputStream stream(
      "test-shuffle-key",
      conf,
      factory,
      std::move(locations),
      attempts,
      0,
      0,
      100,
      false,
      excludedWorkers,
      &shuffleClient);

  std::vector<uint8_t> buffer(payload.size());
  int bytesRead = stream.read(buffer.data(), 0, payload.size());
  EXPECT_EQ(bytesRead, payload.size());
  EXPECT_EQ(std::string(buffer.begin(), buffer.end()), payload);

  for (const auto& host : factory->hosts()) {
    EXPECT_EQ(host, "solo-host");
  }
  EXPECT_EQ(factory->hosts().size(), 2u);
}