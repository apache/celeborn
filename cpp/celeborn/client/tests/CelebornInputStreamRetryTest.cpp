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