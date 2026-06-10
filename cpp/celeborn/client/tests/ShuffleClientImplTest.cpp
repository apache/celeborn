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

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/tests/ShuffleClientTestUtils.h"

using namespace celeborn;
using namespace celeborn::client;

namespace {
// Subclass that exposes the protected push-failure / worker-exclusion helpers
// of ShuffleClientImpl so the pure-logic methods can be unit-tested directly.
class TestableShuffleClient : public TestShuffleClientBase {
 public:
  static std::shared_ptr<TestableShuffleClient> create(
      const std::shared_ptr<const conf::CelebornConf>& conf) {
    return std::shared_ptr<TestableShuffleClient>(
        new TestableShuffleClient(conf));
  }

  static protocol::StatusCode callGetPushDataFailCause(
      const std::string& message) {
    return ShuffleClientImpl::getPushDataFailCause(message);
  }

  void callExcludeWorkerByCause(
      protocol::StatusCode cause,
      const std::shared_ptr<const protocol::PartitionLocation>& oldLocation) {
    excludeWorkerByCause(cause, oldLocation);
  }

  std::optional<protocol::StatusCode> callGetPushTargetWorkerExcludeCause(
      const protocol::PartitionLocation& location) {
    return getPushTargetWorkerExcludeCause(location);
  }

 private:
  explicit TestableShuffleClient(
      const std::shared_ptr<const conf::CelebornConf>& conf)
      : TestShuffleClientBase(conf) {}
};

std::shared_ptr<conf::CelebornConf> makeConf(bool pushExcludeEnabled) {
  auto conf = std::make_shared<conf::CelebornConf>();
  conf->registerProperty(
      conf::CelebornConf::kClientPushExcludeWorkerOnFailureEnabled,
      pushExcludeEnabled ? "true" : "false");
  return conf;
}

// Builds a PRIMARY location, optionally with a REPLICA peer on a different
// host:port so primary/peer exclusion can be told apart.
std::shared_ptr<protocol::PartitionLocation> makeLocation(
    int id,
    int epoch,
    const std::string& host,
    int pushPort,
    bool withPeer = false,
    const std::string& peerHost = "",
    int peerPushPort = 0) {
  auto loc = std::make_shared<protocol::PartitionLocation>();
  loc->id = id;
  loc->epoch = epoch;
  loc->host = host;
  loc->pushPort = pushPort;
  loc->mode = protocol::PartitionLocation::PRIMARY;
  if (withPeer) {
    loc->replicaPeer = std::make_unique<protocol::PartitionLocation>();
    loc->replicaPeer->id = id;
    loc->replicaPeer->epoch = epoch;
    loc->replicaPeer->host = peerHost;
    loc->replicaPeer->pushPort = peerPushPort;
    loc->replicaPeer->mode = protocol::PartitionLocation::REPLICA;
  }
  return loc;
}
} // namespace

// getPushDataFailCause maps a transport error message to a specific StatusCode
// by substring match. The check order must let more specific causes win.
TEST(ShuffleClientImplTest, getPushDataFailCauseKnownCauses) {
  using SC = protocol::StatusCode;
  const std::vector<std::pair<std::string, SC>> cases = {
      // Empty / unknown messages fall back to the non-critical primary cause.
      {"", SC::PUSH_DATA_FAIL_NON_CRITICAL_CAUSE},
      {"some totally unrelated error", SC::PUSH_DATA_FAIL_NON_CRITICAL_CAUSE},
      // Named causes, each wrapped by surrounding text like the transport does.
      {"PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA: x",
       SC::PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA},
      {"PUSH_DATA_WRITE_FAIL_REPLICA: x", SC::PUSH_DATA_WRITE_FAIL_REPLICA},
      {"PUSH_DATA_WRITE_FAIL_PRIMARY: x", SC::PUSH_DATA_WRITE_FAIL_PRIMARY},
      {"PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY: x",
       SC::PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY},
      {"PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA: x",
       SC::PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA},
      {"PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY: x",
       SC::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY},
      {"PUSH_DATA_CONNECTION_EXCEPTION_REPLICA: x",
       SC::PUSH_DATA_CONNECTION_EXCEPTION_REPLICA},
      {"PUSH_DATA_TIMEOUT_PRIMARY: x", SC::PUSH_DATA_TIMEOUT_PRIMARY},
      {"PUSH_DATA_TIMEOUT_REPLICA: x", SC::PUSH_DATA_TIMEOUT_REPLICA},
      {"REPLICATE_DATA_FAILED: x", SC::REPLICATE_DATA_FAILED},
      {"PUSH_DATA_PRIMARY_WORKER_EXCLUDED",
       SC::PUSH_DATA_PRIMARY_WORKER_EXCLUDED},
      {"PUSH_DATA_REPLICA_WORKER_EXCLUDED",
       SC::PUSH_DATA_REPLICA_WORKER_EXCLUDED},
      {"PUSH_DATA_FAIL_PARTITION_NOT_FOUND: x",
       SC::PUSH_DATA_FAIL_PARTITION_NOT_FOUND},
  };
  for (const auto& [message, expected] : cases) {
    EXPECT_EQ(
        TestableShuffleClient::callGetPushDataFailCause(message), expected)
        << "message: " << message;
  }
}

// A client-side push timeout surfaces as a folly FutureTimeout whose message
// carries no StatusCode token; it must classify as PUSH_DATA_TIMEOUT_PRIMARY
// (an excludable cause) like the Java client, not degrade to non-critical.
TEST(ShuffleClientImplTest, getPushDataFailCauseFollyTimeout) {
  using SC = protocol::StatusCode;
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "folly::FutureTimeout: Timed out"),
      SC::PUSH_DATA_TIMEOUT_PRIMARY);
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause("Timed out"),
      SC::PUSH_DATA_TIMEOUT_PRIMARY);
}

// connectFail patterns (no StatusCode name present) classify as a primary
// connection exception, mirroring ExceptionUtils#connectFail in Java.
TEST(ShuffleClientImplTest, getPushDataFailCauseConnectFail) {
  using SC = protocol::StatusCode;
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "Connection from host-1:9099 closed"),
      SC::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "java.io.IOException: Connection reset by peer"),
      SC::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "Failed to send request 42 to host-1:9099"),
      SC::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY);
}

// A REPLICA token must not be misread as its PRIMARY sibling, and a more
// specific named cause wins over a trailing connectFail phrase.
TEST(ShuffleClientImplTest, getPushDataFailCauseOrdering) {
  using SC = protocol::StatusCode;
  // The PRIMARY check (substring) must not match the REPLICA name.
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "PUSH_DATA_TIMEOUT_REPLICA: timed out"),
      SC::PUSH_DATA_TIMEOUT_REPLICA);
  // Named cause present alongside a connectFail phrase: the name wins.
  EXPECT_EQ(
      TestableShuffleClient::callGetPushDataFailCause(
          "PUSH_DATA_WRITE_FAIL_PRIMARY: Connection reset by peer"),
      SC::PUSH_DATA_WRITE_FAIL_PRIMARY);
}

// When clientPushExcludeWorkerOnFailureEnabled is off, exclusion is a no-op and
// the exclude-cause lookup always returns nullopt.
TEST(ShuffleClientImplTest, excludeWorkerDisabledIsNoOp) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/false));
  auto loc = makeLocation(1, 0, "primary-host", 9001);

  client->callExcludeWorkerByCause(
      protocol::StatusCode::PUSH_DATA_TIMEOUT_PRIMARY, loc);

  EXPECT_FALSE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());
}

// A primary connection/timeout cause excludes the primary worker; a later push
// to that location reports PUSH_DATA_PRIMARY_WORKER_EXCLUDED.
TEST(ShuffleClientImplTest, excludeWorkerByPrimaryCause) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/true));
  auto loc = makeLocation(1, 0, "primary-host", 9001);
  auto other = makeLocation(2, 0, "other-host", 9009);

  client->callExcludeWorkerByCause(
      protocol::StatusCode::PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY, loc);

  auto cause = client->callGetPushTargetWorkerExcludeCause(*loc);
  ASSERT_TRUE(cause.has_value());
  EXPECT_EQ(*cause, protocol::StatusCode::PUSH_DATA_PRIMARY_WORKER_EXCLUDED);
  // A different, non-excluded worker is unaffected.
  EXPECT_FALSE(client->callGetPushTargetWorkerExcludeCause(*other).has_value());
}

// A replica connection/timeout cause excludes the peer worker; pushing to the
// primary then reports PUSH_DATA_REPLICA_WORKER_EXCLUDED (the primary itself is
// still healthy).
TEST(ShuffleClientImplTest, excludeWorkerByReplicaCause) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/true));
  auto loc = makeLocation(
      1,
      0,
      "primary-host",
      9001,
      /*withPeer=*/true,
      "replica-host",
      9002);

  client->callExcludeWorkerByCause(
      protocol::StatusCode::PUSH_DATA_TIMEOUT_REPLICA, loc);

  auto cause = client->callGetPushTargetWorkerExcludeCause(*loc);
  ASSERT_TRUE(cause.has_value());
  EXPECT_EQ(*cause, protocol::StatusCode::PUSH_DATA_REPLICA_WORKER_EXCLUDED);
}

// Non-connection causes (e.g. HARD_SPLIT) never exclude a worker.
TEST(ShuffleClientImplTest, excludeWorkerIgnoresNonConnectionCause) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/true));
  auto loc = makeLocation(1, 0, "primary-host", 9001);

  client->callExcludeWorkerByCause(protocol::StatusCode::HARD_SPLIT, loc);

  EXPECT_FALSE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());
}

// Worker exclusion is client-wide: cleaning up one shuffle must not drop
// exclusions other live shuffles rely on (the Java client never touches
// pushExcludedWorkers in cleanupShuffle).
TEST(ShuffleClientImplTest, cleanupShuffleKeepsExcludedWorkers) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/true));
  auto loc = makeLocation(1, 0, "primary-host", 9001);

  client->callExcludeWorkerByCause(
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY, loc);
  ASSERT_TRUE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());

  EXPECT_TRUE(client->cleanupShuffle(/*shuffleId=*/1000));

  EXPECT_TRUE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());
}

// shutdown drops all recorded push-excluded workers, like the Java client.
TEST(ShuffleClientImplTest, shutdownClearsExcludedWorkers) {
  auto client = TestableShuffleClient::create(makeConf(/*enabled=*/true));
  auto loc = makeLocation(1, 0, "primary-host", 9001);

  client->callExcludeWorkerByCause(
      protocol::StatusCode::PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY, loc);
  ASSERT_TRUE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());

  client->shutdown();

  EXPECT_FALSE(client->callGetPushTargetWorkerExcludeCause(*loc).has_value());
}
