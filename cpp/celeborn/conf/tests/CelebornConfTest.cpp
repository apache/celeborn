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
#include <chrono>
#include <fstream>

#include "celeborn/conf/CelebornConf.h"
#include "celeborn/protocol/CompressionCodec.h"

using namespace celeborn::conf;
using namespace celeborn::protocol;

using CelebornUserError = celeborn::utils::CelebornUserError;
using SECOND = std::chrono::seconds;
using MILLISENCOND = std::chrono::milliseconds;

namespace {
void writeToFile(
    const std::string& filename,
    const std::vector<std::string>& lines) {
  std::ofstream file;
  file.open(filename);
  for (auto& line : lines) {
    file << line << "\n";
  }
  file.close();
}
} // namespace

void testDefaultValues(CelebornConf* conf) {
  EXPECT_EQ(conf->rpcLookupTimeout(), SECOND(30));
  EXPECT_EQ(conf->clientRpcGetReducerFileGroupRpcAskTimeout(), SECOND(60));
  EXPECT_EQ(conf->networkConnectTimeout(), SECOND(10));
  EXPECT_EQ(conf->clientFetchTimeout(), SECOND(600));
  EXPECT_EQ(conf->networkIoNumConnectionsPerPeer(), 1);
  EXPECT_EQ(conf->networkIoClientThreads(), 0);
  EXPECT_EQ(conf->clientFetchMaxReqsInFlight(), 3);
  EXPECT_EQ(conf->shuffleCompressionCodec(), CompressionCodec::NONE);
  EXPECT_EQ(conf->shuffleCompressionZstdCompressLevel(), 1);
  EXPECT_EQ(conf->clientFetchMaxRetriesForEachReplica(), 3);
  EXPECT_EQ(conf->networkIoRetryWait(), SECOND(5));
  EXPECT_FALSE(conf->clientPushReplicateEnabled());
  EXPECT_FALSE(conf->clientFetchExcludeWorkerOnFailureEnabled());
  EXPECT_EQ(conf->clientFetchExcludedWorkerExpireTimeout(), SECOND(60));
  EXPECT_FALSE(conf->clientAdaptiveOptimizeSkewedPartitionReadEnabled());
}

TEST(CelebornConfTest, defaultValues) {
  auto conf = std::make_unique<CelebornConf>();
  testDefaultValues(conf.get());
}

TEST(CelebornConfTest, setValues) {
  auto conf = std::make_unique<CelebornConf>();
  testDefaultValues(conf.get());

  conf->registerProperty(CelebornConf::kRpcLookupTimeout, "10s");
  EXPECT_EQ(conf->rpcLookupTimeout(), SECOND(10));
  conf->registerProperty(
      CelebornConf::kClientRpcGetReducerFileGroupRpcAskTimeout, "10s");
  EXPECT_EQ(conf->clientRpcGetReducerFileGroupRpcAskTimeout(), SECOND(10));
  conf->registerProperty(CelebornConf::kNetworkConnectTimeout, "1000ms");
  EXPECT_EQ(conf->networkConnectTimeout(), SECOND(1));
  conf->registerProperty(CelebornConf::kClientFetchTimeout, "10ms");
  EXPECT_EQ(conf->clientFetchTimeout(), MILLISENCOND(10));
  conf->registerProperty(CelebornConf::kNetworkIoNumConnectionsPerPeer, "10");
  EXPECT_EQ(conf->networkIoNumConnectionsPerPeer(), 10);
  conf->registerProperty(CelebornConf::kNetworkIoClientThreads, "10");
  EXPECT_EQ(conf->networkIoClientThreads(), 10);
  conf->registerProperty(CelebornConf::kClientFetchMaxReqsInFlight, "10");
  EXPECT_EQ(conf->clientFetchMaxReqsInFlight(), 10);
  conf->registerProperty(CelebornConf::kShuffleCompressionCodec, "LZ4");
  EXPECT_EQ(conf->shuffleCompressionCodec(), CompressionCodec::LZ4);
  conf->registerProperty(CelebornConf::kShuffleCompressionCodec, "ZSTD");
  EXPECT_EQ(conf->shuffleCompressionCodec(), CompressionCodec::ZSTD);
  conf->registerProperty(CelebornConf::kShuffleCompressionCodec, "NONE");
  EXPECT_EQ(conf->shuffleCompressionCodec(), CompressionCodec::NONE);
  conf->registerProperty(
      CelebornConf::kShuffleCompressionZstdCompressLevel, "5");
  EXPECT_EQ(conf->shuffleCompressionZstdCompressLevel(), 5);
  conf->registerProperty(
      CelebornConf::kClientFetchMaxRetriesForEachReplica, "5");
  EXPECT_EQ(conf->clientFetchMaxRetriesForEachReplica(), 5);
  conf->registerProperty(CelebornConf::kNetworkIoRetryWait, "10s");
  EXPECT_EQ(conf->networkIoRetryWait(), SECOND(10));
  conf->registerProperty(CelebornConf::kClientPushReplicateEnabled, "true");
  EXPECT_TRUE(conf->clientPushReplicateEnabled());
  conf->registerProperty(CelebornConf::kClientPushReplicateEnabled, "false");
  EXPECT_FALSE(conf->clientPushReplicateEnabled());
  conf->registerProperty(
      CelebornConf::kClientFetchExcludeWorkerOnFailureEnabled, "true");
  EXPECT_TRUE(conf->clientFetchExcludeWorkerOnFailureEnabled());
  conf->registerProperty(
      CelebornConf::kClientFetchExcludeWorkerOnFailureEnabled, "false");
  EXPECT_FALSE(conf->clientFetchExcludeWorkerOnFailureEnabled());
  conf->registerProperty(
      CelebornConf::kClientFetchExcludedWorkerExpireTimeout, "30s");
  EXPECT_EQ(conf->clientFetchExcludedWorkerExpireTimeout(), SECOND(30));
  conf->registerProperty(
      CelebornConf::kClientAdaptiveOptimizeSkewedPartitionReadEnabled, "true");
  EXPECT_TRUE(conf->clientAdaptiveOptimizeSkewedPartitionReadEnabled());
  conf->registerProperty(
      CelebornConf::kClientAdaptiveOptimizeSkewedPartitionReadEnabled, "false");
  EXPECT_FALSE(conf->clientAdaptiveOptimizeSkewedPartitionReadEnabled());

  EXPECT_THROW(
      conf->registerProperty("non-exist-key", "non-exist-value"),
      CelebornUserError);
}

TEST(CelebornConfTest, readFromFile) {
  std::vector<std::string> lines;
  lines.push_back(std::string(CelebornConf::kRpcLookupTimeout) + " = 10s");
  lines.push_back(
      std::string(CelebornConf::kNetworkIoNumConnectionsPerPeer) + " = 10");
  const std::string filename = "/tmp/test.conf";
  writeToFile(filename, lines);

  auto conf = std::make_unique<CelebornConf>(filename);
  // The specified configs in file have higher priority.
  EXPECT_EQ(conf->rpcLookupTimeout(), SECOND(10));
  EXPECT_EQ(conf->networkIoNumConnectionsPerPeer(), 10);
  // The unspecified configs should have default values.
  EXPECT_EQ(conf->clientRpcGetReducerFileGroupRpcAskTimeout(), SECOND(60));
  EXPECT_EQ(conf->networkConnectTimeout(), SECOND(10));

  // The registerProperty could rewrite the configs.
  conf->registerProperty(CelebornConf::kRpcLookupTimeout, "5s");
  EXPECT_EQ(conf->rpcLookupTimeout(), SECOND(5));
  conf->registerProperty(CelebornConf::kNetworkIoNumConnectionsPerPeer, "5");
  EXPECT_EQ(conf->networkIoNumConnectionsPerPeer(), 5);
}