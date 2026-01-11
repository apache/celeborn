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

#include "celeborn/conf/BaseConf.h"
#include "celeborn/protocol/CompressionCodec.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
using Timeout = utils::Timeout;
namespace conf {
/***
 * steps to add a new config:
 * === in CelebornConf.h:
 *     1. define the configName with "static constexpr std::string_view";
 *     2. declare the getter method within class;
 * === in CelebornConf.cpp:
 *     3. register the configName in CelebornConf's constructor, with proper
 *        data type and proper default value;
 *     4. implement the getter method.
 */

class CelebornConf : public BaseConf {
 public:
  static const std::unordered_map<std::string, folly::Optional<std::string>>&
  defaultProperties();

  static constexpr std::string_view kRpcAskTimeout{"celeborn.rpc.askTimeout"};

  static constexpr std::string_view kRpcLookupTimeout{
      "celeborn.rpc.lookupTimeout"};

  static constexpr std::string_view kClientIoConnectionTimeout{
      "celeborn.client.io.connectionTimeout"};

  static constexpr std::string_view kClientRpcRegisterShuffleAskTimeout{
      "celeborn.client.rpc.registerShuffle.askTimeout"};

  static constexpr std::string_view kClientRegisterShuffleMaxRetries{
      "celeborn.client.registerShuffle.maxRetries"};

  static constexpr std::string_view kClientRegisterShuffleRetryWait{
      "celeborn.client.registerShuffle.retryWait"};

  static constexpr std::string_view kClientPushRetryThreads{
      "celeborn.client.push.retry.threads"};

  static constexpr std::string_view kClientPushTimeout{
      "celeborn.client.push.timeout"};

  static constexpr std::string_view kClientPushReviveInterval{
      "celeborn.client.push.revive.interval"};

  static constexpr std::string_view kClientPushReviveBatchSize{
      "celeborn.client.push.revive.batchSize"};

  static constexpr std::string_view kClientPushMaxReviveTimes{
      "celeborn.client.push.revive.maxRetries"};

  static constexpr std::string_view kClientPushLimitStrategy{
      "celeborn.client.push.limit.strategy"};

  static constexpr std::string_view kSimplePushStrategy{"SIMPLE"};

  static constexpr std::string_view kClientPushMaxReqsInFlightPerWorker{
      "celeborn.client.push.maxReqsInFlight.perWorker"};

  static constexpr std::string_view kClientPushMaxReqsInFlightTotal{
      "celeborn.client.push.maxReqsInFlight.total"};

  static constexpr std::string_view kClientPushLimitInFlightTimeoutMs{
      "celeborn.client.push.limit.inFlight.timeout"};

  static constexpr std::string_view kClientPushLimitInFlightSleepDeltaMs{
      "celeborn.client.push.limit.inFlight.sleepInterval"};

  static constexpr std::string_view kClientPushBufferMaxSize{
      "celeborn.client.push.buffer.max.size"};

  static constexpr std::string_view kClientPushMaxBytesSizeInFlightEnabled{
      "celeborn.client.push.maxBytesSizeInFlight.enabled"};

  static constexpr std::string_view kClientPushMaxBytesSizeInFlightTotal{
      "celeborn.client.push.maxBytesSizeInFlight.total"};

  static constexpr std::string_view kClientPushMaxBytesSizeInFlightPerWorker{
      "celeborn.client.push.maxBytesSizeInFlight.perWorker"};

  static constexpr std::string_view
      kClientRpcRequestPartitionLocationAskTimeout{
          "celeborn.client.rpc.requestPartition.askTimeout"};

  static constexpr std::string_view kClientRpcGetReducerFileGroupRpcAskTimeout{
      "celeborn.client.rpc.getReducerFileGroup.askTimeout"};

  static constexpr std::string_view kNetworkConnectTimeout{
      "celeborn.network.connect.timeout"};

  static constexpr std::string_view kClientFetchTimeout{
      "celeborn.client.fetch.timeout"};

  static constexpr std::string_view kNetworkIoNumConnectionsPerPeer{
      "celeborn.data.io.numConnectionsPerPeer"};

  static constexpr std::string_view kNetworkIoClientThreads{
      "celeborn.data.io.clientThreads"};

  static constexpr std::string_view kClientFetchMaxReqsInFlight{
      "celeborn.client.fetch.maxReqsInFlight"};

  static constexpr std::string_view kShuffleCompressionCodec{
      "celeborn.client.shuffle.compression.codec"};

  static constexpr std::string_view kShuffleCompressionZstdCompressLevel{
      "celeborn.client.shuffle.compression.zstd.level"};

  static constexpr std::string_view kClientFetchMaxRetriesForEachReplica{
      "celeborn.client.fetch.maxRetriesForEachReplica"};

  static constexpr std::string_view kDataIoRetryWait{
      "celeborn.data.io.retryWait"};

  static constexpr std::string_view kClientPushReplicateEnabled{
      "celeborn.client.push.replicate.enabled"};

  CelebornConf();

  CelebornConf(const std::string& filename);

  CelebornConf(const CelebornConf& other);

  CelebornConf(CelebornConf&& other) = delete;

  void registerProperty(const std::string_view& key, const std::string& value);

  Timeout rpcAskTimeout() const;

  Timeout rpcLookupTimeout() const;

  Timeout clientIoConnectionTimeout() const;

  Timeout clientRpcRegisterShuffleRpcAskTimeout() const;

  int clientRegisterShuffleMaxRetries() const;

  Timeout clientRegisterShuffleRetryWait() const;

  int clientPushRetryThreads() const;

  Timeout clientPushDataTimeout() const;

  Timeout clientPushReviveInterval() const;

  int clientPushReviveBatchSize() const;

  int clientPushMaxReviveTimes() const;

  std::string clientPushLimitStrategy() const;

  int clientPushMaxReqsInFlightPerWorker() const;

  int clientPushMaxReqsInFlightTotal() const;

  long clientPushLimitInFlightTimeoutMs() const;

  long clientPushLimitInFlightSleepDeltaMs() const;

  int clientPushBufferMaxSize() const;

  bool clientPushMaxBytesSizeInFlightEnabled() const;

  long clientPushMaxBytesSizeInFlightTotal() const;

  long clientPushMaxBytesSizeInFlightPerWorker() const;

  Timeout clientRpcRequestPartitionLocationRpcAskTimeout() const;

  Timeout clientRpcGetReducerFileGroupRpcAskTimeout() const;

  Timeout networkConnectTimeout() const;

  Timeout clientFetchTimeout() const;

  int networkIoNumConnectionsPerPeer() const;

  int networkIoClientThreads() const;

  int clientFetchMaxReqsInFlight() const;

  protocol::CompressionCodec shuffleCompressionCodec() const;

  int shuffleCompressionZstdCompressLevel() const;

  int clientFetchMaxRetriesForEachReplica() const;

  Timeout dataIoRetryWait() const;

  bool clientPushReplicateEnabled() const;
};
} // namespace conf
} // namespace celeborn
