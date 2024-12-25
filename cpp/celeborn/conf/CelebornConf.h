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
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
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
  static const std::unordered_map<std::string, folly::Optional<std::string>>
      kDefaultProperties;

  static constexpr std::string_view kRpcLookupTimeout{
      "celeborn.rpc.lookupTimeout"};

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

  CelebornConf();

  CelebornConf(const std::string& filename);

  CelebornConf(const CelebornConf& other);

  CelebornConf(CelebornConf&& other) = delete;

  void registerProperty(const std::string_view& key, const std::string& value);

  Timeout rpcLookupTimeout() const;

  Timeout clientRpcGetReducerFileGroupRpcAskTimeout() const;

  Timeout networkConnectTimeout() const;

  Timeout clientFetchTimeout() const;

  int networkIoNumConnectionsPerPeer() const;

  int networkIoClientThreads() const;

  int clientFetchMaxReqsInFlight() const;
};
} // namespace celeborn
