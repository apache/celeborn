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

#include "celeborn/network/TransportClient.h"
#include "celeborn/protocol/ControlMessages.h"

namespace celeborn {
namespace network {
/**
 * RpcEndpointRef is typically used to communicate with LifecycleManager. It
 * wraps around the TransportClient, add ip and name info to the message as
 * the LifecycleManager requires the information.
 */
class NettyRpcEndpointRef {
 public:
  static constexpr uint8_t kNativeTransportMessageFlag = 0xFF;

  NettyRpcEndpointRef(
      const std::string& name,
      const std::string& srcHost,
      int srcPort,
      const std::string& dstHost,
      int dstPort,
      std::shared_ptr<TransportClient> client);

  // TODO: refactor to template function when needed.
  std::unique_ptr<protocol::GetReducerFileGroupResponse> askSync(
      const protocol::GetReducerFileGroup& msg,
      Timeout timeout);

 private:
  RpcRequest buildRpcRequest(const protocol::GetReducerFileGroup& msg);

  std::unique_ptr<protocol::GetReducerFileGroupResponse> fromRpcResponse(
      RpcResponse&& response);

  std::string name_;
  std::string srcHost_;
  int srcPort_;
  std::string dstHost_;
  int dstPort_;
  std::shared_ptr<TransportClient> client_;
};
} // namespace network
} // namespace celeborn
