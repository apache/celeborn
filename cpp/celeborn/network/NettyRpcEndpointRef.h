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
      const std::shared_ptr<TransportClient>& client,
      const conf::CelebornConf& conf);

  template <class TRequest, class TResponse>
  std::unique_ptr<TResponse> askSync(const TRequest& msg) {
    return askSync<TRequest, TResponse>(msg, defaultTimeout_);
  }

  template <class TRequest, class TResponse>
  std::unique_ptr<TResponse> askSync(const TRequest& msg, Timeout timeout) {
    auto rpcRequest = buildRpcRequest<TRequest>(msg);
    auto rpcResponse = client_->sendRpcRequestSync(rpcRequest, timeout);
    return fromRpcResponse<TResponse>(std::move(rpcResponse));
  }

 private:
  template <class TRequest>
  RpcRequest buildRpcRequest(const TRequest& msg) {
    auto transportData = msg.toTransportMessage().toReadOnlyByteBuffer();
    int size = srcHost_.size() + 3 + 4 + dstHost_.size() + 3 + 4 +
        name_.size() + 2 + 1;
    auto buffer = memory::ByteBuffer::createWriteOnly(size);
    // write srcAddr msg
    utils::writeRpcAddress(*buffer, srcHost_, srcPort_);
    // write dstAddr msg
    utils::writeRpcAddress(*buffer, dstHost_, dstPort_);
    // write srcName
    utils::writeUTF(*buffer, name_);
    // write the isTransportMessage flag
    buffer->write<uint8_t>(kNativeTransportMessageFlag);
    CELEBORN_CHECK_EQ(buffer->size(), size);
    auto result = memory::ByteBuffer::toReadOnly(std::move(buffer));
    auto combined = memory::ByteBuffer::concat(*result, *transportData);
    return RpcRequest(RpcRequest::nextRequestId(), std::move(combined));
  }

  template <class TResponse>
  std::unique_ptr<TResponse> fromRpcResponse(RpcResponse&& response) {
    auto body = response.body();
    bool isTransportMessage = body->read<uint8_t>();
    CELEBORN_CHECK(isTransportMessage);
    auto transportMessage = protocol::TransportMessage(std::move(body));
    return TResponse::fromTransportMessage(transportMessage);
  }

  std::string name_;
  std::string srcHost_;
  int srcPort_;
  std::string dstHost_;
  int dstPort_;
  std::shared_ptr<TransportClient> client_;
  Timeout defaultTimeout_;
};
} // namespace network
} // namespace celeborn
