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

#include "celeborn/network/NettyRpcEndpointRef.h"

namespace celeborn {
namespace network {

NettyRpcEndpointRef::NettyRpcEndpointRef(
    const std::string& name,
    const std::string& srcHost,
    int srcPort,
    const std::string& dstHost,
    int dstPort,
    std::shared_ptr<TransportClient> client)
    : name_(name),
      srcHost_(srcHost),
      srcPort_(srcPort),
      dstHost_(dstHost),
      dstPort_(dstPort),
      client_(client) {}

std::unique_ptr<protocol::GetReducerFileGroupResponse>
NettyRpcEndpointRef::askSync(
    const protocol::GetReducerFileGroup& msg,
    Timeout timeout) {
  auto rpcRequest = buildRpcRequest(msg);
  auto rpcResponse = client_->sendRpcRequestSync(rpcRequest, timeout);
  return fromRpcResponse(std::move(rpcResponse));
}

RpcRequest NettyRpcEndpointRef::buildRpcRequest(
    const protocol::GetReducerFileGroup& msg) {
  auto transportData = msg.toTransportMessage().toReadOnlyByteBuffer();
  int size =
      srcHost_.size() + 3 + 4 + dstHost_.size() + 3 + 4 + name_.size() + 2 + 1;
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

std::unique_ptr<protocol::GetReducerFileGroupResponse>
NettyRpcEndpointRef::fromRpcResponse(RpcResponse&& response) {
  auto body = response.body();
  uint8_t nativeTransportMessageFlag = body->read<uint8_t>();
  CELEBORN_CHECK_EQ(nativeTransportMessageFlag, kNativeTransportMessageFlag);
  auto transportMessage = protocol::TransportMessage(std::move(body));
  return protocol::GetReducerFileGroupResponse::fromTransportMessage(
      transportMessage);
}
} // namespace network
} // namespace celeborn
