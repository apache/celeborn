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

#include "celeborn/network/NettyRpcEndpointRef.h"

using namespace celeborn;
using namespace celeborn::network;

namespace {
using MS = std::chrono::milliseconds;

class MockTransportClient : public TransportClient {
 public:
  MockTransportClient()
      : TransportClient(nullptr, nullptr, MS(100)),
        response_(
            RpcResponse(0, memory::ReadOnlyByteBuffer::createEmptyBuffer())),
        request_(
            RpcRequest(0, memory::ReadOnlyByteBuffer::createEmptyBuffer())) {}
  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout)
      override {
    request_ = request;
    return response_;
  }

  void setResponse(const RpcResponse& response) {
    response_ = response;
  }

  RpcRequest getRequest() {
    return request_;
  }

 private:
  RpcResponse response_;
  RpcRequest request_;
};

void readUTF(memory::ReadOnlyByteBuffer& buffer, std::string& host) {
  int size = buffer.read<short>();
  host = buffer.readToString(size);
}

void readRpcAddress(
    memory::ReadOnlyByteBuffer& buffer,
    std::string& host,
    int& port) {
  EXPECT_EQ(buffer.read<uint8_t>(), 1);
  readUTF(buffer, host);
  port = buffer.read<int32_t>();
}
} // namespace

TEST(NettyRpcEndpointRefTest, askSyncGetReducerFileGroup) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient);

  const std::string responseBody = "test-response-body";

  // rpcResponse -> transportMessage -> getReducerFileGroupResponse
  PbGetReducerFileGroupResponse pbGetReducerFileGroupResponse;
  const int status = 5;
  pbGetReducerFileGroupResponse.set_status(5);
  protocol::TransportMessage transportMessage(
      GET_REDUCER_FILE_GROUP_RESPONSE,
      pbGetReducerFileGroupResponse.SerializeAsString());
  auto msgBody = transportMessage.toReadOnlyByteBuffer();
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(sizeof(uint8_t));
  writeBuffer->write<uint8_t>(NettyRpcEndpointRef::kNativeTransportMessageFlag);
  auto flagBody = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto concatBody = memory::ByteBuffer::concat(*flagBody, *msgBody);

  auto rpcResponse = std::make_unique<RpcResponse>(1000, std::move(concatBody));
  mockedClient->setResponse(*rpcResponse);

  protocol::GetReducerFileGroup request{1001};
  auto response = nettyRpcEndpointRef.askSync(request, MS(100));
  EXPECT_EQ(response->status, status);

  auto sentRequest = mockedClient->getRequest();
  auto sentBody = sentRequest.body();
  std::string host;
  int port;
  readRpcAddress(*sentBody, host, port);
  EXPECT_EQ(host, srcHost);
  EXPECT_EQ(port, srcPort);
  readRpcAddress(*sentBody, host, port);
  EXPECT_EQ(host, dstHost);
  EXPECT_EQ(port, dstPort);
  std::string name;
  readUTF(*sentBody, name);
  EXPECT_EQ(name, srcName);
  EXPECT_EQ(
      sentBody->read<uint8_t>(),
      NettyRpcEndpointRef::kNativeTransportMessageFlag);
}
