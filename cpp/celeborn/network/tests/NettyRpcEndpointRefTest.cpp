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
        respPromise_(),
        respFuture_(respPromise_.getFuture()),
        request_(
            RpcRequest(0, memory::ReadOnlyByteBuffer::createEmptyBuffer())) {}
  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout)
      override {
    request_ = request;
    return std::move(respFuture_).get(timeout);
  }

  void setResponse(const RpcResponse& response) {
    respPromise_.setValue(response);
  }

  RpcRequest getRequest() {
    return request_;
  }

 private:
  folly::Promise<RpcResponse> respPromise_;
  folly::Future<RpcResponse> respFuture_;
  RpcRequest request_;
};

std::unique_ptr<RpcResponse> makeResponseForNettyRpcEndpointRef(
    const protocol::TransportMessage& transportMessage,
    long requestId) {
  auto msgBody = transportMessage.toReadOnlyByteBuffer();
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(sizeof(uint8_t));
  writeBuffer->write<uint8_t>(NettyRpcEndpointRef::kNativeTransportMessageFlag);
  auto flagBody = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto concatBody = memory::ByteBuffer::concat(*flagBody, *msgBody);

  return std::make_unique<RpcResponse>(requestId, std::move(concatBody));
}

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

void verifyRequestForNettyRpcEndpointRef(
    const RpcRequest& request,
    const std::string& srcName,
    const std::string& srcHost,
    const int srcPort,
    const std::string& dstHost,
    const int dstPort) {
  auto sentBody = request.body();
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

} // namespace

TEST(NettyRpcEndpointRefTest, askSyncRegisterShuffle) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  const auto conf = std::make_shared<conf::CelebornConf>();
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient, *conf);

  PbRegisterShuffleResponse pbRegisterShuffleResponse;
  const int status = 5;
  pbRegisterShuffleResponse.set_status(5);
  protocol::TransportMessage transportMessage(
      REGISTER_SHUFFLE_RESPONSE, pbRegisterShuffleResponse.SerializeAsString());
  auto rpcResponse = makeResponseForNettyRpcEndpointRef(transportMessage, 1000);
  mockedClient->setResponse(*rpcResponse);

  protocol::RegisterShuffle request{1001};
  auto response = nettyRpcEndpointRef.askSync<
      protocol::RegisterShuffle,
      protocol::RegisterShuffleResponse>(request, MS(100));
  EXPECT_EQ(response->status, status);

  auto sentRequest = mockedClient->getRequest();
  verifyRequestForNettyRpcEndpointRef(
      sentRequest, srcName, srcHost, srcPort, dstHost, dstPort);
}

TEST(NettyRpcEndpointRefTest, askSyncRevive) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  const auto conf = std::make_shared<conf::CelebornConf>();
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient, *conf);

  PbRevive pbRevive;
  pbRevive.set_shuffleid(5);
  protocol::TransportMessage transportMessage(
      CHANGE_LOCATION_RESPONSE, pbRevive.SerializeAsString());
  auto rpcResponse = makeResponseForNettyRpcEndpointRef(transportMessage, 1000);
  mockedClient->setResponse(*rpcResponse);

  protocol::Revive request{1001};
  auto response =
      nettyRpcEndpointRef
          .askSync<protocol::Revive, protocol::ChangeLocationResponse>(
              request, MS(100));

  auto sentRequest = mockedClient->getRequest();
  verifyRequestForNettyRpcEndpointRef(
      sentRequest, srcName, srcHost, srcPort, dstHost, dstPort);
}

TEST(NettyRpcEndpointRefTest, askSyncMapperEnd) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  const auto conf = std::make_shared<conf::CelebornConf>();
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient, *conf);

  PbMapperEndResponse pbMapperEndResponse;
  const int status = 5;
  pbMapperEndResponse.set_status(5);
  protocol::TransportMessage transportMessage(
      MAPPER_END_RESPONSE, pbMapperEndResponse.SerializeAsString());
  auto rpcResponse = makeResponseForNettyRpcEndpointRef(transportMessage, 1000);
  mockedClient->setResponse(*rpcResponse);

  protocol::MapperEnd request{1001};
  auto response =
      nettyRpcEndpointRef
          .askSync<protocol::MapperEnd, protocol::MapperEndResponse>(
              request, MS(100));
  EXPECT_EQ(response->status, status);

  auto sentRequest = mockedClient->getRequest();
  verifyRequestForNettyRpcEndpointRef(
      sentRequest, srcName, srcHost, srcPort, dstHost, dstPort);
}

TEST(NettyRpcEndpointRefTest, askSyncGetReducerFileGroup) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  const auto conf = std::make_shared<conf::CelebornConf>();
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient, *conf);

  PbGetReducerFileGroupResponse pbGetReducerFileGroupResponse;
  const int status = 5;
  pbGetReducerFileGroupResponse.set_status(5);
  protocol::TransportMessage transportMessage(
      GET_REDUCER_FILE_GROUP_RESPONSE,
      pbGetReducerFileGroupResponse.SerializeAsString());
  auto rpcResponse = makeResponseForNettyRpcEndpointRef(transportMessage, 1000);
  mockedClient->setResponse(*rpcResponse);

  protocol::GetReducerFileGroup request{1001};
  auto response = nettyRpcEndpointRef.askSync<
      protocol::GetReducerFileGroup,
      protocol::GetReducerFileGroupResponse>(request, MS(100));
  EXPECT_EQ(response->status, status);

  auto sentRequest = mockedClient->getRequest();
  verifyRequestForNettyRpcEndpointRef(
      sentRequest, srcName, srcHost, srcPort, dstHost, dstPort);
}

TEST(NettyRpcEndpointRefTest, askSyncTimeout) {
  auto mockedClient = std::make_shared<MockTransportClient>();
  const std::string srcName = "test-name";
  const std::string srcHost = "test-src-host";
  const int srcPort = 100;
  const std::string dstHost = "test-dst-host";
  const int dstPort = 101;
  const auto conf = std::make_shared<conf::CelebornConf>();
  auto nettyRpcEndpointRef = NettyRpcEndpointRef(
      srcName, srcHost, srcPort, dstHost, dstPort, mockedClient, *conf);

  const auto timeoutInterval = MS(200);
  const auto sleepInterval = MS(100);
  protocol::MapperEnd request{1001};
  bool timeoutHappened = false;
  std::thread syncThread([&]() {
    try {
      auto response =
          nettyRpcEndpointRef
              .askSync<protocol::MapperEnd, protocol::MapperEndResponse>(
                  request, timeoutInterval);
    } catch (std::exception e) {
      timeoutHappened = true;
    }
  });
  std::this_thread::sleep_for(sleepInterval);

  auto sentRequest = mockedClient->getRequest();
  verifyRequestForNettyRpcEndpointRef(
      sentRequest, srcName, srcHost, srcPort, dstHost, dstPort);
  EXPECT_FALSE(timeoutHappened);

  syncThread.join();
  EXPECT_TRUE(timeoutHappened);
}
