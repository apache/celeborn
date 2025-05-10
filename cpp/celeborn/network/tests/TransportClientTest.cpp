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

#include "celeborn/network/TransportClient.h"

using namespace celeborn;
using namespace celeborn::network;

namespace {
using MS = std::chrono::milliseconds;
class MockDispatcher : public MessageDispatcher {
 public:
  folly::Future<std::unique_ptr<Message>> sendRpcRequest(
      std::unique_ptr<Message> toSendMsg) override {
    sentMsg_ = std::move(toSendMsg);
    msgPromise_ = MsgPromise();
    return msgPromise_.getFuture();
  }

  void sendRpcRequestWithoutResponse(
      std::unique_ptr<Message> toSendMsg) override {
    sentMsg_ = std::move(toSendMsg);
  }

  folly::Future<std::unique_ptr<Message>> sendFetchChunkRequest(
      const protocol::StreamChunkSlice& streamChunkSlice,
      std::unique_ptr<Message> toSendMsg) override {
    sentMsg_ = std::move(toSendMsg);
    msgPromise_ = MsgPromise();
    return msgPromise_.getFuture();
  }

  std::unique_ptr<Message> getSentMsg() {
    return std::move(sentMsg_);
  }

  void receiveMsg(std::unique_ptr<Message> msg) {
    msgPromise_.setValue(std::move(msg));
  }

 private:
  using MsgPromise = folly::Promise<std::unique_ptr<Message>>;
  std::unique_ptr<Message> sentMsg_;
  MsgPromise msgPromise_;
};

std::unique_ptr<memory::ReadOnlyByteBuffer> toReadOnlyByteBuffer(
    const std::string& content) {
  auto buffer = memory::ByteBuffer::createWriteOnly(content.size());
  buffer->writeFromString(content);
  return memory::ByteBuffer::toReadOnly(std::move(buffer));
}
} // namespace

TEST(TransportClientTest, sendRpcRequestSync) {
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  const auto timeoutInterval = MS(10000);
  const auto sleepInterval = MS(100);
  TransportClient client(nullptr, std::move(mockDispatcher), timeoutInterval);

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  std::unique_ptr<RpcResponse> receivedRpcResponse;
  std::thread syncThread([&]() {
    auto responseMsg = client.sendRpcRequestSync(*rpcRequest, timeoutInterval);
    receivedRpcResponse = std::make_unique<RpcResponse>(responseMsg);
  });
  std::this_thread::sleep_for(sleepInterval);

  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

  // The response is not received yet.
  EXPECT_FALSE(receivedRpcResponse);
  const std::string responseBody = "test-response-body";
  auto rpcResponse = std::make_unique<RpcResponse>(
      requestId, toReadOnlyByteBuffer(responseBody));
  rawMockDispatcher->receiveMsg(std::move(rpcResponse));

  syncThread.join();
  // The response is received.
  EXPECT_TRUE(receivedRpcResponse);
  EXPECT_EQ(receivedRpcResponse->body()->remainingSize(), responseBody.size());
  EXPECT_EQ(
      receivedRpcResponse->body()->readToString(responseBody.size()),
      responseBody);
}

TEST(TransportClientTest, sendRpcRequestSyncTimeout) {
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  const auto timeoutInterval = MS(200);
  const auto sleepInterval = MS(100);
  TransportClient client(nullptr, std::move(mockDispatcher), timeoutInterval);

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  std::unique_ptr<RpcResponse> receivedRpcResponse;
  bool timeoutHappened = false;
  std::thread syncThread([&]() {
    try {
      auto responseMsg =
          client.sendRpcRequestSync(*rpcRequest, timeoutInterval);
      receivedRpcResponse = std::make_unique<RpcResponse>(responseMsg);
    } catch (std::exception e) {
      timeoutHappened = true;
    }
  });
  std::this_thread::sleep_for(sleepInterval);

  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

  // The response is not received yet.
  EXPECT_FALSE(receivedRpcResponse);

  syncThread.join();
  // Not response received, should be timeout.
  EXPECT_FALSE(receivedRpcResponse);
  EXPECT_TRUE(timeoutHappened);
}

TEST(TransportClientTest, sendRpcRequestWithoutResponse) {
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  const auto timeoutInterval = MS(10000);
  TransportClient client(nullptr, std::move(mockDispatcher), timeoutInterval);

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  client.sendRpcRequestWithoutResponse(*rpcRequest);

  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);
}

TEST(TransportClientTest, fetchChunkAsyncSuccess) {
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  TransportClient client(nullptr, std::move(mockDispatcher), MS(10000));

  const protocol::StreamChunkSlice streamChunkSlice{1, 2, 3, 4};
  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  protocol::StreamChunkSlice onSuccessStreamChunkSlice;
  std::unique_ptr<memory::ReadOnlyByteBuffer> onSuccessBuffer;
  FetchChunkSuccessCallback onSuccess =
      [&](protocol::StreamChunkSlice slice,
          std::unique_ptr<memory::ReadOnlyByteBuffer> buffer) {
        onSuccessStreamChunkSlice = slice;
        onSuccessBuffer = std::move(buffer);
      };
  protocol::StreamChunkSlice onFailureStreamChunkSlice;
  std::unique_ptr<std::exception> onFailureException;
  FetchChunkFailureCallback onFailure =
      [&](protocol::StreamChunkSlice slice,
          std::unique_ptr<std::exception> exception) {
        onFailureStreamChunkSlice = slice;
        onFailureException = std::move(exception);
      };

  client.fetchChunkAsync(streamChunkSlice, *rpcRequest, onSuccess, onFailure);
  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);
  // The response is not received yet.
  EXPECT_FALSE(onSuccessBuffer);

  const std::string responseBody = "test-response-body";
  auto chunkFetchSuccess = std::make_unique<ChunkFetchSuccess>(
      streamChunkSlice, toReadOnlyByteBuffer(responseBody));
  rawMockDispatcher->receiveMsg(std::move(chunkFetchSuccess));

  // The response is received.
  EXPECT_TRUE(onSuccessBuffer);
  EXPECT_FALSE(onFailureException);
  EXPECT_EQ(onSuccessBuffer->remainingSize(), responseBody.size());
  EXPECT_EQ(onSuccessBuffer->readToString(responseBody.size()), responseBody);
}

TEST(TransportClientTest, fetchChunkAsyncFailure) {
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  TransportClient client(nullptr, std::move(mockDispatcher), MS(10000));

  const protocol::StreamChunkSlice streamChunkSlice{1, 2, 3, 4};
  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  protocol::StreamChunkSlice onSuccessStreamChunkSlice;
  std::unique_ptr<memory::ReadOnlyByteBuffer> onSuccessBuffer;
  FetchChunkSuccessCallback onSuccess =
      [&](protocol::StreamChunkSlice slice,
          std::unique_ptr<memory::ReadOnlyByteBuffer> buffer) {
        onSuccessStreamChunkSlice = slice;
        onSuccessBuffer = std::move(buffer);
      };
  protocol::StreamChunkSlice onFailureStreamChunkSlice;
  std::unique_ptr<std::exception> onFailureException;
  FetchChunkFailureCallback onFailure =
      [&](protocol::StreamChunkSlice slice,
          std::unique_ptr<std::exception> exception) {
        onFailureStreamChunkSlice = slice;
        onFailureException = std::move(exception);
      };

  client.fetchChunkAsync(streamChunkSlice, *rpcRequest, onSuccess, onFailure);
  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);
  // The response is not received yet.
  EXPECT_FALSE(onSuccessBuffer);

  auto chunkFetchFailure = std::make_unique<ChunkFetchFailure>(
      streamChunkSlice, "test-error-string");
  rawMockDispatcher->receiveMsg(std::move(chunkFetchFailure));

  // The failure is received.
  EXPECT_TRUE(onFailureException);
  EXPECT_FALSE(onSuccessBuffer);
}
