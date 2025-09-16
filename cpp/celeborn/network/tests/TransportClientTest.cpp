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

#include <folly/init/Init.h>
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

  folly::Future<std::unique_ptr<Message>> sendPushDataRequest(
      std::unique_ptr<Message> toSendMsg) override {
    sentMsg_ = std::move(toSendMsg);
    msgPromise_ = MsgPromise();
    return msgPromise_.getFuture();
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

class MockRpcResponseCallback : public RpcResponseCallback {
 public:
  MockRpcResponseCallback() = default;

  ~MockRpcResponseCallback() override = default;

  void onSuccess(std::unique_ptr<memory::ReadOnlyByteBuffer> data) override {
    onSuccessBuffer_ = std::move(data);
  }

  void onFailure(std::unique_ptr<std::exception> exception) override {
    onFailureException_ = std::move(exception);
  }

  std::unique_ptr<memory::ReadOnlyByteBuffer> getOnSuccessBuffer() {
    return std::move(onSuccessBuffer_);
  }

  std::unique_ptr<std::exception> getOnFailureException() {
    return std::move(onFailureException_);
  }

 private:
  std::unique_ptr<memory::ReadOnlyByteBuffer> onSuccessBuffer_;
  std::unique_ptr<std::exception> onFailureException_;
};
} // namespace

class TransportClientTest : public testing::Test {
 protected:
  TransportClientTest() {
    if (!follyInit_) {
      std::lock_guard<std::mutex> lock(initMutex_);
      if (!follyInit_) {
        int argc = 0;
        char* arg = "test-arg";
        char** argv = &arg;
        follyInit_ = std::make_unique<folly::Init>(&argc, &argv, false);
      }
    }
  }

  ~TransportClientTest() override = default;

 private:
  // Must be only inited once per process.
  static std::unique_ptr<folly::Init> follyInit_;
  static std::mutex initMutex_;
};

std::unique_ptr<folly::Init> TransportClientTest::follyInit_ = {};
std::mutex TransportClientTest::initMutex_ = {};

TEST_F(TransportClientTest, sendRpcRequestSync) {
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

TEST_F(TransportClientTest, sendRpcRequestSyncTimeout) {
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

TEST_F(TransportClientTest, sendRpcRequestWithoutResponse) {
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

TEST_F(TransportClientTest, pushDataAsyncSuccess) {
  // Construct mock utils.
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  TransportClient client(nullptr, std::move(mockDispatcher), MS(10000));
  auto mockRpcResponseCallback = std::make_shared<MockRpcResponseCallback>();

  // Construct toSend PushData.
  const long requestId = 1001;
  const uint8_t mode = 2;
  const std::string shuffleKey = "test-shuffle-key";
  const std::string partitionUniqueId = "test-partition-id";
  const std::string requestBody = "test-request-body";
  auto pushData = std::make_unique<PushData>(
      requestId,
      mode,
      shuffleKey,
      partitionUniqueId,
      toReadOnlyByteBuffer(requestBody));

  // Send pushData via client, check that the sentPushData is identical to
  // original pushData.
  client.pushDataAsync(*pushData, MS(10000), mockRpcResponseCallback);
  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::PUSH_DATA);
  auto sentPushData = dynamic_cast<PushData*>(sentMsg.get());
  EXPECT_EQ(sentPushData->requestId(), requestId);
  EXPECT_EQ(sentPushData->mode(), mode);
  EXPECT_EQ(sentPushData->shuffleKey(), shuffleKey);
  EXPECT_EQ(sentPushData->partitionUniqueId(), partitionUniqueId);
  EXPECT_EQ(sentPushData->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentPushData->body()->readToString(requestBody.size()), requestBody);
  EXPECT_FALSE(mockRpcResponseCallback->getOnSuccessBuffer());

  // Construct response and make dispatcher receive it.
  const std::string responseBody = "test-response-body";
  auto rpcResponse = std::make_unique<RpcResponse>(
      requestId, toReadOnlyByteBuffer(responseBody));
  rawMockDispatcher->receiveMsg(std::move(rpcResponse));

  // Check the received message.
  auto onSuccessBuffer = mockRpcResponseCallback->getOnSuccessBuffer();
  auto onFailureException = mockRpcResponseCallback->getOnFailureException();
  EXPECT_TRUE(onSuccessBuffer);
  EXPECT_FALSE(onFailureException);
  EXPECT_EQ(onSuccessBuffer->remainingSize(), responseBody.size());
  EXPECT_EQ(onSuccessBuffer->readToString(responseBody.size()), responseBody);
}

TEST_F(TransportClientTest, pushDataAsyncFailure) {
  // Construct mock utils.
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  TransportClient client(nullptr, std::move(mockDispatcher), MS(10000));
  auto mockRpcResponseCallback = std::make_shared<MockRpcResponseCallback>();

  // Construct toSend PushData.
  const long requestId = 1001;
  const uint8_t mode = 2;
  const std::string shuffleKey = "test-shuffle-key";
  const std::string partitionUniqueId = "test-partition-id";
  const std::string requestBody = "test-request-body";
  auto pushData = std::make_unique<PushData>(
      requestId,
      mode,
      shuffleKey,
      partitionUniqueId,
      toReadOnlyByteBuffer(requestBody));

  // Send pushData via client, check that the sentPushData is identical to
  // original pushData.
  client.pushDataAsync(*pushData, MS(10000), mockRpcResponseCallback);
  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::PUSH_DATA);
  auto sentPushData = dynamic_cast<PushData*>(sentMsg.get());
  EXPECT_EQ(sentPushData->requestId(), requestId);
  EXPECT_EQ(sentPushData->mode(), mode);
  EXPECT_EQ(sentPushData->shuffleKey(), shuffleKey);
  EXPECT_EQ(sentPushData->partitionUniqueId(), partitionUniqueId);
  EXPECT_EQ(sentPushData->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentPushData->body()->readToString(requestBody.size()), requestBody);
  EXPECT_FALSE(mockRpcResponseCallback->getOnSuccessBuffer());

  // Construct failure and make dispatcher receive it.
  auto rpcFailure = std::make_unique<RpcFailure>(requestId, "failure-msg-body");
  rawMockDispatcher->receiveMsg(std::move(rpcFailure));

  // Check the received message.
  auto onSuccessBuffer = mockRpcResponseCallback->getOnSuccessBuffer();
  auto onFailureException = mockRpcResponseCallback->getOnFailureException();
  EXPECT_FALSE(onSuccessBuffer);
  EXPECT_TRUE(onFailureException);
}

TEST_F(TransportClientTest, pushDataAsyncTimeout) {
  // Construct mock utils.
  auto mockDispatcher = std::make_unique<MockDispatcher>();
  auto rawMockDispatcher = mockDispatcher.get();
  TransportClient client(nullptr, std::move(mockDispatcher), MS(10000));
  auto mockRpcResponseCallback = std::make_shared<MockRpcResponseCallback>();

  // Construct toSend PushData.
  const long requestId = 1001;
  const uint8_t mode = 2;
  const std::string shuffleKey = "test-shuffle-key";
  const std::string partitionUniqueId = "test-partition-id";
  const std::string requestBody = "test-request-body";
  auto pushData = std::make_unique<PushData>(
      requestId,
      mode,
      shuffleKey,
      partitionUniqueId,
      toReadOnlyByteBuffer(requestBody));

  // Send pushData via client, check that the sentPushData is identical to
  // original pushData.
  auto timeoutInterval = MS(100);
  client.pushDataAsync(*pushData, timeoutInterval, mockRpcResponseCallback);
  auto sentMsg = rawMockDispatcher->getSentMsg();
  EXPECT_EQ(sentMsg->type(), Message::PUSH_DATA);
  auto sentPushData = dynamic_cast<PushData*>(sentMsg.get());
  EXPECT_EQ(sentPushData->requestId(), requestId);
  EXPECT_EQ(sentPushData->mode(), mode);
  EXPECT_EQ(sentPushData->shuffleKey(), shuffleKey);
  EXPECT_EQ(sentPushData->partitionUniqueId(), partitionUniqueId);
  EXPECT_EQ(sentPushData->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentPushData->body()->readToString(requestBody.size()), requestBody);
  EXPECT_FALSE(mockRpcResponseCallback->getOnSuccessBuffer());

  // Wait for timeout.
  std::this_thread::sleep_for(timeoutInterval * 3);

  // Check the received message.
  auto onSuccessBuffer = mockRpcResponseCallback->getOnSuccessBuffer();
  auto onFailureException = mockRpcResponseCallback->getOnFailureException();
  EXPECT_FALSE(onSuccessBuffer);
  EXPECT_TRUE(onFailureException);
}

TEST_F(TransportClientTest, fetchChunkAsyncSuccess) {
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

TEST_F(TransportClientTest, fetchChunkAsyncFailure) {
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
