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

#include "celeborn/network/FrameDecoder.h"
#include "celeborn/network/MessageDispatcher.h"

using namespace celeborn;
using namespace celeborn::network;

namespace {
class MockHandler : public wangle::Handler<
                        std::unique_ptr<folly::IOBuf>,
                        std::unique_ptr<Message>,
                        std::unique_ptr<Message>,
                        std::unique_ptr<folly::IOBuf>> {
 public:
  MockHandler(std::unique_ptr<Message>& writedMsg) : writedMsg_(writedMsg) {}

  void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override {}

  folly::Future<folly::Unit> write(Context* ctx, std::unique_ptr<Message> msg)
      override {
    writedMsg_ = std::move(msg);
    return {};
  }

 private:
  std::unique_ptr<Message>& writedMsg_;
};

SerializePipeline::Ptr createMockedPipeline(MockHandler&& mockHandler) {
  auto pipeline = SerializePipeline::create();
  // FrameDecoder here is just for forming a complete pipeline to pass
  // the type checking, not used here.
  pipeline->addBack(FrameDecoder());
  pipeline->addBack(std::move(mockHandler));
  pipeline->finalize();
  return pipeline;
}

std::unique_ptr<memory::ReadOnlyByteBuffer> toReadOnlyByteBuffer(
    const std::string& content) {
  auto buffer = memory::ByteBuffer::createWriteOnly(content.size());
  buffer->writeFromString(content);
  return memory::ByteBuffer::toReadOnly(std::move(buffer));
}

} // namespace

TEST(MessageDispatcherTest, sendRpcRequestAndReceiveResponse) {
  std::unique_ptr<Message> sendedMsg;
  MockHandler mockHandler(sendedMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sendedMsg->type(), Message::RPC_REQUEST);
  auto sendedRpcRequest = dynamic_cast<RpcRequest*>(sendedMsg.get());
  EXPECT_EQ(sendedRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sendedRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string responseBody = "test-response-body";
  auto rpcResponse = std::make_unique<RpcResponse>(
      requestId, toReadOnlyByteBuffer(responseBody));
  dispatcher->read(nullptr, std::move(rpcResponse));

  EXPECT_TRUE(future.isReady());
  auto receivedMsg = std::move(future).get();
  EXPECT_EQ(receivedMsg->type(), Message::RPC_RESPONSE);
  auto receivedRpcResponse = dynamic_cast<RpcResponse*>(receivedMsg.get());
  EXPECT_EQ(receivedRpcResponse->body()->remainingSize(), responseBody.size());
  EXPECT_EQ(
      receivedRpcResponse->body()->readToString(responseBody.size()),
      responseBody);
}

TEST(MessageDispatcherTest, sendRpcRequestAndReceiveFailure) {
  std::unique_ptr<Message> sendedMsg;
  MockHandler mockHandler(sendedMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sendedMsg->type(), Message::RPC_REQUEST);
  auto sendedRpcRequest = dynamic_cast<RpcRequest*>(sendedMsg.get());
  EXPECT_EQ(sendedRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sendedRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string errorMsg = "test-error-msg";
  auto copiedErrorMsg = errorMsg;
  auto rpcFailure =
      std::make_unique<RpcFailure>(requestId, std::move(copiedErrorMsg));
  dispatcher->read(nullptr, std::move(rpcFailure));

  EXPECT_TRUE(future.hasException());
}

TEST(MessageDispatcherTest, sendFetchChunkRequestAndReceiveSuccess) {
  std::unique_ptr<Message> sendedMsg;
  MockHandler mockHandler(sendedMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const protocol::StreamChunkSlice streamChunkSlice{1001, 1002, 1003, 1004};
  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendFetchChunkRequest(
      streamChunkSlice, std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sendedMsg->type(), Message::RPC_REQUEST);
  auto sendedRpcRequest = dynamic_cast<RpcRequest*>(sendedMsg.get());
  EXPECT_EQ(sendedRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sendedRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string chunkFetchSuccessBody = "test-chunk-fetch-success-body";
  auto chunkFetchSuccess = std::make_unique<ChunkFetchSuccess>(
      streamChunkSlice, toReadOnlyByteBuffer(chunkFetchSuccessBody));
  dispatcher->read(nullptr, std::move(chunkFetchSuccess));

  EXPECT_TRUE(future.isReady());
  auto receivedMsg = std::move(future).get();
  EXPECT_EQ(receivedMsg->type(), Message::CHUNK_FETCH_SUCCESS);
  auto receivedChunkFetchSuccess =
      dynamic_cast<ChunkFetchSuccess*>(receivedMsg.get());
  EXPECT_EQ(
      receivedChunkFetchSuccess->body()->remainingSize(),
      chunkFetchSuccessBody.size());
  EXPECT_EQ(
      receivedChunkFetchSuccess->body()->readToString(
          chunkFetchSuccessBody.size()),
      chunkFetchSuccessBody);
}

TEST(MessageDispatcherTest, sendFetchChunkRequestAndReceiveFailure) {
  std::unique_ptr<Message> sendedMsg;
  MockHandler mockHandler(sendedMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const protocol::StreamChunkSlice streamChunkSlice{1001, 1002, 1003, 1004};
  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendFetchChunkRequest(
      streamChunkSlice, std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sendedMsg->type(), Message::RPC_REQUEST);
  auto sendedRpcRequest = dynamic_cast<RpcRequest*>(sendedMsg.get());
  EXPECT_EQ(sendedRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sendedRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string errorMsg = "test-error-msg";
  auto copiedErrorMsg = errorMsg;
  auto chunkFetchFailure = std::make_unique<ChunkFetchFailure>(
      streamChunkSlice, std::move(copiedErrorMsg));
  dispatcher->read(nullptr, std::move(chunkFetchFailure));

  EXPECT_TRUE(future.hasException());
}
