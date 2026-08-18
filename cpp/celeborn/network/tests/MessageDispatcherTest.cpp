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
#include "celeborn/network/TransportClient.h"

using namespace celeborn;
using namespace celeborn::network;

namespace {
// How write() reports a failure: through the returned future, the way wangle's
// AsyncSocketHandler does, or by throwing, the way a handler that serializes
// the message does -- MessageSerializeHandler encodes it, and Message::encode
// checks its own invariants.
// kThrowNonStd throws something that does not derive from std::exception, which
// a handler is free to do.
enum class WriteFailure { kFailFuture, kThrow, kThrowNonStd };

class MockHandler : public wangle::Handler<
                        std::unique_ptr<folly::IOBuf>,
                        std::unique_ptr<Message>,
                        std::unique_ptr<Message>,
                        std::unique_ptr<folly::IOBuf>> {
 public:
  MockHandler(std::unique_ptr<Message>& writedMsg) : writedMsg_(writedMsg) {}

  // When writeError is set, write() reports the failure through the returned
  // future, the way wangle's AsyncSocketHandler does for a socket that is no
  // longer good or whose write callback fails. The writes before
  // failFromWrite succeed, so that a test can leave an earlier request in
  // flight on the connection that the failing write kills.
  MockHandler(
      std::unique_ptr<Message>& writedMsg,
      std::string writeError,
      int failFromWrite = 1,
      WriteFailure writeFailure = WriteFailure::kFailFuture)
      : writedMsg_(writedMsg),
        writeError_(std::move(writeError)),
        failFromWrite_(failFromWrite),
        writeFailure_(writeFailure) {}

  // Hands the write future back to the test, which completes it whenever it
  // wants -- AsyncSocketHandler reports a failed write from the socket's write
  // callback, so it can arrive long after write() returned, including while the
  // connection is being torn down.
  MockHandler(
      std::unique_ptr<Message>& writedMsg,
      folly::Promise<folly::Unit>& writePromise)
      : writedMsg_(writedMsg), writePromise_(&writePromise) {}

  void read(Context* ctx, std::unique_ptr<folly::IOBuf> msg) override {}

  folly::Future<folly::Unit> write(Context* ctx, std::unique_ptr<Message> msg)
      override {
    writedMsg_ = std::move(msg);
    ++numWrites_;
    if (writePromise_ != nullptr) {
      return writePromise_->getFuture();
    }
    if (writeFailure_ == WriteFailure::kThrow ||
        writeFailure_ == WriteFailure::kThrowNonStd) {
      // Only the one malformed message throws: what a handler rejects is the
      // message, not the connection, so the writes around it go through.
      if (numWrites_ == failFromWrite_) {
        if (writeFailure_ == WriteFailure::kThrowNonStd) {
          throw writeError_;
        }
        throw std::runtime_error(writeError_);
      }
      return {};
    }
    if (!writeError_.empty() && numWrites_ >= failFromWrite_) {
      return folly::makeFuture<folly::Unit>(std::runtime_error(writeError_));
    }
    return {};
  }

 private:
  std::unique_ptr<Message>& writedMsg_;
  const std::string writeError_;
  const int failFromWrite_{1};
  const WriteFailure writeFailure_{WriteFailure::kFailFuture};
  folly::Promise<folly::Unit>* writePromise_{nullptr};
  int numWrites_{0};
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

std::string takeExceptionMessage(
    folly::Future<std::unique_ptr<Message>>&& future) {
  return std::move(future).result().exception().what().toStdString();
}

// Returns true when the future failed with a CelebornException marked
// retriable.
bool failedRetriably(folly::Future<std::unique_ptr<Message>>&& future) {
  bool retriable = false;
  const bool matched = std::move(future).result().exception().with_exception(
      [&](const utils::CelebornException& e) { retriable = e.isRetriable(); });
  return matched && retriable;
}

} // namespace

TEST(MessageDispatcherTest, sendRpcRequestAndReceiveResponse) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

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
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string errorMsg = "test-error-msg";
  auto copiedErrorMsg = errorMsg;
  auto rpcFailure =
      std::make_unique<RpcFailure>(requestId, std::move(copiedErrorMsg));
  dispatcher->read(nullptr, std::move(rpcFailure));

  ASSERT_TRUE(future.hasException());
  EXPECT_NE(
      takeExceptionMessage(std::move(future)).find(errorMsg),
      std::string::npos);
}

TEST(MessageDispatcherTest, sendPushDataAndReceiveSuccess) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

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
  auto future = dispatcher->sendPushDataRequest(std::move(pushData));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sentMsg->type(), Message::PUSH_DATA);
  auto sentPushData = dynamic_cast<PushData*>(sentMsg.get());
  EXPECT_EQ(sentPushData->requestId(), requestId);
  EXPECT_EQ(sentPushData->mode(), mode);
  EXPECT_EQ(sentPushData->shuffleKey(), shuffleKey);
  EXPECT_EQ(sentPushData->partitionUniqueId(), partitionUniqueId);
  EXPECT_EQ(sentPushData->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentPushData->body()->readToString(requestBody.size()), requestBody);

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

TEST(MessageDispatcherTest, sendPushDataAndReceiveFailure) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

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
  auto future = dispatcher->sendPushDataRequest(std::move(pushData));

  EXPECT_FALSE(future.isReady());
  EXPECT_EQ(sentMsg->type(), Message::PUSH_DATA);
  auto sentPushData = dynamic_cast<PushData*>(sentMsg.get());
  EXPECT_EQ(sentPushData->requestId(), requestId);
  EXPECT_EQ(sentPushData->mode(), mode);
  EXPECT_EQ(sentPushData->shuffleKey(), shuffleKey);
  EXPECT_EQ(sentPushData->partitionUniqueId(), partitionUniqueId);
  EXPECT_EQ(sentPushData->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentPushData->body()->readToString(requestBody.size()), requestBody);

  // A push failure carries the worker's StatusCode name. It must survive on the
  // exception, otherwise ShuffleClientImpl::getPushDataFailCause cannot tell
  // this apart from a generic failure and worker exclusion never engages.
  const std::string errorMsg = "PUSH_DATA_FAIL_PARTITION_NOT_FOUND";
  auto copiedErrorMsg = errorMsg;
  auto rpcFailure =
      std::make_unique<RpcFailure>(requestId, std::move(copiedErrorMsg));
  dispatcher->read(nullptr, std::move(rpcFailure));

  ASSERT_TRUE(future.hasException());
  EXPECT_NE(
      takeExceptionMessage(std::move(future)).find(errorMsg),
      std::string::npos);
}

TEST(MessageDispatcherTest, sendFetchChunkRequestAndReceiveSuccess) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
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
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

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
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
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
  EXPECT_EQ(sentMsg->type(), Message::RPC_REQUEST);
  auto sentRpcRequest = dynamic_cast<RpcRequest*>(sentMsg.get());
  EXPECT_EQ(sentRpcRequest->body()->remainingSize(), requestBody.size());
  EXPECT_EQ(
      sentRpcRequest->body()->readToString(requestBody.size()), requestBody);

  const std::string errorMsg = "test-error-msg";
  auto copiedErrorMsg = errorMsg;
  auto chunkFetchFailure = std::make_unique<ChunkFetchFailure>(
      streamChunkSlice, std::move(copiedErrorMsg));
  dispatcher->read(nullptr, std::move(chunkFetchFailure));

  ASSERT_TRUE(future.hasException());
  // The fetch-failure message must keep both the worker's error text and the
  // streamChunkSlice it belongs to, so the reader can attribute the failure.
  const auto exceptionMsg = takeExceptionMessage(std::move(future));
  EXPECT_NE(exceptionMsg.find(errorMsg), std::string::npos);
  EXPECT_NE(exceptionMsg.find(streamChunkSlice.toString()), std::string::npos);
}

// A send issued after the connection is closed must fail gracefully with a
// ready, retriable exception instead of tripping an assertion. This mirrors the
// Java client, where a send on an inactive channel surfaces as a retriable
// IOException that CelebornInputStream retries.
TEST(MessageDispatcherTest, sendRpcRequestAfterCloseFailsRetriably) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  dispatcher->close();
  EXPECT_FALSE(dispatcher->isAvailable());

  const long requestId = 2001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));
}

TEST(MessageDispatcherTest, sendFetchChunkRequestAfterCloseFailsRetriably) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  dispatcher->close();
  EXPECT_FALSE(dispatcher->isAvailable());

  const protocol::StreamChunkSlice streamChunkSlice{2001, 2002, 2003, 2004};
  const long requestId = 2001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendFetchChunkRequest(
      streamChunkSlice, std::move(rpcRequest));

  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));
}

// close() must fail any in-flight request rather than leaving its future
// pending forever, matching Java's failOutstandingRequests on channelInactive.
TEST(MessageDispatcherTest, closeFailsInFlightRequestsRetriably) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 3001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));
  EXPECT_FALSE(future.isReady());

  dispatcher->close();

  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));
}

// A failed write must fail the registered request instead of leaving its future
// pending until the request timeout, and it must retire the connection.
// wangle's AsyncSocketHandler reports such a failure through the write future
// -- immediately when the socket is no longer good, or later from its write
// callback -- without going through transportInactive first, so the dispatcher
// is not closed at that point: TransportClient::active() would keep reporting
// true and TransportClientFactory would hand the same dead connection to every
// retry. Java's StdChannelListener closes the channel before reporting the
// failure, and closing it fails whatever else was outstanding.
TEST(MessageDispatcherTest, sendRpcRequestFailedWriteRetiresConnection) {
  std::unique_ptr<Message> sentMsg;
  // The first write succeeds and leaves its request in flight; the second one
  // fails.
  MockHandler mockHandler(
      sentMsg, "socket is closed in write()", /*failFromWrite=*/2);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const std::string requestBody = "test-request-body";
  auto inFlight = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/4001, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(inFlight.isReady());

  auto future = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/4002, toReadOnlyByteBuffer(requestBody)));

  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));
  // The connection is retired, so the client pool stops handing it out.
  EXPECT_FALSE(dispatcher->isAvailable());
  // And the request that was still outstanding on it is failed too, rather than
  // waiting for its timeout on a dead connection.
  ASSERT_TRUE(inFlight.isReady());
  ASSERT_TRUE(inFlight.hasException());
  EXPECT_TRUE(failedRetriably(std::move(inFlight)));
  // A caller still holding the retired connection fails fast on it instead of
  // writing to a dead socket.
  auto rejected = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/4003, toReadOnlyByteBuffer(requestBody)));
  ASSERT_TRUE(rejected.isReady());
  EXPECT_TRUE(failedRetriably(std::move(rejected)));
}

// The hop from a retired connection to the client pool: TransportClient::active
// reports the dispatcher's availability, and TransportClientFactory only reuses
// a cached client while that is true, so a failed write must make the client
// report itself inactive.
TEST(MessageDispatcherTest, failedWriteMakesTransportClientInactive) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg, "socket is closed in write()");
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());
  auto* rawDispatcher = dispatcher.get();
  TransportClient client(
      /*client=*/nullptr, std::move(dispatcher), Timeout(10000));
  EXPECT_TRUE(client.active());

  auto future = rawDispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/4101, toReadOnlyByteBuffer("test-request-body")));

  ASSERT_TRUE(future.isReady());
  EXPECT_TRUE(failedRetriably(std::move(future)));
  EXPECT_FALSE(client.active());
}

TEST(MessageDispatcherTest, sendFetchChunkRequestFailedWriteRetiresConnection) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(
      sentMsg, "socket is closed in write()", /*failFromWrite=*/2);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const std::string requestBody = "test-request-body";
  const protocol::StreamChunkSlice inFlightSlice{4001, 4002, 4003, 4004};
  auto inFlight = dispatcher->sendFetchChunkRequest(
      inFlightSlice,
      std::make_unique<RpcRequest>(
          /*requestId=*/4001, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(inFlight.isReady());

  const protocol::StreamChunkSlice streamChunkSlice{4002, 4002, 4003, 4004};
  auto future = dispatcher->sendFetchChunkRequest(
      streamChunkSlice,
      std::make_unique<RpcRequest>(
          /*requestId=*/4002, toReadOnlyByteBuffer(requestBody)));

  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));
  EXPECT_FALSE(dispatcher->isAvailable());
  ASSERT_TRUE(inFlight.isReady());
  ASSERT_TRUE(inFlight.hasException());
  EXPECT_TRUE(failedRetriably(std::move(inFlight)));
  const protocol::StreamChunkSlice rejectedSlice{4003, 4002, 4003, 4004};
  auto rejected = dispatcher->sendFetchChunkRequest(
      rejectedSlice,
      std::make_unique<RpcRequest>(
          /*requestId=*/4003, toReadOnlyByteBuffer(requestBody)));
  ASSERT_TRUE(rejected.isReady());
  EXPECT_TRUE(failedRetriably(std::move(rejected)));
}

// The write failure may be reported after the dispatcher is destroyed: it comes
// from the socket's write callback, and AsyncSocket fails whatever is still
// pending when it is torn down. TransportClient destroys its dispatcher before
// the bootstrap that owns the pipeline -- and it has to, because
// ~ClientDispatcherBase unregisters itself from that pipeline -- so the write
// continuation must not depend on the dispatcher being alive.
TEST(MessageDispatcherTest, failedWriteAfterDispatcherDestroyedIsIgnored) {
  std::unique_ptr<Message> sentMsg;
  folly::Promise<folly::Unit> writePromise;
  MockHandler mockHandler(sentMsg, writePromise);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  auto future = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/5001, toReadOnlyByteBuffer("test-request-body")));
  EXPECT_FALSE(future.isReady());

  dispatcher.reset();
  // The destroyed dispatcher failed the request it still had outstanding, with
  // the same retriable error it reports on close() rather than with folly's
  // BrokenPromise, which carries no cause for the caller to classify.
  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));

  // The write fails only now, with no dispatcher left to report it to. The
  // continuation must be a no-op instead of reaching into freed memory.
  writePromise.setException(
      std::runtime_error("socket is closed during teardown"));
}

TEST(MessageDispatcherTest, failedFetchWriteAfterDispatcherDestroyedIsIgnored) {
  std::unique_ptr<Message> sentMsg;
  folly::Promise<folly::Unit> writePromise;
  MockHandler mockHandler(sentMsg, writePromise);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const protocol::StreamChunkSlice streamChunkSlice{5001, 5002, 5003, 5004};
  auto future = dispatcher->sendFetchChunkRequest(
      streamChunkSlice,
      std::make_unique<RpcRequest>(
          /*requestId=*/5001, toReadOnlyByteBuffer("test-request-body")));
  EXPECT_FALSE(future.isReady());

  dispatcher.reset();
  ASSERT_TRUE(future.isReady());
  ASSERT_TRUE(future.hasException());
  EXPECT_TRUE(failedRetriably(std::move(future)));

  writePromise.setException(
      std::runtime_error("socket is closed during teardown"));
}

// A handler may also fail by throwing rather than by failing the write future:
// the message is serialized on the way down the pipeline, by
// MessageSerializeHandler, and wangle::Pipeline::write has no try/catch of its
// own. The connection itself is fine in that case, so the exception keeps
// propagating to the caller -- a violation of our own encoding invariants is
// not retriable -- but the request must not be left registered on the
// connection, since nothing was sent and no response will ever arrive for it.
TEST(MessageDispatcherTest, throwingWriteUnregistersTheRequest) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(
      sentMsg,
      "encoded length mismatch",
      /*failFromWrite=*/2,
      WriteFailure::kThrow);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const std::string requestBody = "test-request-body";
  auto inFlight = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/7001, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(inFlight.isReady());

  const long requestId = 7002;
  EXPECT_THROW(
      dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
          requestId, toReadOnlyByteBuffer(requestBody))),
      std::runtime_error);

  // The connection is untouched: it is still usable, and what was outstanding
  // on it is still outstanding.
  EXPECT_TRUE(dispatcher->isAvailable());
  EXPECT_FALSE(inFlight.isReady());
  // And the request whose write threw is gone from the registry. Registering
  // the same id again would otherwise find the leftover entry, whose future has
  // already been handed out.
  auto retried = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(retried.isReady());
}

TEST(MessageDispatcherTest, throwingFetchWriteUnregistersTheRequest) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(
      sentMsg,
      "encoded length mismatch",
      /*failFromWrite=*/1,
      WriteFailure::kThrow);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const std::string requestBody = "test-request-body";
  const protocol::StreamChunkSlice streamChunkSlice{7003, 7004, 7005, 7006};
  EXPECT_THROW(
      dispatcher->sendFetchChunkRequest(
          streamChunkSlice,
          std::make_unique<RpcRequest>(
              /*requestId=*/7003, toReadOnlyByteBuffer(requestBody))),
      std::runtime_error);

  EXPECT_TRUE(dispatcher->isAvailable());
  auto retried = dispatcher->sendFetchChunkRequest(
      streamChunkSlice,
      std::make_unique<RpcRequest>(
          /*requestId=*/7003, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(retried.isReady());
}

// A send that expects no response has no promise to fail, but a failed write
// still means the connection is dead: it must be retired, or the client pool
// keeps handing it to the next caller. ~WorkerPartitionReader takes this path
// to send BufferStreamEnd.
TEST(MessageDispatcherTest, failedWriteWithoutResponseRetiresConnection) {
  std::unique_ptr<Message> sentMsg;
  // The first write succeeds and leaves its request in flight; the second one
  // fails.
  MockHandler mockHandler(
      sentMsg, "socket is closed in write()", /*failFromWrite=*/2);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const std::string requestBody = "test-request-body";
  auto inFlight = dispatcher->sendRpcRequest(std::make_unique<RpcRequest>(
      /*requestId=*/6001, toReadOnlyByteBuffer(requestBody)));
  EXPECT_FALSE(inFlight.isReady());

  dispatcher->sendRpcRequestWithoutResponse(std::make_unique<RpcRequest>(
      /*requestId=*/6002, toReadOnlyByteBuffer(requestBody)));

  EXPECT_FALSE(dispatcher->isAvailable());
  ASSERT_TRUE(inFlight.isReady());
  ASSERT_TRUE(inFlight.hasException());
  EXPECT_TRUE(failedRetriably(std::move(inFlight)));
}

// A handler that rejects the message by throwing must not propagate out of a
// send that expects no response either: ~WorkerPartitionReader sends
// BufferStreamEnd this way, and an exception escaping a destructor aborts the
// process.
TEST(MessageDispatcherTest, throwingWriteWithoutResponseIsReported) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(
      sentMsg,
      "encoded length mismatch",
      /*failFromWrite=*/1,
      WriteFailure::kThrow);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  EXPECT_NO_THROW(
      dispatcher->sendRpcRequestWithoutResponse(std::make_unique<RpcRequest>(
          /*requestId=*/7007, toReadOnlyByteBuffer("test-request-body"))));

  // The message was rejected, not the connection.
  EXPECT_TRUE(dispatcher->isAvailable());
}

// Including when what it throws does not derive from std::exception.
TEST(MessageDispatcherTest, nonStdThrowingWriteWithoutResponseIsReported) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(
      sentMsg,
      "encoded length mismatch",
      /*failFromWrite=*/1,
      WriteFailure::kThrowNonStd);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  EXPECT_NO_THROW(
      dispatcher->sendRpcRequestWithoutResponse(std::make_unique<RpcRequest>(
          /*requestId=*/7008, toReadOnlyByteBuffer("test-request-body"))));

  EXPECT_TRUE(dispatcher->isAvailable());
}

// And once the connection is retired, such a send is skipped rather than
// written to a socket that is known to be dead.
TEST(MessageDispatcherTest, sendWithoutResponseAfterCloseIsSkipped) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  dispatcher->close();
  ASSERT_FALSE(dispatcher->isAvailable());

  dispatcher->sendRpcRequestWithoutResponse(std::make_unique<RpcRequest>(
      /*requestId=*/6003, toReadOnlyByteBuffer("test-request-body")));

  EXPECT_EQ(sentMsg, nullptr);
}

TEST(MessageDispatcherTest, heartbeatIsSilentlyConsumed) {
  std::unique_ptr<Message> sentMsg;
  MockHandler mockHandler(sentMsg);
  auto mockPipeline = createMockedPipeline(std::move(mockHandler));
  auto dispatcher = std::make_unique<MessageDispatcher>();
  dispatcher->setPipeline(mockPipeline.get());

  const long requestId = 1001;
  const std::string requestBody = "test-request-body";
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, toReadOnlyByteBuffer(requestBody));
  auto future = dispatcher->sendRpcRequest(std::move(rpcRequest));

  EXPECT_FALSE(future.isReady());

  // A heartbeat arriving mid-request must be a no-op: it should not fulfill
  // the pending future, not throw, and not close the dispatcher.
  auto heartbeat = std::make_unique<Heartbeat>();
  dispatcher->read(nullptr, std::move(heartbeat));

  EXPECT_FALSE(future.isReady());
  EXPECT_TRUE(dispatcher->isAvailable());

  const std::string responseBody = "test-response-body";
  auto rpcResponse = std::make_unique<RpcResponse>(
      requestId, toReadOnlyByteBuffer(responseBody));
  dispatcher->read(nullptr, std::move(rpcResponse));

  EXPECT_TRUE(future.isReady());
}
