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

#include "celeborn/client/reader/WorkerPartitionReader.h"

using namespace celeborn;
using namespace celeborn::client;
using namespace celeborn::network;
using namespace celeborn::protocol;
using namespace celeborn::memory;
using namespace celeborn::conf;

namespace {
using MS = std::chrono::milliseconds;

class MockTransportClient : public TransportClient {
 public:
  MockTransportClient()
      : TransportClient(nullptr, nullptr, MS(100)),
        syncResponse_(RpcResponse(0, ReadOnlyByteBuffer::createEmptyBuffer())),
        syncRequest_(RpcRequest(0, ReadOnlyByteBuffer::createEmptyBuffer())),
        nonResponseRequest_(
            RpcRequest(0, ReadOnlyByteBuffer::createEmptyBuffer())),
        fetchChunkRequest_(
            RpcRequest(0, ReadOnlyByteBuffer::createEmptyBuffer())) {}

  RpcResponse sendRpcRequestSync(const RpcRequest& request, Timeout timeout)
      override {
    syncRequest_ = request;
    return syncResponse_;
  }

  void sendRpcRequestWithoutResponse(const RpcRequest& request) override {
    nonResponseRequest_ = request;
  }

  void fetchChunkAsync(
      const StreamChunkSlice& streamChunkSlice,
      const RpcRequest& request,
      FetchChunkSuccessCallback onSuccess,
      FetchChunkFailureCallback onFailure) override {
    streamChunkSlice_ = streamChunkSlice;
    fetchChunkRequest_ = request;
    if (fetchChunkOnSuccessResult_) {
      onSuccess(streamChunkSlice, std::move(fetchChunkOnSuccessResult_));
    } else if (fetchChunkOnFailureResult_) {
      onFailure(streamChunkSlice, std::move(fetchChunkOnFailureResult_));
    }
  }

  void setSyncResponse(const RpcResponse& response) {
    syncResponse_ = response;
  }

  void setFetchChunkSuccessResult(std::unique_ptr<ReadOnlyByteBuffer> result) {
    fetchChunkOnSuccessResult_ = std::move(result);
  }

  void setFetchChunkFailureResult(std::unique_ptr<std::exception> result) {
    fetchChunkOnFailureResult_ = std::move(result);
  }

  RpcRequest getSyncRequest() {
    return syncRequest_;
  }

  RpcRequest getNoneResponseRequest() {
    return nonResponseRequest_;
  }

  StreamChunkSlice getStreamChunkSlice() {
    return streamChunkSlice_;
  }

  RpcRequest getFetchChunkRequest() {
    return fetchChunkRequest_;
  }

 private:
  RpcResponse syncResponse_;
  RpcRequest syncRequest_;
  RpcRequest nonResponseRequest_;
  StreamChunkSlice streamChunkSlice_;
  RpcRequest fetchChunkRequest_;
  std::unique_ptr<ReadOnlyByteBuffer> fetchChunkOnSuccessResult_;
  std::unique_ptr<std::exception> fetchChunkOnFailureResult_;
};

class MockTransportClientFactory : public TransportClientFactory {
 public:
  MockTransportClientFactory()
      : TransportClientFactory(std::make_shared<CelebornConf>()),
        transportClient_(std::make_shared<MockTransportClient>()) {}

  std::shared_ptr<TransportClient> createClient(
      const std::string& host,
      uint16_t port) override {
    return transportClient_;
  }

  std::shared_ptr<MockTransportClient> getClient() {
    return transportClient_;
  }

 private:
  std::shared_ptr<MockTransportClient> transportClient_;
};

std::unique_ptr<ReadOnlyByteBuffer> toReadOnlyByteBuffer(
    const std::string& content) {
  auto buffer = ByteBuffer::createWriteOnly(content.size());
  buffer->writeFromString(content);
  return ByteBuffer::toReadOnly(std::move(buffer));
}
} // namespace

TEST(WorkerPartitionReaderTest, fetchChunkSuccess) {
  const std::string shuffleKey = "test-shuffle-key";
  PartitionLocation location;
  location.host = "test-host";
  location.fetchPort = 1011;
  location.id = 1;
  location.epoch = 2;
  location.mode = PartitionLocation::Mode::PRIMARY;
  location.storageInfo = std::make_unique<StorageInfo>();
  const std::string filename = std::to_string(location.id) + "-" +
      std::to_string(location.epoch) + "-" + std::to_string(location.mode);
  const int32_t startMapIndex = 0;
  const int32_t endMapIndex = 100;
  MockTransportClientFactory mockedClientFactory;
  auto conf = std::make_shared<CelebornConf>();
  auto transportClient = mockedClientFactory.getClient();

  // Build a pbStreamHandler with 1 chunk,  pack into a RpcResponse,
  // set as response.
  PbStreamHandler pb;
  const int streamId = 100;
  const int numChunks = 1;
  pb.set_streamid(streamId);
  pb.set_numchunks(numChunks);
  for (int i = 0; i < numChunks; i++) {
    pb.add_chunkoffsets(i);
  }
  pb.set_fullpath("test-fullpath");
  TransportMessage transportMessage(STREAM_HANDLER, pb.SerializeAsString());
  RpcResponse response =
      RpcResponse(1111, transportMessage.toReadOnlyByteBuffer());
  transportClient->setSyncResponse(response);

  // Set the chunk to be returned.
  const std::string chunkBody = "test-chunk-body";
  transportClient->setFetchChunkSuccessResult(toReadOnlyByteBuffer(chunkBody));

  auto partitionReader = WorkerPartitionReader::create(
      conf,
      shuffleKey,
      location,
      startMapIndex,
      endMapIndex,
      &mockedClientFactory);
  EXPECT_TRUE(partitionReader->hasNext());
  auto chunk = partitionReader->next();
  EXPECT_EQ(chunk->remainingSize(), chunkBody.size());
  EXPECT_EQ(chunk->readToString(chunk->remainingSize()), chunkBody);
  EXPECT_FALSE(partitionReader->hasNext());
}

TEST(WorkerPartitionReaderTest, fetchChunkFailure) {
  const std::string shuffleKey = "test-shuffle-key";
  PartitionLocation location;
  location.host = "test-host";
  location.fetchPort = 1011;
  location.id = 1;
  location.epoch = 2;
  location.mode = PartitionLocation::Mode::PRIMARY;
  location.storageInfo = std::make_unique<StorageInfo>();
  const std::string filename = std::to_string(location.id) + "-" +
      std::to_string(location.epoch) + "-" + std::to_string(location.mode);
  const int32_t startMapIndex = 0;
  const int32_t endMapIndex = 100;
  MockTransportClientFactory mockedClientFactory;
  auto conf = std::make_shared<CelebornConf>();
  auto transportClient = mockedClientFactory.getClient();

  // Build a pbStreamHandler with 1 chunk,  pack into a RpcResponse,
  // set as response.
  PbStreamHandler pb;
  const int streamId = 100;
  const int numChunks = 1;
  pb.set_streamid(streamId);
  pb.set_numchunks(numChunks);
  for (int i = 0; i < numChunks; i++) {
    pb.add_chunkoffsets(i);
  }
  pb.set_fullpath("test-fullpath");
  TransportMessage transportMessage(STREAM_HANDLER, pb.SerializeAsString());
  RpcResponse response =
      RpcResponse(1111, transportMessage.toReadOnlyByteBuffer());
  transportClient->setSyncResponse(response);

  // Set the error to be returned.
  transportClient->setFetchChunkFailureResult(
      std::make_unique<std::runtime_error>("test-runtime-error"));

  auto partitionReader = WorkerPartitionReader::create(
      conf,
      shuffleKey,
      location,
      startMapIndex,
      endMapIndex,
      &mockedClientFactory);
  EXPECT_TRUE(partitionReader->hasNext());
  EXPECT_THROW(partitionReader->next(), std::exception);
}
