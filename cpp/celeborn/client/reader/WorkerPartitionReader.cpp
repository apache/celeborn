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

#include "celeborn/client/reader/WorkerPartitionReader.h"

namespace celeborn {
namespace client {
std::shared_ptr<WorkerPartitionReader> WorkerPartitionReader::create(
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const std::string& shuffleKey,
    const protocol::PartitionLocation& location,
    int32_t startMapIndex,
    int32_t endMapIndex,
    network::TransportClientFactory* clientFactory) {
  return std::shared_ptr<WorkerPartitionReader>(new WorkerPartitionReader(
      conf, shuffleKey, location, startMapIndex, endMapIndex, clientFactory));
}

WorkerPartitionReader::WorkerPartitionReader(
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const std::string& shuffleKey,
    const protocol::PartitionLocation& location,
    int32_t startMapIndex,
    int32_t endMapIndex,
    network::TransportClientFactory* clientFactory)
    : shuffleKey_(shuffleKey),
      location_(location),
      startMapIndex_(startMapIndex),
      endMapIndex_(endMapIndex),
      fetchingChunkId_(0),
      toConsumeChunkId_(0),
      maxFetchChunksInFlight_(conf->clientFetchMaxReqsInFlight()),
      fetchTimeout_(conf->clientFetchTimeout()) {
  CELEBORN_CHECK_NOT_NULL(clientFactory);
  client_ = clientFactory->createClient(location_.host, location_.fetchPort);

  protocol::OpenStream openStream(
      shuffleKey, location_.filename(), startMapIndex_, endMapIndex_);

  network::RpcRequest request(
      network::Message::nextRequestId(),
      openStream.toTransportMessage().toReadOnlyByteBuffer());

  // TODO: it might not be safe to call blocking & might failing command
  // in constructor
  auto response = client_->sendRpcRequestSync(request);
  auto body = response.body();
  auto transportMessage = protocol::TransportMessage(std::move(body));
  streamHandler_ =
      protocol::StreamHandler::fromTransportMessage(transportMessage);
}

WorkerPartitionReader::~WorkerPartitionReader() {
  protocol::BufferStreamEnd bufferStreamEnd;
  bufferStreamEnd.streamId = streamHandler_->streamId;
  network::RpcRequest request(
      network::Message::nextRequestId(),
      bufferStreamEnd.toTransportMessage().toReadOnlyByteBuffer());
  client_->sendRpcRequestWithoutResponse(request);
}

bool WorkerPartitionReader::hasNext() {
  return toConsumeChunkId_ < streamHandler_->numChunks;
}

std::unique_ptr<memory::ReadOnlyByteBuffer> WorkerPartitionReader::next() {
  initAndCheck();
  fetchChunks();
  auto result = std::unique_ptr<memory::ReadOnlyByteBuffer>();
  // TODO: the try iter here is not aligned with java version.
  while (!result) {
    initAndCheck();
    // TODO: add metric or time tracing
    chunkQueue_.try_dequeue_for(result, kDefaultConsumeIter);
  }
  toConsumeChunkId_++;
  return std::move(result);
}

void WorkerPartitionReader::fetchChunks() {
  initAndCheck();
  while (fetchingChunkId_ - toConsumeChunkId_ < maxFetchChunksInFlight_ &&
         fetchingChunkId_ < streamHandler_->numChunks) {
    auto chunkId = fetchingChunkId_++;
    auto streamChunkSlice = protocol::StreamChunkSlice{
        streamHandler_->streamId, chunkId, 0, INT_MAX};
    protocol::ChunkFetchRequest chunkFetchRequest;
    chunkFetchRequest.streamChunkSlice = streamChunkSlice;
    network::RpcRequest request(
        network::Message::nextRequestId(),
        chunkFetchRequest.toTransportMessage().toReadOnlyByteBuffer());
    client_->fetchChunkAsync(streamChunkSlice, request, onSuccess_, onFailure_);
  }
}

void WorkerPartitionReader::initAndCheck() {
  if (!onSuccess_) {
    onSuccess_ = [weak_this = weak_from_this()](
                     protocol::StreamChunkSlice streamChunkSlice,
                     std::unique_ptr<memory::ReadOnlyByteBuffer> chunk) {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      shared_this->chunkQueue_.enqueue(std::move(chunk));
      VLOG(1) << "WorkerPartitionReader::onSuccess: "
              << streamChunkSlice.toString();
    };

    onFailure_ = [weak_this = weak_from_this()](
                     protocol::StreamChunkSlice streamChunkSlice,
                     std::unique_ptr<std::exception> exception) {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      LOG(ERROR) << "WorkerPartitionReader::onFailure: "
                 << streamChunkSlice.toString()
                 << " msg: " << exception->what();
      {
        auto exp = shared_this->exception_.wlock();
        *exp = std::move(exception);
      }
    };
  }

  {
    auto exp = exception_.rlock();
    if (*exp) {
      CELEBORN_FAIL((*exp)->what());
    }
  }
}
} // namespace client
} // namespace celeborn
