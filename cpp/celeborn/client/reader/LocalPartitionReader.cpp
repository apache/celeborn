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

#include "celeborn/client/reader/LocalPartitionReader.h"

#include <limits>

#include "celeborn/network/TransportClient.h"

namespace celeborn {
namespace client {

std::shared_ptr<LocalPartitionReader> LocalPartitionReader::create(
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const std::string& shuffleKey,
    const protocol::PartitionLocation& location,
    int32_t startMapIndex,
    int32_t endMapIndex,
    std::shared_ptr<network::TransportClient> client) {
  CELEBORN_CHECK_NOT_NULL(client.get());
  auto reader = std::shared_ptr<LocalPartitionReader>(
      new LocalPartitionReader(location, std::move(client)));
  protocol::OpenStream openStream(
      shuffleKey, location.filename(), startMapIndex, endMapIndex, true);
  network::RpcRequest request(
      network::Message::nextRequestId(),
      openStream.toTransportMessage().toReadOnlyByteBuffer());
  auto response =
      reader->client_->sendRpcRequestSync(request, conf->clientFetchTimeout());
  reader->streamHandler_ = protocol::StreamHandler::fromTransportMessage(
      protocol::TransportMessage(response.body()));
  // Once the handler is set, the destructor also closes the worker stream if
  // opening the local file fails.
  reader->openFile();
  return reader;
}

LocalPartitionReader::LocalPartitionReader(
    const protocol::PartitionLocation& location,
    std::shared_ptr<network::TransportClient> client)
    : location_(location), client_(std::move(client)) {}

LocalPartitionReader::~LocalPartitionReader() {
  if (file_.is_open()) {
    file_.close();
  }
  if (streamHandler_) {
    try {
      protocol::BufferStreamEnd streamEnd;
      streamEnd.streamId = streamHandler_->streamId;
      network::RpcRequest request(
          network::Message::nextRequestId(),
          streamEnd.toTransportMessage().toReadOnlyByteBuffer());
      client_->sendRpcRequestWithoutResponse(request);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to close local shuffle stream "
                   << streamHandler_->streamId << ": " << e.what();
    }
  }
}

void LocalPartitionReader::openFile() {
  const auto& handler = *streamHandler_;
  if (handler.numChunks == 0) {
    return;
  }

  const auto& offsets = handler.chunkOffsets;
  CELEBORN_CHECK(
      handler.numChunks > 0 &&
          offsets.size() == static_cast<size_t>(handler.numChunks) + 1,
      "Invalid chunk offsets for local shuffle file {}",
      handler.fullPath);
  file_.open(handler.fullPath, std::ios::binary | std::ios::ate);
  CELEBORN_CHECK(
      file_.is_open(),
      "Failed to open local shuffle file {}",
      handler.fullPath);
  const auto fileSize = static_cast<std::streamoff>(file_.tellg());
  CELEBORN_CHECK(
      fileSize >= 0 && offsets.front() >= 0 && offsets.back() <= fileSize,
      "Chunk offsets are outside local shuffle file {}",
      handler.fullPath);
  for (size_t i = 1; i < offsets.size(); ++i) {
    CELEBORN_CHECK(
        offsets[i] > offsets[i - 1] &&
            offsets[i] - offsets[i - 1] <= std::numeric_limits<int32_t>::max(),
        "Invalid size for chunk {} in local shuffle file {}",
        i - 1,
        handler.fullPath);
  }
  file_.seekg(offsets.front(), std::ios::beg);
  CELEBORN_CHECK(
      file_.good(),
      "Failed to seek in local shuffle file {}",
      handler.fullPath);
  VLOG(1) << "Read local shuffle file " << handler.fullPath;
}

bool LocalPartitionReader::hasNext() {
  return nextChunk_ < static_cast<size_t>(streamHandler_->numChunks);
}

std::unique_ptr<memory::ReadOnlyByteBuffer> LocalPartitionReader::next() {
  CELEBORN_CHECK(hasNext(), "No more chunks in local shuffle stream");
  const auto& offsets = streamHandler_->chunkOffsets;
  const auto size =
      static_cast<size_t>(offsets[nextChunk_ + 1] - offsets[nextChunk_]);
  auto data = folly::IOBuf::createCombined(size);
  file_.read(
      reinterpret_cast<char*>(data->writableData()),
      static_cast<std::streamsize>(size));
  CELEBORN_CHECK(
      file_.good(),
      "Failed to read chunk {} from local shuffle file {}",
      nextChunk_,
      streamHandler_->fullPath);
  data->append(size);
  ++nextChunk_;
  return memory::ByteBuffer::createReadOnly(std::move(data), false);
}

const protocol::PartitionLocation& LocalPartitionReader::getLocation() const {
  return location_;
}

} // namespace client
} // namespace celeborn
