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

#include "celeborn/network/Message.h"
#include "celeborn/protocol/Encoders.h"

namespace celeborn {
namespace network {
Message::Type Message::decodeType(uint8_t typeId) {
  switch (typeId) {
    case 0:
      return CHUNK_FETCH_REQUEST;
    case 1:
      return CHUNK_FETCH_SUCCESS;
    case 2:
      return CHUNK_FETCH_FAILURE;
    case 3:
      return RPC_REQUEST;
    case 4:
      return RPC_RESPONSE;
    case 5:
      return RPC_FAILURE;
    case 6:
      return OPEN_STREAM;
    case 7:
      return STREAM_HANDLE;
    case 9:
      return ONE_WAY_MESSAGE;
    case 11:
      return PUSH_DATA;
    case 12:
      return PUSH_MERGED_DATA;
    case 13:
      return REGION_START;
    case 14:
      return REGION_FINISH;
    case 15:
      return PUSH_DATA_HAND_SHAKE;
    case 16:
      return READ_ADD_CREDIT;
    case 17:
      return READ_DATA;
    case 18:
      return OPEN_STREAM_WITH_CREDIT;
    case 19:
      return BACKLOG_ANNOUNCEMENT;
    case 20:
      return TRANSPORTABLE_ERROR;
    case 21:
      return BUFFER_STREAM_END;
    case 22:
      return HEARTBEAT;
    default:
      CELEBORN_FAIL("Unknown message type " + std::to_string(typeId));
  }
}

std::atomic<long> Message::currRequestId_ = 0;

std::unique_ptr<memory::ReadOnlyByteBuffer> Message::encode() const {
  int bodyLength = body_->remainingSize();
  int encodedLength = internalEncodedLength();
  int headerLength =
      sizeof(int32_t) + sizeof(uint8_t) + sizeof(int32_t) + encodedLength;
  auto buffer = memory::ByteBuffer::createWriteOnly(headerLength);
  buffer->write<int32_t>(encodedLength);
  buffer->write<uint8_t>(type_);
  buffer->write<int32_t>(bodyLength);
  internalEncodeTo(*buffer);
  auto header = memory::ByteBuffer::toReadOnly(std::move(buffer));
  auto combinedFrame = memory::ByteBuffer::concat(*header, *body_);
  return std::move(combinedFrame);
}

std::unique_ptr<Message> Message::decodeFrom(
    std::unique_ptr<memory::ReadOnlyByteBuffer>&& data) {
  int32_t encodedLength = data->read<int32_t>();
  uint8_t typeId = data->read<uint8_t>();
  int32_t bodyLength = data->read<int32_t>();
  CELEBORN_CHECK_EQ(encodedLength + bodyLength, data->remainingSize());
  Type type = decodeType(typeId);
  switch (type) {
    case RPC_RESPONSE:
      return RpcResponse::decodeFrom(std::move(data));
    case RPC_FAILURE:
      return RpcFailure::decodeFrom(std::move(data));
    case CHUNK_FETCH_SUCCESS:
      return ChunkFetchSuccess::decodeFrom(std::move(data));
    case CHUNK_FETCH_FAILURE:
      return ChunkFetchFailure::decodeFrom(std::move(data));
    default:
      CELEBORN_FAIL("unsupported Message decode type " + std::to_string(type));
  }
}

int RpcRequest::internalEncodedLength() const {
  return sizeof(long) + sizeof(int32_t);
}

void RpcRequest::internalEncodeTo(memory::WriteOnlyByteBuffer& buffer) const {
  buffer.write<long>(requestId_);
  buffer.write<int32_t>(body_->remainingSize());
}

std::unique_ptr<RpcResponse> RpcResponse::decodeFrom(
    std::unique_ptr<memory::ReadOnlyByteBuffer>&& data) {
  long requestId = data->read<long>();
  data->skip(4);
  auto result = std::make_unique<RpcResponse>(requestId, std::move(data));
  return result;
}

std::unique_ptr<RpcFailure> RpcFailure::decodeFrom(
    std::unique_ptr<memory::ReadOnlyByteBuffer>&& data) {
  long requestId = data->read<long>();
  int strLen = data->read<int>();
  CELEBORN_CHECK_EQ(data->remainingSize(), strLen);
  std::string errorString = data->readToString(strLen);
  return std::make_unique<RpcFailure>(requestId, std::move(errorString));
}

std::unique_ptr<ChunkFetchSuccess> ChunkFetchSuccess::decodeFrom(
    std::unique_ptr<memory::ReadOnlyByteBuffer>&& data) {
  protocol::StreamChunkSlice streamChunkSlice =
      protocol::StreamChunkSlice::decodeFrom(*data);
  return std::make_unique<ChunkFetchSuccess>(streamChunkSlice, std::move(data));
}

std::unique_ptr<ChunkFetchFailure> ChunkFetchFailure::decodeFrom(
    std::unique_ptr<memory::ReadOnlyByteBuffer>&& data) {
  protocol::StreamChunkSlice streamChunkSlice =
      protocol::StreamChunkSlice::decodeFrom(*data);
  int strLen = data->read<int>();
  CELEBORN_CHECK_EQ(data->remainingSize(), strLen);
  std::string errorString = data->readToString(strLen);
  return std::make_unique<ChunkFetchFailure>(
      streamChunkSlice, std::move(errorString));
}

PushData::PushData(
    long requestId,
    uint8_t mode,
    const std::string& shuffleKey,
    const std::string& partitionUniqueId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> body)
    : Message(PUSH_DATA, std::move(body)),
      requestId_(requestId),
      mode_(mode),
      shuffleKey_(shuffleKey),
      partitionUniqueId_(partitionUniqueId) {}

PushData::PushData(const PushData& other)
    : Message(PUSH_DATA, other.body()),
      requestId_(other.requestId_),
      mode_(other.mode_),
      shuffleKey_(other.shuffleKey_),
      partitionUniqueId_(other.partitionUniqueId_) {}

int PushData::internalEncodedLength() const {
  return sizeof(long) + sizeof(uint8_t) + protocol::encodedLength(shuffleKey_) +
      protocol::encodedLength(partitionUniqueId_);
}

void PushData::internalEncodeTo(memory::WriteOnlyByteBuffer& buffer) const {
  buffer.write<long>(requestId_);
  buffer.write<uint8_t>(mode_);
  protocol::encode(buffer, shuffleKey_);
  protocol::encode(buffer, partitionUniqueId_);
}
} // namespace network
} // namespace celeborn
