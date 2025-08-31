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

#pragma once

#include <cstdint>

#include "celeborn/memory/ByteBuffer.h"
#include "celeborn/protocol/ControlMessages.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace network {
/// the Message  = RpcRequest, RpcResponse, ... is the direct object that
/// decoded/encoded by the java's netty encode/decode stack. The message is then
/// decoded again to get application-level objects.
class Message {
 public:
  enum Type {
    UNKNOWN_TYPE = -1,
    CHUNK_FETCH_REQUEST = 0,
    CHUNK_FETCH_SUCCESS = 1,
    CHUNK_FETCH_FAILURE = 2,
    RPC_REQUEST = 3,
    RPC_RESPONSE = 4,
    RPC_FAILURE = 5,
    OPEN_STREAM = 6,
    STREAM_HANDLE = 7,
    ONE_WAY_MESSAGE = 9,
    PUSH_DATA = 11,
    PUSH_MERGED_DATA = 12,
    REGION_START = 13,
    REGION_FINISH = 14,
    PUSH_DATA_HAND_SHAKE = 15,
    READ_ADD_CREDIT = 16,
    READ_DATA = 17,
    OPEN_STREAM_WITH_CREDIT = 18,
    BACKLOG_ANNOUNCEMENT = 19,
    TRANSPORTABLE_ERROR = 20,
    BUFFER_STREAM_END = 21,
    HEARTBEAT = 22,
  };

  static Type decodeType(uint8_t typeId);

  Message(Type type, std::unique_ptr<memory::ReadOnlyByteBuffer>&& body)
      : type_(type), body_(std::move(body)) {}

  virtual ~Message() = default;

  Type type() const {
    return type_;
  }

  std::unique_ptr<memory::ReadOnlyByteBuffer> body() const {
    return body_->clone();
  }

  std::unique_ptr<memory::ReadOnlyByteBuffer> encode() const;

  static std::unique_ptr<Message> decodeFrom(
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& data);

  static long nextRequestId() {
    return currRequestId_.fetch_add(1);
  }

 protected:
  virtual int internalEncodedLength() const {
    CELEBORN_UNREACHABLE(
        "unsupported message encodeLength type " + std::to_string(type_));
  }

  virtual void internalEncodeTo(memory::WriteOnlyByteBuffer& buffer) const {
    CELEBORN_UNREACHABLE(
        "unsupported message internalEncodeTo type " + std::to_string(type_));
  }

  Type type_;
  std::unique_ptr<memory::ReadOnlyByteBuffer> body_;

  static std::atomic<long> currRequestId_;
};

class RpcRequest : public Message {
  // TODO: add decode method when required
 public:
  RpcRequest(long requestId, std::unique_ptr<memory::ReadOnlyByteBuffer>&& buf)
      : Message(Type::RPC_REQUEST, std::move(buf)), requestId_(requestId) {}

  RpcRequest(const RpcRequest& other)
      : Message(RPC_REQUEST, other.body_->clone()),
        requestId_(other.requestId_) {}

  void operator=(const RpcRequest& lhs) {
    requestId_ = lhs.requestId();
    body_ = lhs.body_->clone();
  }

  virtual ~RpcRequest() = default;

  long requestId() const {
    return requestId_;
  }

 private:
  int internalEncodedLength() const override;

  void internalEncodeTo(memory::WriteOnlyByteBuffer& buffer) const override;

  long requestId_;
};

class RpcResponse : public Message {
  // TODO: add decode method when required
 public:
  RpcResponse(
      long requestId,
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& body)
      : Message(RPC_RESPONSE, std::move(body)), requestId_(requestId) {}

  RpcResponse(const RpcResponse& lhs)
      : Message(RPC_RESPONSE, lhs.body_->clone()), requestId_(lhs.requestId_) {}

  void operator=(const RpcResponse& lhs) {
    requestId_ = lhs.requestId();
    body_ = lhs.body_->clone();
  }

  long requestId() const {
    return requestId_;
  }

  static std::unique_ptr<RpcResponse> decodeFrom(
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& data);

 private:
  long requestId_;
};

class RpcFailure : public Message {
 public:
  RpcFailure(long requestId, std::string&& errorString)
      : Message(RPC_FAILURE, memory::ReadOnlyByteBuffer::createEmptyBuffer()),
        requestId_(requestId),
        errorString_(std::move(errorString)) {}

  RpcFailure(const RpcFailure& other)
      : Message(RPC_FAILURE, memory::ReadOnlyByteBuffer::createEmptyBuffer()),
        requestId_(other.requestId_),
        errorString_(other.errorString_) {}

  long requestId() const {
    return requestId_;
  }

  std::string errorMsg() const {
    return errorString_;
  }

  static std::unique_ptr<RpcFailure> decodeFrom(
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& data);

 private:
  long requestId_;
  std::string errorString_;
};

class ChunkFetchSuccess : public Message {
 public:
  ChunkFetchSuccess(
      const protocol::StreamChunkSlice& streamChunkSlice,
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& body)
      : Message(CHUNK_FETCH_SUCCESS, std::move(body)),
        streamChunkSlice_(streamChunkSlice) {}

  ChunkFetchSuccess(const ChunkFetchSuccess& other)
      : Message(CHUNK_FETCH_SUCCESS, other.body_->clone()),
        streamChunkSlice_(other.streamChunkSlice_) {}

  static std::unique_ptr<ChunkFetchSuccess> decodeFrom(
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& data);

  protocol::StreamChunkSlice streamChunkSlice() const {
    return streamChunkSlice_;
  }

 private:
  protocol::StreamChunkSlice streamChunkSlice_;
};

class ChunkFetchFailure : public Message {
 public:
  ChunkFetchFailure(
      const protocol::StreamChunkSlice& streamChunkSlice,
      std::string&& errorString)
      : Message(
            CHUNK_FETCH_FAILURE,
            memory::ReadOnlyByteBuffer::createEmptyBuffer()),
        streamChunkSlice_(streamChunkSlice),
        errorString_(std::move(errorString)) {}

  ChunkFetchFailure(const ChunkFetchFailure& other)
      : Message(
            CHUNK_FETCH_FAILURE,
            memory::ReadOnlyByteBuffer::createEmptyBuffer()),
        streamChunkSlice_(other.streamChunkSlice_),
        errorString_(other.errorString_) {}

  static std::unique_ptr<ChunkFetchFailure> decodeFrom(
      std::unique_ptr<memory::ReadOnlyByteBuffer>&& data);

  protocol::StreamChunkSlice streamChunkSlice() const {
    return streamChunkSlice_;
  }

  std::string errorMsg() const {
    return errorString_;
  }

 private:
  protocol::StreamChunkSlice streamChunkSlice_;
  std::string errorString_;
};

class PushData : public Message {
 public:
  PushData(
      long requestId,
      uint8_t mode,
      const std::string& shuffleKey,
      const std::string& partitionUniqueId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body);

  PushData(const PushData& other);

  long requestId() const {
    return requestId_;
  }

 private:
  int internalEncodedLength() const override;

  void internalEncodeTo(memory::WriteOnlyByteBuffer& buffer) const override;

  long requestId_;
  // 0 for primary, 1 for replica. Ref to PartitionLocation::Mode.
  uint8_t mode_;
  std::string shuffleKey_;
  std::string partitionUniqueId_;
};
} // namespace network
} // namespace celeborn
