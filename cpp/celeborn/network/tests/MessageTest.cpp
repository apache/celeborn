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

#include "celeborn/network/Message.h"

using namespace celeborn;
using namespace celeborn::network;

TEST(MessageTest, encodeRpcRequest) {
  const std::string body = "test-body";
  auto bodyBuffer = memory::ByteBuffer::createWriteOnly(body.size());
  bodyBuffer->writeFromString(body);
  const long requestId = 1000;
  auto rpcRequest = std::make_unique<RpcRequest>(
      requestId, memory::ByteBuffer::toReadOnly(std::move(bodyBuffer)));

  auto encodedBuffer = rpcRequest->encode();
  EXPECT_EQ(encodedBuffer->read<int32_t>(), sizeof(long) + sizeof(int32_t));
  EXPECT_EQ(encodedBuffer->read<uint8_t>(), Message::Type::RPC_REQUEST);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), body.size());
  EXPECT_EQ(encodedBuffer->read<long>(), requestId);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), body.size());
  EXPECT_EQ(encodedBuffer->readToString(body.size()), body);
}

TEST(MessageTest, decodeRpcResponse) {
  const std::string body = "test-body";
  const long requestId = 1000;
  const int headerLength = sizeof(int32_t) + sizeof(uint8_t) + sizeof(int32_t);
  const int encodedLength = sizeof(long) + 4;
  const int bodyLength = body.size();
  size_t size = headerLength + encodedLength + bodyLength;
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(size);
  writeBuffer->write<int32_t>(encodedLength);
  writeBuffer->write<uint8_t>(Message::Type::RPC_RESPONSE);
  writeBuffer->write<int32_t>(bodyLength);
  writeBuffer->write<long>(requestId);
  writeBuffer->write<int32_t>(bodyLength);
  writeBuffer->writeFromString(body);
  auto message = Message::decodeFrom(
      memory::ByteBuffer::toReadOnly(std::move(writeBuffer)));
  EXPECT_EQ(message->type(), Message::Type::RPC_RESPONSE);
  auto rpcResponse = dynamic_cast<RpcResponse*>(message.get());
  EXPECT_EQ(rpcResponse->requestId(), requestId);
  auto rpcResponseBody = rpcResponse->body();
  EXPECT_EQ(rpcResponseBody->remainingSize(), body.size());
  EXPECT_EQ(rpcResponseBody->readToString(body.size()), body);
}

TEST(MessageTest, decodeRpcFailure) {
  const std::string failureMsg = "test-failure-msg";
  const long requestId = 1000;
  const int headerLength = sizeof(int32_t) + sizeof(uint8_t) + sizeof(int32_t);
  const int encodedLength = sizeof(long) + sizeof(int);
  const int failureMsgLength = failureMsg.size();
  size_t size = headerLength + encodedLength + failureMsgLength;
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(size);
  writeBuffer->write<int32_t>(encodedLength);
  writeBuffer->write<uint8_t>(Message::Type::RPC_FAILURE);
  writeBuffer->write<int32_t>(failureMsgLength);
  writeBuffer->write<long>(requestId);
  writeBuffer->write<int32_t>(failureMsgLength);
  writeBuffer->writeFromString(failureMsg);
  auto message = Message::decodeFrom(
      memory::ByteBuffer::toReadOnly(std::move(writeBuffer)));
  EXPECT_EQ(message->type(), Message::Type::RPC_FAILURE);
  auto rpcFailure = dynamic_cast<RpcFailure*>(message.get());
  EXPECT_EQ(rpcFailure->requestId(), requestId);
  auto rpcFailureBody = rpcFailure->body();
  EXPECT_EQ(rpcFailureBody->remainingSize(), 0);
  EXPECT_EQ(rpcFailure->errorMsg(), failureMsg);
}

TEST(MessageTest, decodeChunkFetchSuccess) {
  const long streamId = 1000;
  const int chunkIndex = 1001;
  const int offset = 1002;
  const int len = 1003;
  const std::string body = "test-body";
  const int headerLength = sizeof(int32_t) + sizeof(uint8_t) + sizeof(int32_t);
  const int encodedLength =
      sizeof(long) + sizeof(int) + sizeof(int) + sizeof(int);
  const int bodyLength = body.size();
  size_t size = headerLength + encodedLength + bodyLength;
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(size);
  writeBuffer->write<int32_t>(encodedLength);
  writeBuffer->write<uint8_t>(Message::Type::CHUNK_FETCH_SUCCESS);
  writeBuffer->write<int32_t>(bodyLength);
  writeBuffer->write<long>(streamId);
  writeBuffer->write<int>(chunkIndex);
  writeBuffer->write<int>(offset);
  writeBuffer->write<int>(len);
  writeBuffer->writeFromString(body);
  auto message = Message::decodeFrom(
      memory::ByteBuffer::toReadOnly(std::move(writeBuffer)));
  EXPECT_EQ(message->type(), Message::Type::CHUNK_FETCH_SUCCESS);
  auto chunkFetchSuccess = dynamic_cast<ChunkFetchSuccess*>(message.get());
  auto streamChunkSlice = chunkFetchSuccess->streamChunkSlice();
  EXPECT_EQ(streamChunkSlice.streamId, streamId);
  EXPECT_EQ(streamChunkSlice.chunkIndex, chunkIndex);
  EXPECT_EQ(streamChunkSlice.offset, offset);
  EXPECT_EQ(streamChunkSlice.len, len);
  auto chunkFetchSuccessBody = chunkFetchSuccess->body();
  EXPECT_EQ(chunkFetchSuccessBody->remainingSize(), body.size());
  EXPECT_EQ(chunkFetchSuccessBody->readToString(body.size()), body);
}

TEST(MessageTest, decodeChunkFetchFailure) {
  const long streamId = 1000;
  const int chunkIndex = 1001;
  const int offset = 1002;
  const int len = 1003;
  const std::string failureMsg = "test-failure-msg";
  const int headerLength = sizeof(int32_t) + sizeof(uint8_t) + sizeof(int32_t);
  const int encodedLength =
      sizeof(long) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int);
  const int failureMsgLength = failureMsg.size();
  size_t size = headerLength + encodedLength + failureMsgLength;
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(size);
  writeBuffer->write<int32_t>(encodedLength);
  writeBuffer->write<uint8_t>(Message::Type::CHUNK_FETCH_FAILURE);
  writeBuffer->write<int32_t>(failureMsgLength);
  writeBuffer->write<long>(streamId);
  writeBuffer->write<int>(chunkIndex);
  writeBuffer->write<int>(offset);
  writeBuffer->write<int>(len);
  writeBuffer->write<int>(failureMsgLength);
  writeBuffer->writeFromString(failureMsg);
  auto message = Message::decodeFrom(
      memory::ByteBuffer::toReadOnly(std::move(writeBuffer)));
  EXPECT_EQ(message->type(), Message::Type::CHUNK_FETCH_FAILURE);
  auto chunkFetchFailure = dynamic_cast<ChunkFetchFailure*>(message.get());
  auto streamChunkSlice = chunkFetchFailure->streamChunkSlice();
  EXPECT_EQ(streamChunkSlice.streamId, streamId);
  EXPECT_EQ(streamChunkSlice.chunkIndex, chunkIndex);
  EXPECT_EQ(streamChunkSlice.offset, offset);
  EXPECT_EQ(streamChunkSlice.len, len);
  EXPECT_EQ(chunkFetchFailure->errorMsg(), failureMsg);
}

TEST(MessageTest, encodePushData) {
  const std::string body = "test-body";
  auto bodyBuffer = memory::ByteBuffer::createWriteOnly(body.size());
  bodyBuffer->writeFromString(body);
  const long requestId = 1000;
  const uint8_t mode = 2;
  const std::string shuffleKey = "test-shuffle-key";
  const std::string partitionUniqueId = "test-partition-id";
  auto pushData = std::make_unique<PushData>(
      requestId,
      mode,
      shuffleKey,
      partitionUniqueId,
      memory::ByteBuffer::toReadOnly(std::move(bodyBuffer)));

  auto encodedBuffer = pushData->encode();
  EXPECT_EQ(
      encodedBuffer->read<int32_t>(),
      sizeof(long) + sizeof(uint8_t) + sizeof(int) + shuffleKey.size() +
          sizeof(int) + partitionUniqueId.size());
  EXPECT_EQ(encodedBuffer->read<uint8_t>(), Message::Type::PUSH_DATA);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), body.size());
  EXPECT_EQ(encodedBuffer->read<long>(), requestId);
  EXPECT_EQ(encodedBuffer->read<uint8_t>(), mode);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), shuffleKey.size());
  EXPECT_EQ(encodedBuffer->readToString(shuffleKey.size()), shuffleKey);
  EXPECT_EQ(encodedBuffer->read<int32_t>(), partitionUniqueId.size());
  EXPECT_EQ(
      encodedBuffer->readToString(partitionUniqueId.size()), partitionUniqueId);
}
