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

#include "celeborn/memory/ByteBuffer.h"

using namespace celeborn;

namespace {
template <typename T>
uint8_t* writeBigEndian(T t, uint8_t* data) {
  const size_t size = sizeof(T);
  const T mask = static_cast<T>(0xFF) << ((size - 1) * 8);
  for (int i = 0; i < size; i++) {
    T maskT = t & mask;
    uint8_t byte = static_cast<uint8_t>(maskT >> ((size - 1) * 8));
    data[i] = byte;
    // We only use leftShift here to avoid arithmetic shifting.
    t <<= 8;
  }
  return data + size;
}

template <typename T>
uint8_t* writeLittleEndian(T t, uint8_t* data) {
  const size_t size = sizeof(T);
  const T mask = static_cast<T>(0xFF);
  for (int i = 0; i < size; i++) {
    T maskT = t & mask;
    uint8_t byte = static_cast<uint8_t>(maskT);
    data[i] = byte;
    t >>= 8;
  }
  return data + size;
}

const std::string strPayload = "this is a test";
const int16_t int16Payload = 0xBEEF;
const int32_t int32Payload = 0xBAADBEEF;
const int64_t int64Payload = 0xBAADBEEFBAADBEEF;

} // namespace

const size_t testSize = strPayload.size() +
    (sizeof(int16_t) + sizeof(int32_t) + sizeof(int64_t)) * 2;

std::unique_ptr<uint8_t> createRawData(size_t& size) {
  size = testSize;
  auto data = std::unique_ptr<uint8_t>(new uint8_t[size]);

  uint8_t* curr = data.get();
  memcpy(curr, strPayload.c_str(), strPayload.size());
  curr += strPayload.size();
  curr = writeBigEndian(int16Payload, curr);
  curr = writeBigEndian(int32Payload, curr);
  curr = writeBigEndian(int64Payload, curr);
  curr = writeLittleEndian(int16Payload, curr);
  curr = writeLittleEndian(int32Payload, curr);
  curr = writeLittleEndian(int64Payload, curr);
  EXPECT_EQ(curr, data.get() + size);
  return std::move(data);
}

std::unique_ptr<WriteOnlyByteBuffer> createWriteOnlyBuffer(size_t& size) {
  size = testSize;
  auto writeBuffer = ByteBuffer::createWriteOnly(size);
  EXPECT_EQ(writeBuffer->size(), 0);
  writeBuffer->writeFromString(strPayload);
  EXPECT_EQ(writeBuffer->size(), strPayload.size());
  writeBuffer->writeBE(int16Payload);
  writeBuffer->writeBE(int32Payload);
  writeBuffer->writeBE(int64Payload);
  writeBuffer->writeLE(int16Payload);
  writeBuffer->writeLE(int32Payload);
  writeBuffer->writeLE(int64Payload);
  EXPECT_EQ(writeBuffer->size(), size);
  return std::move(writeBuffer);
}

void testReadData(ReadOnlyByteBuffer* readBuffer, size_t size) {
  EXPECT_EQ(size, testSize);
  size_t remainingSize = size;
  EXPECT_EQ(readBuffer->size(), size);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);

  // Test read string.
  auto strRead = readBuffer->readToString(strPayload.size());
  EXPECT_EQ(strRead, strPayload);
  remainingSize -= strPayload.size();
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);

  // Test read BigEndian.
  EXPECT_EQ(readBuffer->readBE<int16_t>(), int16Payload);
  remainingSize -= sizeof(int16_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(readBuffer->readBE<int32_t>(), int32Payload);
  remainingSize -= sizeof(int32_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(readBuffer->readBE<int64_t>(), int64Payload);
  remainingSize -= sizeof(int64_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);

  // Test read LittleEndian.
  EXPECT_EQ(readBuffer->readLE<int16_t>(), int16Payload);
  remainingSize -= sizeof(int16_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(readBuffer->readLE<int32_t>(), int32Payload);
  remainingSize -= sizeof(int32_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(readBuffer->readLE<int64_t>(), int64Payload);
  remainingSize -= sizeof(int64_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);

  // Test retreat and skip.
  const auto retreatSize = sizeof(int32_t) + sizeof(int64_t);
  remainingSize += retreatSize;
  readBuffer->retreat(retreatSize);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(readBuffer->readLE<int32_t>(), int32Payload);
  remainingSize -= sizeof(int32_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);
  readBuffer->skip(sizeof(int64_t));
  remainingSize -= sizeof(int64_t);
  EXPECT_EQ(readBuffer->remainingSize(), remainingSize);

  // Test read end.
  EXPECT_EQ(readBuffer->size(), size);
  EXPECT_EQ(readBuffer->remainingSize(), 0);
  EXPECT_THROW(readBuffer->readLE<int64_t>(), std::exception);
}

TEST(ByteBufferTest, continuousBufferRead) {
  size_t size = 0;
  auto data = createRawData(size);
  auto ioBuf = folly::IOBuf::wrapBuffer(data.get(), size);

  auto readBuffer = ByteBuffer::createReadOnly(std::move(ioBuf));
  testReadData(readBuffer.get(), size);
}

TEST(ByteBufferTest, segmentedBufferRead) {
  size_t size = 0;
  auto data = createRawData(size);

  std::vector<size_t> sizes(4);
  std::vector<std::unique_ptr<uint8_t>> segments(4);
  sizes[0] = 7;
  sizes[1] = 11;
  sizes[2] = 17;
  sizes[3] = size - sizes[0] - sizes[1] - sizes[2];
  EXPECT_GT(sizes[3], 0);
  std::unique_ptr<folly::IOBuf> ioBuf;
  // Create segmented IOBuf source data.
  for (int i = 0, accSize = 0; i < sizes.size(); i++) {
    segments[i] = std::unique_ptr<uint8_t>(new uint8_t[sizes[i]]);
    auto segmentBuf = folly::IOBuf::wrapBuffer(segments[i].get(), sizes[i]);
    memcpy(segments[i].get(), data.get() + accSize, sizes[i]);
    if (ioBuf) {
      ioBuf->appendToChain(std::move(segmentBuf));
    } else {
      ioBuf = std::move(segmentBuf);
    }
    accSize += sizes[i];
  }

  auto readBuffer = ByteBuffer::createReadOnly(std::move(ioBuf));
  testReadData(readBuffer.get(), size);
}

TEST(ByteBufferTest, writeBufferAndRead) {
  size_t size = 0;
  auto writeBuffer = createWriteOnlyBuffer(size);
  auto readBuffer = WriteOnlyByteBuffer::toReadOnly(std::move(writeBuffer));
  testReadData(readBuffer.get(), size);
}

TEST(ByteBufferTest, concatReadBuffer) {
  size_t size = 0;
  auto writeBuffer1 = createWriteOnlyBuffer(size);
  auto readBuffer1 = WriteOnlyByteBuffer::toReadOnly(std::move(writeBuffer1));
  testReadData(readBuffer1.get(), size);
  auto writeBuffer2 = createWriteOnlyBuffer(size);
  auto readBuffer2 = WriteOnlyByteBuffer::toReadOnly(std::move(writeBuffer2));
  testReadData(readBuffer2.get(), size);
  auto retreatSize1 = sizeof(int32_t) + sizeof(int64_t);
  auto retreatSize2 = sizeof(int64_t);
  readBuffer1->retreat(retreatSize1);
  readBuffer2->retreat(retreatSize2);
  auto concatedReadBuffer = ByteBuffer::concat(*readBuffer1, *readBuffer2);
  auto remainingSize = retreatSize1 + retreatSize2;
  EXPECT_EQ(concatedReadBuffer->remainingSize(), remainingSize);

  // Read content of original readBuffer1.
  EXPECT_EQ(concatedReadBuffer->readLE<int32_t>(), int32Payload);
  remainingSize -= sizeof(int32_t);
  EXPECT_EQ(concatedReadBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(concatedReadBuffer->readLE<int64_t>(), int64Payload);
  remainingSize -= sizeof(int64_t);
  EXPECT_EQ(concatedReadBuffer->remainingSize(), remainingSize);

  // Read content of original readBuffer2.
  EXPECT_EQ(concatedReadBuffer->readLE<int64_t>(), int64Payload);
  remainingSize -= sizeof(int64_t);
  EXPECT_EQ(concatedReadBuffer->remainingSize(), remainingSize);
  EXPECT_EQ(concatedReadBuffer->remainingSize(), 0);
}