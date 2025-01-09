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

#include "celeborn/network/MessageDecoder.h"

using namespace celeborn::network;

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
} // namespace

void testDecodeWithEndian(bool isBigEndian) {
  const std::string encodedContent = "test-encodedContent";
  const std::string bodyContent = "test-bodyContent";
  const int encodedLength = encodedContent.size();
  const uint8_t msgType = 10;
  const int bodyLength = bodyContent.size();
  const int frameLength =
      sizeof(int) + sizeof(uint8_t) + sizeof(int) + encodedLength + bodyLength;
  int endianEncodedLength = 0;
  int endianBodyLength = 0;
  if (isBigEndian) {
    writeBigEndian<int>(
        encodedLength, reinterpret_cast<uint8_t*>(&endianEncodedLength));
    writeBigEndian<int>(
        bodyLength, reinterpret_cast<uint8_t*>(&endianBodyLength));
  } else {
    writeLittleEndian<int>(
        encodedLength, reinterpret_cast<uint8_t*>(&endianEncodedLength));
    writeLittleEndian<int>(
        bodyLength, reinterpret_cast<uint8_t*>(&endianBodyLength));
  }

  size_t dummy;
  MessageDecoder decoder(isBigEndian);
  folly::IOBufQueue::Options option;
  option.cacheChainLength = true;
  folly::IOBufQueue queue(option);
  std::unique_ptr<folly::IOBuf> result;

  queue.append(folly::IOBuf::wrapBuffer(&endianEncodedLength, sizeof(int)));
  EXPECT_FALSE(decoder.decode(nullptr, queue, result, dummy));

  queue.append(folly::IOBuf::wrapBuffer(&msgType, sizeof(uint8_t)));
  EXPECT_FALSE(decoder.decode(nullptr, queue, result, dummy));

  queue.append(folly::IOBuf::wrapBuffer(&endianBodyLength, sizeof(int)));
  EXPECT_FALSE(decoder.decode(nullptr, queue, result, dummy));

  queue.append(
      folly::IOBuf::wrapBuffer(encodedContent.c_str(), encodedContent.size()));
  EXPECT_FALSE(decoder.decode(nullptr, queue, result, dummy));

  queue.append(
      folly::IOBuf::wrapBuffer(bodyContent.c_str(), bodyContent.size()));
  EXPECT_TRUE(decoder.decode(nullptr, queue, result, dummy));
  EXPECT_EQ(queue.chainLength(), 0);

  auto cursor = std::make_unique<folly::io::Cursor>(result.get());
  EXPECT_EQ(cursor->totalLength(), frameLength);
  if (isBigEndian) {
    EXPECT_EQ(cursor->readBE<int>(), encodedLength);
  } else {
    EXPECT_EQ(cursor->readLE<int>(), encodedLength);
  }
  EXPECT_EQ(cursor->read<uint8_t>(), msgType);
  if (isBigEndian) {
    EXPECT_EQ(cursor->readBE<int>(), bodyLength);
  } else {
    EXPECT_EQ(cursor->readLE<int>(), bodyLength);
  }
  EXPECT_EQ(cursor->readFixedString(encodedLength), encodedContent);
  EXPECT_EQ(cursor->readFixedString(bodyLength), bodyContent);
}

TEST(MessageDecoderTest, decodeBigEndian) {
  testDecodeWithEndian(true);
}

TEST(MessageDecoderTest, decodeLittleEndian) {
  testDecodeWithEndian(true);
}
