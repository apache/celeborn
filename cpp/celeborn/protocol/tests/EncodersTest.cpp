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

#include "celeborn/protocol/Encoders.h"

using namespace celeborn;
using namespace celeborn::protocol;

TEST(EncodersTest, encodedLength) {
  std::string testString = "test-string";
  EXPECT_EQ(encodedLength(testString), sizeof(int) + testString.size());
}

TEST(EncodersTest, encode) {
  std::string testString = "test-string";
  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(sizeof(int) + testString.size());
  encode(*writeBuffer, testString);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  EXPECT_EQ(readBuffer->read<int>(), testString.size());
  EXPECT_EQ(readBuffer->readToString(testString.size()), testString);
}

TEST(EncodersTest, decode) {
  std::string testString = "test-string";
  auto writeBuffer =
      memory::ByteBuffer::createWriteOnly(sizeof(int) + testString.size());
  writeBuffer->write<int>(testString.size());
  writeBuffer->writeFromString(testString);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  EXPECT_EQ(decode(*readBuffer), testString);
}

TEST(EncodersTest, stringArrayEncodedLength) {
  std::vector<std::string> arr = {"hello", "world", "test"};
  int expected = sizeof(int);
  for (const auto& s : arr) {
    expected += sizeof(int) + s.size();
  }
  EXPECT_EQ(encodedLength(arr), expected);
}

TEST(EncodersTest, stringArrayEmpty) {
  std::vector<std::string> arr;
  EXPECT_EQ(encodedLength(arr), sizeof(int));
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(encodedLength(arr));
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto decoded = decodeStringArray(*readBuffer);
  EXPECT_EQ(decoded.size(), 0);
}

TEST(EncodersTest, stringArrayEncodeAndDecode) {
  std::vector<std::string> arr = {"partition-0", "partition-1", "partition-2"};
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(encodedLength(arr));
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto decoded = decodeStringArray(*readBuffer);
  EXPECT_EQ(decoded.size(), arr.size());
  for (size_t i = 0; i < arr.size(); i++) {
    EXPECT_EQ(decoded[i], arr[i]);
  }
}

TEST(EncodersTest, intArrayEncodedLength) {
  std::vector<int32_t> arr = {10, 20, 30, 40};
  EXPECT_EQ(
      encodedLength(arr),
      static_cast<int>(sizeof(int) + sizeof(int32_t) * arr.size()));
}

TEST(EncodersTest, intArrayEmpty) {
  std::vector<int32_t> arr;
  EXPECT_EQ(encodedLength(arr), sizeof(int));
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(encodedLength(arr));
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto decoded = decodeIntArray(*readBuffer);
  EXPECT_EQ(decoded.size(), 0);
}

TEST(EncodersTest, intArrayEncodeAndDecode) {
  std::vector<int32_t> arr = {0, 100, 250, 500};
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(encodedLength(arr));
  encode(*writeBuffer, arr);
  auto readBuffer = memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
  auto decoded = decodeIntArray(*readBuffer);
  EXPECT_EQ(decoded.size(), arr.size());
  for (size_t i = 0; i < arr.size(); i++) {
    EXPECT_EQ(decoded[i], arr[i]);
  }
}
