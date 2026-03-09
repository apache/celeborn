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

#include "celeborn/protocol/Encoders.h"

namespace celeborn {
namespace protocol {

int encodedLength(const std::string& msg) {
  return sizeof(int) + msg.size();
}

void encode(memory::WriteOnlyByteBuffer& buffer, const std::string& msg) {
  buffer.write<int>(msg.size());
  buffer.writeFromString(msg);
}

std::string decode(memory::ReadOnlyByteBuffer& buffer) {
  int size = buffer.read<int>();
  return buffer.readToString(size);
}

int encodedLength(const std::vector<std::string>& arr) {
  int total = sizeof(int);
  for (const auto& s : arr) {
    total += encodedLength(s);
  }
  return total;
}

void encode(
    memory::WriteOnlyByteBuffer& buffer,
    const std::vector<std::string>& arr) {
  buffer.write<int>(static_cast<int>(arr.size()));
  for (const auto& s : arr) {
    encode(buffer, s);
  }
}

std::vector<std::string> decodeStringArray(memory::ReadOnlyByteBuffer& buffer) {
  int count = buffer.read<int>();
  std::vector<std::string> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    result.push_back(decode(buffer));
  }
  return result;
}

int encodedLength(const std::vector<int32_t>& arr) {
  return sizeof(int) + sizeof(int32_t) * arr.size();
}

void encode(
    memory::WriteOnlyByteBuffer& buffer,
    const std::vector<int32_t>& arr) {
  buffer.write<int>(static_cast<int>(arr.size()));
  for (auto val : arr) {
    buffer.write<int32_t>(val);
  }
}

std::vector<int32_t> decodeIntArray(memory::ReadOnlyByteBuffer& buffer) {
  int count = buffer.read<int>();
  std::vector<int32_t> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    result.push_back(buffer.read<int32_t>());
  }
  return result;
}

} // namespace protocol
} // namespace celeborn
