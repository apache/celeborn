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

#include "celeborn/memory/ByteBuffer.h"

namespace celeborn {
namespace memory {
std::unique_ptr<WriteOnlyByteBuffer> ByteBuffer::createWriteOnly(
    size_t initialCapacity,
    bool isBigEndian) {
  return std::make_unique<WriteOnlyByteBuffer>(initialCapacity, isBigEndian);
}

std::unique_ptr<ReadOnlyByteBuffer> ByteBuffer::createReadOnly(
    std::unique_ptr<folly::IOBuf>&& data,
    bool isBigEndian) {
  return std::make_unique<ReadOnlyByteBuffer>(std::move(data), isBigEndian);
}

std::unique_ptr<ReadOnlyByteBuffer> ByteBuffer::toReadOnly(
    std::unique_ptr<ByteBuffer>&& buffer) {
  return std::make_unique<ReadOnlyByteBuffer>(
      std::move(buffer->data_), buffer->isBigEndian_);
}

std::unique_ptr<ReadOnlyByteBuffer> ByteBuffer::concat(
    const ReadOnlyByteBuffer& left,
    const ReadOnlyByteBuffer& right) {
  assert(left.isBigEndian_ == right.isBigEndian_);
  bool isBigEndian = left.isBigEndian_;
  if (left.remainingSize() == 0) {
    return std::make_unique<ReadOnlyByteBuffer>(right);
  }
  if (right.remainingSize() == 0) {
    return std::make_unique<ReadOnlyByteBuffer>(left);
  }

  auto leftData = trimBuffer(left);
  auto rightData = trimBuffer(right);
  assert(leftData);
  assert(rightData);
  leftData->appendToChain(std::move(rightData));
  return createReadOnly(std::move(leftData), isBigEndian);
}

std::unique_ptr<folly::IOBuf> ByteBuffer::trimBuffer(
    const ReadOnlyByteBuffer& buffer) {
  auto data = buffer.data_->clone();
  auto pos = buffer.cursor_->getCurrentPosition();
  while (pos > 0 && data) {
    if (pos >= data->length()) {
      auto next = data->pop();
      auto curr = std::move(data);
      data = std::move(next);
      pos -= curr->length();
    } else {
      data->trimStart(pos);
      pos = 0;
    }
  }
  return std::move(data);
}
} // namespace memory
} // namespace celeborn
