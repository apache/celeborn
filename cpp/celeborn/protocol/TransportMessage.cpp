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

#include "celeborn/protocol/TransportMessage.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace protocol {
TransportMessage::TransportMessage(MessageType type, std::string&& payload)
    : type_(type), payload_(std::move(payload)) {
  messageTypeValue_ = type;
}

TransportMessage::TransportMessage(
    std::unique_ptr<memory::ReadOnlyByteBuffer> buf) {
  int messageTypeValue = buf->read<int32_t>();
  int payloadLen = buf->read<int32_t>();
  CELEBORN_CHECK_EQ(buf->remainingSize(), payloadLen);
  CELEBORN_CHECK(MessageType_IsValid(messageTypeValue));
  type_ = static_cast<MessageType>(messageTypeValue);
  messageTypeValue_ = type_;
  payload_ = buf->readToString(payloadLen);
}

std::unique_ptr<memory::ReadOnlyByteBuffer>
TransportMessage::toReadOnlyByteBuffer() const {
  int bufSize = payload_.size() + 4 + 4;
  auto buffer = memory::ByteBuffer::createWriteOnly(bufSize);
  buffer->write<int>(messageTypeValue_);
  buffer->write<int>(payload_.size());
  buffer->writeFromString(payload_);
  CELEBORN_CHECK_EQ(buffer->size(), bufSize);
  return memory::ByteBuffer::toReadOnly(std::move(buffer));
}
} // namespace protocol
} // namespace celeborn
