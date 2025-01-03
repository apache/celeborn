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

#include "celeborn/proto/TransportMessagesCpp.pb.h"
#include "celeborn/protocol/TransportMessage.h"

using namespace celeborn::protocol;

TEST(TransportMessageTest, constructFromPayload) {
  const std::string payload = "payload";
  auto payloadToMove = payload;
  auto messageType = 10;
  auto transportMessage = std::make_unique<TransportMessage>(
      static_cast<MessageType>(messageType), std::move(payloadToMove));

  auto transportMessageBuffer = transportMessage->toReadOnlyByteBuffer();
  EXPECT_EQ(
      transportMessageBuffer->remainingSize(),
      sizeof(int) * 2 + payload.size());
  EXPECT_EQ(transportMessageBuffer->read<int>(), messageType);
  EXPECT_EQ(transportMessageBuffer->read<int>(), payload.size());
  EXPECT_EQ(transportMessageBuffer->readToString(), payload);
}

TEST(TransportMessageTest, constructFromReadOnlyBuffer) {
  const std::string payload = "payload";
  auto payloadToMove = payload;
  auto messageType = 10;
  auto contentMessage = std::make_unique<TransportMessage>(
      static_cast<MessageType>(messageType), std::move(payloadToMove));
  auto contentBuffer = contentMessage->toReadOnlyByteBuffer();

  auto transportMessage =
      std::make_unique<TransportMessage>(std::move(contentBuffer));
  auto transportMessageBuffer = transportMessage->toReadOnlyByteBuffer();
  EXPECT_EQ(
      transportMessageBuffer->remainingSize(),
      sizeof(int) * 2 + payload.size());
  EXPECT_EQ(transportMessageBuffer->read<int>(), messageType);
  EXPECT_EQ(transportMessageBuffer->read<int>(), payload.size());
  EXPECT_EQ(transportMessageBuffer->readToString(), payload);
}
