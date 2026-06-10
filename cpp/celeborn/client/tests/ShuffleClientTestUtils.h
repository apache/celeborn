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

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/writer/PushState.h"

namespace celeborn {
namespace client {

// Endpoint shared by all test ShuffleClientImpl subclasses; its resources
// (thread pools, client factory) are never exercised by the unit tests.
inline const ShuffleClientEndpoint& testDummyEndpoint() {
  static auto conf = std::make_shared<conf::CelebornConf>();
  static auto dummy = ShuffleClientEndpoint(conf);
  return dummy;
}

// Base for test ShuffleClientImpl subclasses: supplies the construction
// boilerplate (app id + dummy endpoint) so each mock only adds the hooks it
// captures. The conf is injectable to toggle the features under test.
class TestShuffleClientBase : public ShuffleClientImpl {
 protected:
  explicit TestShuffleClientBase(
      std::shared_ptr<const conf::CelebornConf> conf =
          std::make_shared<conf::CelebornConf>())
      : ShuffleClientImpl("test-app", conf, testDummyEndpoint()) {}
};

// Single status-byte response, as a worker returns it to the push callbacks.
inline std::unique_ptr<memory::ReadOnlyByteBuffer> createStatusResponse(
    uint8_t code) {
  auto writeBuffer = memory::ByteBuffer::createWriteOnly(1);
  writeBuffer->write<uint8_t>(code);
  return memory::ByteBuffer::toReadOnly(std::move(writeBuffer));
}

// PushState with a short in-flight timeout so a regression (a batch never
// released) makes limitZeroInFlight time out quickly instead of hanging.
inline std::shared_ptr<PushState> makeShortTimeoutPushState() {
  conf::CelebornConf conf;
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitInFlightTimeoutMs, "200");
  conf.registerProperty(
      conf::CelebornConf::kClientPushLimitInFlightSleepDeltaMs, "10");
  return std::make_shared<PushState>(conf);
}

} // namespace client
} // namespace celeborn
