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

#include <fstream>

#include "celeborn/client/reader/PartitionReader.h"

namespace celeborn {
namespace conf {
class CelebornConf;
} // namespace conf

namespace network {
class TransportClient;
} // namespace network

namespace protocol {
struct StreamHandler;
} // namespace protocol

namespace client {

class LocalPartitionReader : public PartitionReader {
 public:
  static std::shared_ptr<LocalPartitionReader> create(
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::string& shuffleKey,
      const protocol::PartitionLocation& location,
      int32_t startMapIndex,
      int32_t endMapIndex,
      std::shared_ptr<network::TransportClient> client);

  ~LocalPartitionReader() override;

  bool hasNext() override;

  std::unique_ptr<memory::ReadOnlyByteBuffer> next() override;

  const protocol::PartitionLocation& getLocation() const override;

 private:
  LocalPartitionReader(
      const protocol::PartitionLocation& location,
      std::shared_ptr<network::TransportClient> client);

  void openFile();

  protocol::PartitionLocation location_;
  std::shared_ptr<network::TransportClient> client_;
  std::unique_ptr<protocol::StreamHandler> streamHandler_;
  std::ifstream file_;
  size_t nextChunk_{0};
};

} // namespace client
} // namespace celeborn
