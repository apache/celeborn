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

#include "celeborn/network/TransportClient.h"
#include "celeborn/protocol/PartitionLocation.h"

namespace celeborn {
namespace client {
class PartitionReader {
 public:
  virtual ~PartitionReader() = default;

  virtual bool hasNext() = 0;

  virtual std::unique_ptr<memory::ReadOnlyByteBuffer> next() = 0;
};

class WorkerPartitionReader
    : public PartitionReader,
      public std::enable_shared_from_this<WorkerPartitionReader> {
 public:
  // Only allow using create method to get the shared_ptr holder. This is
  // required by the std::enable_shared_from_this functionality.
  static std::shared_ptr<WorkerPartitionReader> create(
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::string& shuffleKey,
      const protocol::PartitionLocation& location,
      int32_t startMapIndex,
      int32_t endMapIndex,
      network::TransportClientFactory* clientFactory);

  ~WorkerPartitionReader() override;

  bool hasNext() override;

  std::unique_ptr<memory::ReadOnlyByteBuffer> next() override;

 private:
  // Disable creating the object directly to make sure that
  // std::enable_shared_from_this works properly.
  WorkerPartitionReader(
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::string& shuffleKey,
      const protocol::PartitionLocation& location,
      int32_t startMapIndex,
      int32_t endMapIndex,
      network::TransportClientFactory* clientFactory);

  void fetchChunks();

  // This function cannot be called within constructor!
  void initAndCheck();

  std::string shuffleKey_;
  protocol::PartitionLocation location_;
  std::shared_ptr<network::TransportClient> client_;
  int32_t startMapIndex_;
  int32_t endMapIndex_;
  std::unique_ptr<protocol::StreamHandler> streamHandler_;

  int32_t fetchingChunkId_;
  int32_t toConsumeChunkId_;
  int32_t maxFetchChunksInFlight_;
  Timeout fetchTimeout_;

  folly::UMPSCQueue<std::unique_ptr<memory::ReadOnlyByteBuffer>, true>
      chunkQueue_;
  network::FetchChunkSuccessCallback onSuccess_;
  network::FetchChunkFailureCallback onFailure_;
  folly::Synchronized<std::unique_ptr<std::exception>> exception_;

  static constexpr auto kDefaultConsumeIter = std::chrono::milliseconds(500);

  // TODO: add other params, such as fetchChunkRetryCnt, fetchChunkMaxRetry
};
} // namespace client
} // namespace celeborn
