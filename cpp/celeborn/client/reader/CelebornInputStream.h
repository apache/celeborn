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

#include <thread>

#include "celeborn/client/compress/Decompressor.h"
#include "celeborn/client/reader/WorkerPartitionReader.h"
#include "celeborn/conf/CelebornConf.h"
#include "celeborn/utils/CelebornUtils.h"

namespace celeborn {
namespace client {
class CelebornInputStream {
 public:
  using FetchExcludedWorkers = utils::ConcurrentHashMap<std::string, int64_t>;

  CelebornInputStream(
      const std::string& shuffleKey,
      const std::shared_ptr<const conf::CelebornConf>& conf,
      const std::shared_ptr<network::TransportClientFactory>& clientFactory,
      std::vector<std::shared_ptr<const protocol::PartitionLocation>>&&
          locations,
      const std::vector<int>& attempts,
      int attemptNumber,
      int startMapIndex,
      int endMapIndex,
      bool needCompression,
      const std::shared_ptr<FetchExcludedWorkers>& fetchExcludedWorkers);

  int read(uint8_t* buffer, size_t offset, size_t len);

 private:
  bool fillBuffer();

  bool moveToNextChunk();

  std::unique_ptr<memory::ReadOnlyByteBuffer> getNextChunk();

  void verifyChunk(const std::unique_ptr<memory::ReadOnlyByteBuffer>& chunk);

  void moveToNextReader();

  std::shared_ptr<PartitionReader> createReaderWithRetry(
      const protocol::PartitionLocation& location);

  std::shared_ptr<PartitionReader> createReader(
      const protocol::PartitionLocation& location);

  bool isExcluded(const protocol::PartitionLocation& location);

  void excludeFailedFetchLocation(
      const std::string& hostAndFetchPort,
      const std::exception& e);

  std::shared_ptr<const protocol::PartitionLocation> nextReadableLocation();

  std::unordered_set<int>& getBatchRecord(int mapId);

  void cleanupReader();

  std::string shuffleKey_;
  std::shared_ptr<const conf::CelebornConf> conf_;
  std::shared_ptr<network::TransportClientFactory> clientFactory_;
  std::vector<std::shared_ptr<const protocol::PartitionLocation>> locations_;
  std::vector<int> attempts_;
  int attemptNumber_;
  int startMapIndex_;
  int endMapIndex_;
  bool shouldDecompress_;
  std::unique_ptr<compress::Decompressor> decompressor_;
  std::vector<uint8_t> compressedBuf_;

  int currLocationIndex_;
  std::unique_ptr<memory::ReadOnlyByteBuffer> currChunk_;
  std::unique_ptr<memory::ReadOnlyByteBuffer> decompressedChunk_;
  size_t currBatchPos_;
  size_t currBatchSize_;
  std::shared_ptr<PartitionReader> currReader_;
  std::vector<std::unique_ptr<std::unordered_set<int>>> batchRecords_;

  int fetchChunkRetryCnt_;
  int fetchChunkMaxRetry_;
  utils::Timeout retryWait_;
  std::shared_ptr<FetchExcludedWorkers> fetchExcludedWorkers_;
  int64_t fetchExcludedWorkerExpireTimeoutMs_;
  bool readSkewPartitionWithoutMapRange_;
};
} // namespace client
} // namespace celeborn
