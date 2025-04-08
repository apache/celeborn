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

#include "celeborn/client/reader/CelebornInputStream.h"

namespace celeborn {
namespace client {
CelebornInputStream::CelebornInputStream(
    const std::string& shuffleKey,
    const std::shared_ptr<const conf::CelebornConf>& conf,
    const std::shared_ptr<network::TransportClientFactory>& clientFactory,
    std::vector<std::shared_ptr<const protocol::PartitionLocation>>&& locations,
    const std::vector<int>& attempts,
    int attemptNumber,
    int startMapIndex,
    int endMapIndex)
    : shuffleKey_(shuffleKey),
      conf_(conf),
      clientFactory_(clientFactory),
      locations_(std::move(locations)),
      attempts_(attempts),
      attemptNumber_(attemptNumber),
      startMapIndex_(startMapIndex),
      endMapIndex_(endMapIndex),
      currLocationIndex_(0),
      currBatchPos_(0),
      currBatchSize_(0) {
  moveToNextReader();
}

int CelebornInputStream::read(uint8_t* buffer, size_t offset, size_t len) {
  CELEBORN_CHECK_NOT_NULL(buffer);
  uint8_t* buf = buffer + offset;
  if (len <= 0)
    return 0;

  size_t readBytes = 0;
  while (readBytes < len) {
    while (currBatchPos_ >= currBatchSize_) {
      if (!fillBuffer()) {
        return readBytes > 0 ? readBytes : -1;
      }
    }
    size_t batchRemainingSize = currBatchSize_ - currBatchPos_;
    size_t toReadBytes = std::min(len - readBytes, batchRemainingSize);
    CELEBORN_CHECK_GE(currChunk_->remainingSize(), toReadBytes);
    auto size = currChunk_->readToBuffer(&buf[readBytes], toReadBytes);
    CELEBORN_CHECK_EQ(toReadBytes, size);
    readBytes += toReadBytes;
    currBatchPos_ += toReadBytes;
  }
  return readBytes;
}

bool CelebornInputStream::fillBuffer() {
  if (!currChunk_) {
    return false;
  }

  bool hasData = false;
  while (currChunk_->remainingSize() > 0 || moveToNextChunk()) {
    CELEBORN_CHECK_GE(currChunk_->remainingSize(), 4 * 4);
    // TODO: in java this is UNSAFE and PLATFORM related. hand-crafted here,
    // might not be safe.
    int mapId = currChunk_->readLE<int>();
    int attemptId = currChunk_->readLE<int>();
    int batchId = currChunk_->readLE<int>();
    int size = currChunk_->readLE<int>();
    CELEBORN_CHECK_GE(currChunk_->remainingSize(), size);
    CELEBORN_CHECK_LT(mapId, attempts_.size());

    // TODO: compression is not supported yet!

    if (attemptId == attempts_[mapId]) {
      auto& batchRecord = getBatchRecord(mapId);
      if (batchRecord.count(batchId) <= 0) {
        batchRecord.insert(batchId);
        currBatchSize_ = size;
        currBatchPos_ = 0;
        hasData = true;
        break;
      } else {
        currChunk_->skip(size);
      }
    }
  }

  return hasData;
}

bool CelebornInputStream::moveToNextChunk() {
  currChunk_.reset();

  if (currReader_->hasNext()) {
    currChunk_ = getNextChunk();
    return true;
  }
  if (currLocationIndex_ < locations_.size()) {
    moveToNextReader();
    return currReader_ != nullptr;
  }
  cleanupReader();
  return false;
}

std::unique_ptr<memory::ReadOnlyByteBuffer>
CelebornInputStream::getNextChunk() {
  // TODO: support the failure retrying, including excluding the failed
  // location, open a reader to read from the location's peer.
  auto chunk = currReader_->next();
  verifyChunk(chunk);
  return std::move(chunk);
}

void CelebornInputStream::verifyChunk(
    const std::unique_ptr<memory::ReadOnlyByteBuffer>& chunk) {
  auto data = chunk->clone();
  while (data->remainingSize() > 0) {
    CELEBORN_CHECK_GE(data->remainingSize(), 4 * 4);
    // TODO: in java this is UNSAFE and PLATFORM related. hand-crafted here,
    // might not be safe.
    int mapId = data->readLE<int>();
    int attemptId = data->readLE<int>();
    int batchId = data->readLE<int>();
    int size = data->readLE<int>();
    CELEBORN_CHECK_GE(data->remainingSize(), size);
    CELEBORN_CHECK_LT(mapId, attempts_.size());
    data->skip(size);
  }
}

void CelebornInputStream::moveToNextReader() {
  cleanupReader();
  auto location = nextReadableLocation();
  if (!location) {
    return;
  }
  currReader_ = createReaderWithRetry(*location);
  currLocationIndex_++;
  if (currReader_->hasNext()) {
    currChunk_ = getNextChunk();
    return;
  }
  moveToNextReader();
}

std::shared_ptr<PartitionReader> CelebornInputStream::createReaderWithRetry(
    const protocol::PartitionLocation& location) {
  // TODO: support retrying when createReader failed. Maybe switch to peer
  // location?
  return createReader(location);
}

std::shared_ptr<PartitionReader> CelebornInputStream::createReader(
    const protocol::PartitionLocation& location) {
  switch (location.storageInfo->type) {
    case protocol::StorageInfo::HDD:
    case protocol::StorageInfo::SSD: {
      // TODO: support localPartitionReader...
      return WorkerPartitionReader::create(
          conf_,
          shuffleKey_,
          location,
          startMapIndex_,
          endMapIndex_,
          clientFactory_.get());
    }
    case protocol::StorageInfo::HDFS:
    default:
      // TODO: support DfsPartitionReader...
      CELEBORN_FAIL(
          "unsupported protocol::StorageInfo type " +
          std::to_string(location.storageInfo->type));
  }
}

std::shared_ptr<const protocol::PartitionLocation>
CelebornInputStream::nextReadableLocation() {
  if (currLocationIndex_ >= locations_.size()) {
    return nullptr;
  }
  return locations_[currLocationIndex_];
  // TODO: support skipLocation functionality...
  // TODO: the currLocationIndex_ management is a mess. might be
  // managed all within this function?
}

std::unordered_set<int>& CelebornInputStream::getBatchRecord(int mapId) {
  batchRecords_.resize(mapId + 1);
  if (!batchRecords_[mapId]) {
    batchRecords_[mapId] = std::make_unique<std::unordered_set<int>>();
  }
  return *batchRecords_[mapId];
}

void CelebornInputStream::cleanupReader() {
  currReader_ = nullptr;
}
} // namespace client
} // namespace celeborn
