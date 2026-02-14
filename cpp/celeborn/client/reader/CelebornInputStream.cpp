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
#include <lz4.h>
#include "celeborn/client/compress/Decompressor.h"

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
    int endMapIndex,
    bool needCompression,
    const std::shared_ptr<FetchExcludedWorkers>& fetchExcludedWorkers)
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
      currBatchSize_(0),
      shouldDecompress_(
          conf_->shuffleCompressionCodec() !=
              protocol::CompressionCodec::NONE &&
          needCompression),
      fetchChunkRetryCnt_(0),
      fetchChunkMaxRetry_(
          conf_->clientPushReplicateEnabled()
              ? conf_->clientFetchMaxRetriesForEachReplica() * 2
              : conf_->clientFetchMaxRetriesForEachReplica()),
      retryWait_(conf_->networkIoRetryWait()),
      fetchExcludedWorkers_(fetchExcludedWorkers),
      fetchExcludedWorkerExpireTimeoutMs_(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              conf_->clientFetchExcludedWorkerExpireTimeout())
              .count()),
      readSkewPartitionWithoutMapRange_(
          conf_->clientAdaptiveOptimizeSkewedPartitionReadEnabled() &&
          startMapIndex > endMapIndex) {
  if (shouldDecompress_) {
    decompressor_ = compress::Decompressor::createDecompressor(
        conf_->shuffleCompressionCodec());
  }
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
    CELEBORN_CHECK_GE(decompressedChunk_->remainingSize(), toReadBytes);
    auto size = decompressedChunk_->readToBuffer(&buf[readBytes], toReadBytes);
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

    if (shouldDecompress_) {
      if (size > compressedBuf_.size()) {
        compressedBuf_.resize(size);
      }
      currChunk_->readToBuffer(compressedBuf_.data(), size);
    }

    if (attemptId == attempts_[mapId]) {
      auto& batchRecord = getBatchRecord(mapId);
      if (batchRecord.count(batchId) <= 0) {
        batchRecord.insert(batchId);
        if (shouldDecompress_) {
          const auto originalLength =
              decompressor_->getOriginalLen(compressedBuf_.data());
          std::unique_ptr<folly::IOBuf> decompressedBuf_ =
              folly::IOBuf::createCombined(originalLength);
          decompressedBuf_->append(originalLength);
          currBatchSize_ = decompressor_->decompress(
              compressedBuf_.data(), decompressedBuf_->writableData(), 0);
          decompressedChunk_ = memory::ByteBuffer::createReadOnly(
              std::move(decompressedBuf_), false);
        } else {
          currBatchSize_ = size;
          decompressedChunk_ = currChunk_->readToReadOnlyBuffer(size);
        }
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
  fetchChunkRetryCnt_ = 0;
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
  const protocol::PartitionLocation* currentLocation = &location;
  std::exception_ptr lastException;

  while (fetchChunkRetryCnt_ < fetchChunkMaxRetry_) {
    try {
      VLOG(1) << "Create reader for location " << currentLocation->host << ":"
              << currentLocation->fetchPort;
      if (isExcluded(*currentLocation)) {
        throw std::runtime_error(
            "Fetch data from excluded worker! " +
            currentLocation->hostAndFetchPort());
      }
      auto reader = createReader(*currentLocation);
      return reader;
    } catch (const std::exception& e) {
      lastException = std::current_exception();
      excludeFailedFetchLocation(currentLocation->hostAndFetchPort(), e);
      fetchChunkRetryCnt_++;

      if (currentLocation->hasPeer() && !readSkewPartitionWithoutMapRange_) {
        if (fetchChunkRetryCnt_ % 2 == 0) {
          std::this_thread::sleep_for(
              std::chrono::milliseconds(retryWait_.count()));
        }
        LOG(WARNING) << "CreatePartitionReader failed " << fetchChunkRetryCnt_
                     << "/" << fetchChunkMaxRetry_ << " times for location "
                     << currentLocation->hostAndFetchPort()
                     << ", change to peer. Error: " << e.what();
        // TODO: When stream handlers are supported, send BUFFER_STREAM_END
        // to close the active stream before switching to peer, matching the
        // Java CelebornInputStream behavior. Currently, the C++ client does
        // not have pre-opened stream handlers, so stream cleanup is handled
        // by WorkerPartitionReader's destructor.
        currentLocation = currentLocation->getPeer();
      } else {
        LOG(WARNING) << "CreatePartitionReader failed " << fetchChunkRetryCnt_
                     << "/" << fetchChunkMaxRetry_ << " times for location "
                     << currentLocation->hostAndFetchPort()
                     << ", retry the same location. Error: " << e.what();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(retryWait_.count()));
      }
    }
  }

  // Max retries exceeded, rethrow the last exception wrapped with context
  throw utils::CelebornRuntimeError(
      lastException,
      "createPartitionReader failed after " +
          std::to_string(fetchChunkRetryCnt_) + " retries for location " +
          location.hostAndFetchPort(),
      false);
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

bool CelebornInputStream::isExcluded(
    const protocol::PartitionLocation& location) {
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now().time_since_epoch())
                 .count();
  auto timestamp = fetchExcludedWorkers_->get(location.hostAndFetchPort());
  if (!timestamp.has_value()) {
    return false;
  }
  if (now - timestamp.value() > fetchExcludedWorkerExpireTimeoutMs_) {
    fetchExcludedWorkers_->erase(location.hostAndFetchPort());
    return false;
  }
  if (location.hasPeer()) {
    auto peerTimestamp =
        fetchExcludedWorkers_->get(location.getPeer()->hostAndFetchPort());
    // To avoid both replicate locations being excluded, if peer was added to
    // excluded list earlier, change to try peer.
    if (!peerTimestamp.has_value() ||
        peerTimestamp.value() < timestamp.value()) {
      return true;
    }
    return false;
  }
  return true;
}

void CelebornInputStream::excludeFailedFetchLocation(
    const std::string& hostAndFetchPort,
    const std::exception& e) {
  if (conf_->clientPushReplicateEnabled() &&
      conf_->clientFetchExcludeWorkerOnFailureEnabled() &&
      utils::isCriticalCauseForFetch(e)) {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
                   .count();
    fetchExcludedWorkers_->set(hostAndFetchPort, now);
  }
}

void CelebornInputStream::cleanupReader() {
  currReader_ = nullptr;
}
} // namespace client
} // namespace celeborn
