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

#include "celeborn/client/writer/DataBatches.h"
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {

void DataBatches::addDataBatch(
    std::shared_ptr<const protocol::PartitionLocation> loc,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> body) {
  std::lock_guard<std::mutex> lock(mutex_);
  CELEBORN_CHECK_LE(
      body->size(),
      static_cast<size_t>(INT_MAX),
      "Data batch body size {} exceeds INT_MAX",
      body->size());
  int bodySize = static_cast<int>(body->size());
  CELEBORN_CHECK_LE(
      bodySize,
      INT_MAX - totalSize_,
      "Adding batch of size {} would overflow totalSize (current: {})",
      bodySize,
      totalSize_);
  batches_.emplace_back(std::move(loc), batchId, std::move(body));
  totalSize_ += bodySize;
}

int DataBatches::getTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return totalSize_;
}

std::vector<DataBatch> DataBatches::requireBatches() {
  std::lock_guard<std::mutex> lock(mutex_);
  totalSize_ = 0;
  return std::move(batches_);
}

std::vector<DataBatch> DataBatches::requireBatches(int requestSize) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (requestSize >= totalSize_) {
    totalSize_ = 0;
    return std::move(batches_);
  }
  std::vector<DataBatch> result;
  int currentSize = 0;
  size_t count = 0;
  while (count < batches_.size() && currentSize < requestSize) {
    int bodySize = static_cast<int>(batches_[count].body->size());
    currentSize += bodySize;
    totalSize_ -= bodySize;
    ++count;
  }
  result.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    result.push_back(std::move(batches_[i]));
  }
  batches_.erase(batches_.begin(), batches_.begin() + count);
  return result;
}

} // namespace client
} // namespace celeborn
