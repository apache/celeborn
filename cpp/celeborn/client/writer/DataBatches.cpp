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

namespace celeborn {
namespace client {

void DataBatches::addDataBatch(
    std::shared_ptr<const protocol::PartitionLocation> loc,
    int batchId,
    std::unique_ptr<memory::ReadOnlyByteBuffer> body) {
  std::lock_guard<std::mutex> lock(mutex_);
  int bodySize = static_cast<int>(body->size());
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
  while (currentSize < requestSize && !batches_.empty()) {
    int bodySize = static_cast<int>(batches_.front().body->size());
    result.push_back(std::move(batches_.front()));
    batches_.erase(batches_.begin());
    currentSize += bodySize;
    totalSize_ -= bodySize;
  }
  return result;
}

} // namespace client
} // namespace celeborn
