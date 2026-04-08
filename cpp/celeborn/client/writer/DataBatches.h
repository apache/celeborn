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

#include <memory>
#include <mutex>
#include <vector>

#include "celeborn/memory/ByteBuffer.h"
#include "celeborn/protocol/PartitionLocation.h"

namespace celeborn {
namespace client {

struct DataBatch {
  std::shared_ptr<const protocol::PartitionLocation> loc;
  int batchId;
  std::unique_ptr<memory::ReadOnlyByteBuffer> body;

  DataBatch(
      std::shared_ptr<const protocol::PartitionLocation> loc,
      int batchId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body)
      : loc(std::move(loc)), batchId(batchId), body(std::move(body)) {}

  DataBatch(DataBatch&& other) noexcept = default;
  DataBatch& operator=(DataBatch&& other) noexcept = default;
};

class DataBatches {
 public:
  void addDataBatch(
      std::shared_ptr<const protocol::PartitionLocation> loc,
      int batchId,
      std::unique_ptr<memory::ReadOnlyByteBuffer> body);

  int getTotalSize() const;

  std::vector<DataBatch> requireBatches();

  std::vector<DataBatch> requireBatches(int requestSize);

 private:
  mutable std::mutex mutex_;
  int totalSize_{0};
  std::vector<DataBatch> batches_;
};

} // namespace client
} // namespace celeborn
