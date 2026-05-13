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
#include <string>
#include <vector>
#include "rust/cxx.h"
#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/conf/CelebornConf.h"

namespace celeborn_ffi {

struct ShuffleClientHandle {
  std::shared_ptr<celeborn::conf::CelebornConf> conf;
  std::shared_ptr<celeborn::client::ShuffleClientEndpoint> endpoint;
  std::shared_ptr<celeborn::client::ShuffleClientImpl> client;
  std::string app_id;
  std::string lifecycle_manager_host;
};

// Opaque handle wrapping a single CelebornInputStream so each reader can be
// dropped independently. The underlying stream holds a raw ShuffleClient*,
// so the owning ShuffleClientHandle must outlive every PartitionReaderHandle.
struct PartitionReaderHandle {
  std::unique_ptr<celeborn::client::CelebornInputStream> stream;
};

std::unique_ptr<ShuffleClientHandle> create_client(
    const std::string& app_id,
    int32_t push_buffer_max_size,
    const std::string& shuffle_compression_codec);

void setup_lifecycle_manager(
    ShuffleClientHandle& handle, const std::string& host, int32_t port);

void shutdown(ShuffleClientHandle& handle);

void push_data(ShuffleClientHandle& handle,
               int32_t shuffle_id, int32_t map_id, int32_t attempt_id,
               int32_t partition_id,
               rust::Slice<const uint8_t> data,
               int32_t num_mappers, int32_t num_partitions);

void mapper_end(ShuffleClientHandle& handle,
                int32_t shuffle_id, int32_t map_id,
                int32_t attempt_id, int32_t num_mappers);

void update_reducer_file_group(ShuffleClientHandle& handle, int32_t shuffle_id);

rust::Vec<uint8_t> read_partition_full(
    ShuffleClientHandle& handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index);

std::unique_ptr<PartitionReaderHandle> open_partition_reader(
    ShuffleClientHandle& handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index);

// Fills `out` with the next chunk of partition bytes.
// Returns the number of bytes written; 0 means EOF (std::io::Read semantics).
size_t read_partition_chunk(
    PartitionReaderHandle& reader, rust::Slice<uint8_t> out);

}  // namespace celeborn_ffi
