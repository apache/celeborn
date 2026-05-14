#pragma once
#include <memory>
#include <string>
#include <vector>
#include "rust/cxx.h"
#include "celeborn/client/ShuffleClient.h"
#include "celeborn/conf/CelebornConf.h"

namespace celeborn_ffi {

struct ShuffleClientHandle {
  std::shared_ptr<celeborn::conf::CelebornConf> conf;
  std::shared_ptr<celeborn::client::ShuffleClientEndpoint> endpoint;
  std::shared_ptr<celeborn::client::ShuffleClientImpl> client;
  std::string app_id;
  std::string lifecycle_manager_host;
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

}  // namespace celeborn_ffi
