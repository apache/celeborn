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

#include "celeborn/ffi/CelebornFfi.h"

#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include "celeborn/client/ShuffleClient.h"
#include "celeborn/client/reader/CelebornInputStream.h"
#include "celeborn/conf/CelebornConf.h"

namespace {

struct ClientImpl {
  std::shared_ptr<celeborn::conf::CelebornConf> conf;
  std::shared_ptr<celeborn::client::ShuffleClientEndpoint> endpoint;
  std::shared_ptr<celeborn::client::ShuffleClientImpl> client;
  std::string app_id;
  std::string lifecycle_manager_host;
};

// One open partition stream. The stream references the owning
// ShuffleClient, so callers must close every reader before shutting down
// the client.
struct PartitionReaderImpl {
  std::unique_ptr<celeborn::client::CelebornInputStream> stream;
};

inline ClientImpl* as_impl(celeborn_ffi_handle* h) {
  return reinterpret_cast<ClientImpl*>(h);
}

inline PartitionReaderImpl* as_reader_impl(celeborn_ffi_partition_reader* r) {
  return reinterpret_cast<PartitionReaderImpl*>(r);
}

char* dup_message(const std::string& msg) {
  char* out = static_cast<char*>(std::malloc(msg.size() + 1));
  if (!out) {
    return nullptr;
  }
  std::memcpy(out, msg.data(), msg.size());
  out[msg.size()] = '\0';
  return out;
}

void set_error(char** err_out, const std::string& msg) {
  if (err_out) {
    *err_out = dup_message("celeborn-ffi: " + msg);
  }
}

template <typename Fn>
celeborn_ffi_status guarded(char** err_out, Fn&& fn) {
  try {
    fn();
    return CELEBORN_FFI_OK;
  } catch (const std::exception& e) {
    set_error(err_out, e.what());
    return CELEBORN_FFI_ERROR;
  } catch (...) {
    set_error(err_out, "unknown C++ exception");
    return CELEBORN_FFI_ERROR;
  }
}

// Reject null pointers at the FFI boundary so that misuse from non-Rust
// callers surfaces as CELEBORN_FFI_ERROR with a descriptive message rather
// than a SIGSEGV inside the C++ implementation.
#define CELEBORN_FFI_REQUIRE_NON_NULL(ptr, err_out)                     \
  do {                                                                  \
    if ((ptr) == nullptr) {                                             \
      set_error((err_out), "null pointer for argument '" #ptr "'");     \
      return CELEBORN_FFI_ERROR;                                        \
    }                                                                   \
  } while (0)

} // namespace

extern "C" {

void celeborn_ffi_free_error(char* err) {
  std::free(err);
}

void celeborn_ffi_free_buffer(uint8_t* data) {
  delete[] data;
}

celeborn_ffi_handle* celeborn_ffi_create_client(
    const char* app_id,
    size_t app_id_len,
    int32_t push_buffer_max_size,
    const char* codec,
    size_t codec_len,
    char** err_out) {
  if (app_id == nullptr && app_id_len > 0) {
    set_error(err_out, "null pointer for argument 'app_id'");
    return nullptr;
  }
  if (codec == nullptr && codec_len > 0) {
    set_error(err_out, "null pointer for argument 'codec'");
    return nullptr;
  }
  try {
    auto impl = std::make_unique<ClientImpl>();
    if (app_id_len > 0) {
      impl->app_id.assign(app_id, app_id_len);
    }
    impl->conf = std::make_shared<celeborn::conf::CelebornConf>();

    if (push_buffer_max_size > 0) {
      impl->conf->registerProperty(
          celeborn::conf::CelebornConf::kClientPushBufferMaxSize,
          std::to_string(push_buffer_max_size) + "b");
    }
    if (codec_len > 0) {
      impl->conf->registerProperty(
          celeborn::conf::CelebornConf::kShuffleCompressionCodec,
          std::string(codec, codec_len));
    }

    impl->endpoint =
        std::make_shared<celeborn::client::ShuffleClientEndpoint>(impl->conf);
    impl->client = celeborn::client::ShuffleClientImpl::create(
        impl->app_id, impl->conf, *(impl->endpoint));
    return reinterpret_cast<celeborn_ffi_handle*>(impl.release());
  } catch (const std::exception& e) {
    set_error(err_out, e.what());
    return nullptr;
  } catch (...) {
    set_error(err_out, "unknown C++ exception");
    return nullptr;
  }
}

celeborn_ffi_status celeborn_ffi_setup_lifecycle_manager(
    celeborn_ffi_handle* handle,
    const char* host,
    size_t host_len,
    int32_t port,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  if (host == nullptr && host_len > 0) {
    set_error(err_out, "null pointer for argument 'host'");
    return CELEBORN_FFI_ERROR;
  }
  return guarded(err_out, [&] {
    auto* impl = as_impl(handle);
    if (host_len > 0) {
      impl->lifecycle_manager_host.assign(host, host_len);
    } else {
      impl->lifecycle_manager_host.clear();
    }
    impl->client->setupLifecycleManagerRef(impl->lifecycle_manager_host, port);
  });
}

celeborn_ffi_status celeborn_ffi_shutdown(
    celeborn_ffi_handle* handle,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  return guarded(err_out, [&] { as_impl(handle)->client->shutdown(); });
}

celeborn_ffi_status celeborn_ffi_push_data(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t map_id,
    int32_t attempt_id,
    int32_t partition_id,
    const uint8_t* data,
    size_t data_len,
    int32_t num_mappers,
    int32_t num_partitions,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  if (data == nullptr && data_len > 0) {
    set_error(err_out, "null pointer for argument 'data'");
    return CELEBORN_FFI_ERROR;
  }
  return guarded(err_out, [&] {
    as_impl(handle)->client->pushData(
        shuffle_id,
        map_id,
        attempt_id,
        partition_id,
        data,
        0,
        static_cast<int>(data_len),
        num_mappers,
        num_partitions);
  });
}

celeborn_ffi_status celeborn_ffi_mapper_end(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t map_id,
    int32_t attempt_id,
    int32_t num_mappers,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  return guarded(err_out, [&] {
    as_impl(handle)->client->mapperEnd(
        shuffle_id, map_id, attempt_id, num_mappers);
  });
}

celeborn_ffi_status celeborn_ffi_update_reducer_file_group(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  return guarded(err_out, [&] {
    as_impl(handle)->client->updateReducerFileGroup(shuffle_id);
  });
}

celeborn_ffi_status celeborn_ffi_read_partition_full(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index,
    uint8_t** data_out,
    size_t* len_out,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  CELEBORN_FFI_REQUIRE_NON_NULL(data_out, err_out);
  CELEBORN_FFI_REQUIRE_NON_NULL(len_out, err_out);
  return guarded(err_out, [&] {
    auto stream = as_impl(handle)->client->readPartition(
        shuffle_id,
        partition_id,
        attempt_number,
        start_map_index,
        end_map_index);

    constexpr size_t kReadBufSize = 64 * 1024;
    std::vector<uint8_t> accumulated;
    accumulated.reserve(kReadBufSize);
    std::vector<uint8_t> buf(kReadBufSize);

    while (true) {
      int n = stream->read(buf.data(), 0, buf.size());
      if (n == -1) {
        break;
      }
      if (n <= 0) {
        throw std::runtime_error(
            "CelebornInputStream::read returned unexpected non-positive " +
            std::to_string(n));
      }
      accumulated.insert(accumulated.end(), buf.data(), buf.data() + n);
    }

    auto* out = new uint8_t[accumulated.size()];
    std::memcpy(out, accumulated.data(), accumulated.size());
    *data_out = out;
    *len_out = accumulated.size();
  });
}

celeborn_ffi_status celeborn_ffi_open_partition_reader(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index,
    celeborn_ffi_partition_reader** reader_out,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(handle, err_out);
  CELEBORN_FFI_REQUIRE_NON_NULL(reader_out, err_out);
  return guarded(err_out, [&] {
    auto reader = std::make_unique<PartitionReaderImpl>();
    reader->stream = as_impl(handle)->client->readPartition(
        shuffle_id,
        partition_id,
        attempt_number,
        start_map_index,
        end_map_index);
    *reader_out =
        reinterpret_cast<celeborn_ffi_partition_reader*>(reader.release());
  });
}

celeborn_ffi_status celeborn_ffi_read_partition_chunk(
    celeborn_ffi_partition_reader* reader,
    uint8_t* buf,
    size_t buf_len,
    size_t* bytes_read,
    char** err_out) {
  CELEBORN_FFI_REQUIRE_NON_NULL(reader, err_out);
  CELEBORN_FFI_REQUIRE_NON_NULL(bytes_read, err_out);
  if (buf == nullptr && buf_len > 0) {
    set_error(err_out, "null pointer for argument 'buf'");
    return CELEBORN_FFI_ERROR;
  }
  return guarded(err_out, [&] {
    if (buf_len == 0) {
      *bytes_read = 0;
      return;
    }
    int n = as_reader_impl(reader)->stream->read(buf, 0, buf_len);
    if (n == -1) {
      // EOF — surface as 0 bytes read to match std::io::Read semantics.
      *bytes_read = 0;
      return;
    }
    if (n < 0) {
      throw std::runtime_error(
          "CelebornInputStream::read returned unexpected negative " +
          std::to_string(n));
    }
    *bytes_read = static_cast<size_t>(n);
  });
}

void celeborn_ffi_close_partition_reader(
    celeborn_ffi_partition_reader* reader) {
  delete as_reader_impl(reader);
}

} // extern "C"
