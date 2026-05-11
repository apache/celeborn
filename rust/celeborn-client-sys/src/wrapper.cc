#include "wrapper.h"
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

// Access rust::Vec<T>::set_len which is private in cxx 1.x.
// Uses the well-known C++ explicit-instantiation access technique (ISO 14882 §14.7.2)
// to obtain a pointer-to-member for the private method, then invokes it.
// This avoids per-byte push_back overhead when bulk-copying data into rust::Vec.
namespace detail {
using SetLenFn = void (rust::Vec<uint8_t>::*)(size_t);
template <SetLenFn Fn> struct VecSetLenAccessor {
  friend void vec_unsafe_set_len(rust::Vec<uint8_t>& v, size_t len) {
    (v.*Fn)(len);
  }
};
// The explicit instantiation is granted access to all members per the standard.
template struct VecSetLenAccessor<&rust::Vec<uint8_t>::set_len>;
void vec_unsafe_set_len(rust::Vec<uint8_t>& v, size_t len);
} // namespace detail

namespace celeborn_ffi {

std::unique_ptr<ShuffleClientHandle> create_client(
    const std::string& app_id,
    int32_t push_buffer_max_size,
    const std::string& shuffle_compression_codec) {
  try {
    auto handle = std::make_unique<ShuffleClientHandle>();
    handle->app_id = app_id;
    handle->conf = std::make_shared<celeborn::conf::CelebornConf>();

    if (push_buffer_max_size > 0) {
      handle->conf->registerProperty(
          celeborn::conf::CelebornConf::kClientPushBufferMaxSize,
          std::to_string(push_buffer_max_size) + "b");
    }

    if (!shuffle_compression_codec.empty()) {
      handle->conf->registerProperty(
          celeborn::conf::CelebornConf::kShuffleCompressionCodec,
          shuffle_compression_codec);
    }

    handle->endpoint = std::make_shared<celeborn::client::ShuffleClientEndpoint>(
        handle->conf);
    handle->client = celeborn::client::ShuffleClientImpl::create(
        app_id, handle->conf, *(handle->endpoint));

    return handle;
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

void setup_lifecycle_manager(
    ShuffleClientHandle& handle, const std::string& host, int32_t port) {
  try {
    handle.lifecycle_manager_host = host;
    handle.client->setupLifecycleManagerRef(handle.lifecycle_manager_host, port);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

void shutdown(ShuffleClientHandle& handle) {
  try {
    handle.client->shutdown();
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

void push_data(ShuffleClientHandle& handle,
               int32_t shuffle_id, int32_t map_id, int32_t attempt_id,
               int32_t partition_id,
               rust::Slice<const uint8_t> data,
               int32_t num_mappers, int32_t num_partitions) {
  try {
    handle.client->pushData(
        shuffle_id, map_id, attempt_id, partition_id,
        data.data(), 0,
        static_cast<int>(data.size()),
        num_mappers, num_partitions);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

void mapper_end(ShuffleClientHandle& handle,
                int32_t shuffle_id, int32_t map_id,
                int32_t attempt_id, int32_t num_mappers) {
  try {
    handle.client->mapperEnd(shuffle_id, map_id, attempt_id, num_mappers);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

void update_reducer_file_group(ShuffleClientHandle& handle, int32_t shuffle_id) {
  try {
    handle.client->updateReducerFileGroup(shuffle_id);
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

rust::Vec<uint8_t> read_partition_full(
    ShuffleClientHandle& handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index) {
  try {
    auto stream = handle.client->readPartition(
        shuffle_id, partition_id, attempt_number, start_map_index, end_map_index);

    // Accumulate into std::vector (memcpy-based insert) to avoid per-byte
    // push_back overhead on rust::Vec during the read loop, then bulk-copy
    // into rust::Vec via reserve_total + memcpy + set_len.
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
            "celeborn-ffi: CelebornInputStream::read returned unexpected non-positive " +
            std::to_string(n));
      }
      accumulated.insert(accumulated.end(), buf.data(), buf.data() + n);
    }

    rust::Vec<uint8_t> out;
    out.reserve(accumulated.size());
    // SAFETY: reserve() allocated at least accumulated.size() bytes.
    // memcpy writes into that region, then set_len makes them visible.
    std::memcpy(out.data(), accumulated.data(), accumulated.size());
    detail::vec_unsafe_set_len(out, accumulated.size());
    return out;
  } catch (const std::exception& e) {
    throw std::runtime_error(std::string("celeborn-ffi: ") + e.what());
  } catch (...) {
    throw std::runtime_error("celeborn-ffi: unknown C++ exception");
  }
}

}  // namespace celeborn_ffi
