// Pure C ABI shim around the celeborn C++ shuffle client. Lives inside
// libceleborn_client.{so,dylib} so downstream language bindings (Rust /
// Python via ctypes / etc.) only need to link the single shared library
// and never pull folly / protobuf / glog / abseil headers themselves.
#ifndef CELEBORN_FFI_WRAPPER_H
#define CELEBORN_FFI_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle. Allocated by celeborn_ffi_create_client; intentionally
// never freed (folly's IOThreadPoolExecutor::join() races with
// TransportClient teardown — calling the C++ destructor produces SIGSEGV).
typedef struct celeborn_ffi_handle celeborn_ffi_handle;

// Opaque handle for a single open partition reader. Allocated by
// celeborn_ffi_open_partition_reader; released by
// celeborn_ffi_close_partition_reader. Holds a CelebornInputStream that
// references the owning client, so it must be closed before its
// celeborn_ffi_handle is shut down.
typedef struct celeborn_ffi_partition_reader celeborn_ffi_partition_reader;

// Status codes. Every fallible function returns CELEBORN_FFI_OK on success
// and writes a heap-allocated, NUL-terminated message into *err_out on
// failure (caller must release with celeborn_ffi_free_error).
typedef int32_t celeborn_ffi_status;
#define CELEBORN_FFI_OK 0
#define CELEBORN_FFI_ERROR 1

void celeborn_ffi_free_error(char* err);

// Releases a buffer returned via celeborn_ffi_read_partition_full.
void celeborn_ffi_free_buffer(uint8_t* data);

// Returns NULL on failure; in that case *err_out is set.
celeborn_ffi_handle* celeborn_ffi_create_client(
    const char* app_id,
    size_t app_id_len,
    int32_t push_buffer_max_size,
    const char* codec,
    size_t codec_len,
    char** err_out);

celeborn_ffi_status celeborn_ffi_setup_lifecycle_manager(
    celeborn_ffi_handle* handle,
    const char* host,
    size_t host_len,
    int32_t port,
    char** err_out);

celeborn_ffi_status celeborn_ffi_shutdown(
    celeborn_ffi_handle* handle,
    char** err_out);

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
    char** err_out);

celeborn_ffi_status celeborn_ffi_mapper_end(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t map_id,
    int32_t attempt_id,
    int32_t num_mappers,
    char** err_out);

celeborn_ffi_status celeborn_ffi_update_reducer_file_group(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    char** err_out);

// On success, *data_out is a heap buffer (free with celeborn_ffi_free_buffer)
// and *len_out is its byte length.
celeborn_ffi_status celeborn_ffi_read_partition_full(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index,
    uint8_t** data_out,
    size_t* len_out,
    char** err_out);

// On success, *reader_out is a heap-allocated reader that must eventually
// be released with celeborn_ffi_close_partition_reader.
celeborn_ffi_status celeborn_ffi_open_partition_reader(
    celeborn_ffi_handle* handle,
    int32_t shuffle_id,
    int32_t partition_id,
    int32_t attempt_number,
    int32_t start_map_index,
    int32_t end_map_index,
    celeborn_ffi_partition_reader** reader_out,
    char** err_out);

// Reads up to buf_len bytes into buf. *bytes_read is set to the number of
// bytes actually read; 0 indicates EOF (matching Rust std::io::Read).
celeborn_ffi_status celeborn_ffi_read_partition_chunk(
    celeborn_ffi_partition_reader* reader,
    uint8_t* buf,
    size_t buf_len,
    size_t* bytes_read,
    char** err_out);

void celeborn_ffi_close_partition_reader(celeborn_ffi_partition_reader* reader);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // CELEBORN_FFI_WRAPPER_H
