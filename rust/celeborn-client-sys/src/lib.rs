//! Raw C ABI bindings for `libceleborn_client.{so,dylib}`.
//!
//! All FFI is plain `extern "C"` — no `cxx`, no C++ headers, no template
//! instantiations leak across the language boundary. The dylib is the sole
//! external link dependency; folly / protobuf / glog / abseil all stay
//! hidden inside it.

#![allow(non_camel_case_types, non_upper_case_globals)]

use std::os::raw::c_char;

/// Opaque handle. Returned by [`celeborn_ffi_create_client`].
///
/// The handle is *intentionally never freed* — folly's
/// `IOThreadPoolExecutor::join()` races with `TransportClient` teardown,
/// so calling the C++ destructor produces SIGSEGV. Process-lifetime use.
#[repr(C)]
pub struct celeborn_ffi_handle {
    _opaque: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

/// Opaque handle to a single open partition reader. Returned by
/// [`celeborn_ffi_open_partition_reader`]; released with
/// [`celeborn_ffi_close_partition_reader`]. Holds a raw pointer into the
/// owning client, so it must not outlive its [`celeborn_ffi_handle`].
#[repr(C)]
pub struct celeborn_ffi_partition_reader {
    _opaque: [u8; 0],
    _marker: core::marker::PhantomData<(*mut u8, core::marker::PhantomPinned)>,
}

pub type celeborn_ffi_status = i32;
pub const CELEBORN_FFI_OK: celeborn_ffi_status = 0;
pub const CELEBORN_FFI_ERROR: celeborn_ffi_status = 1;

extern "C" {
    pub fn celeborn_ffi_free_error(err: *mut c_char);
    pub fn celeborn_ffi_free_buffer(data: *mut u8);

    pub fn celeborn_ffi_create_client(
        app_id: *const c_char,
        app_id_len: usize,
        push_buffer_max_size: i32,
        codec: *const c_char,
        codec_len: usize,
        err_out: *mut *mut c_char,
    ) -> *mut celeborn_ffi_handle;

    pub fn celeborn_ffi_setup_lifecycle_manager(
        handle: *mut celeborn_ffi_handle,
        host: *const c_char,
        host_len: usize,
        port: i32,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_shutdown(
        handle: *mut celeborn_ffi_handle,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_push_data(
        handle: *mut celeborn_ffi_handle,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: *const u8,
        data_len: usize,
        num_mappers: i32,
        num_partitions: i32,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_mapper_end(
        handle: *mut celeborn_ffi_handle,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_update_reducer_file_group(
        handle: *mut celeborn_ffi_handle,
        shuffle_id: i32,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_read_partition_full(
        handle: *mut celeborn_ffi_handle,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
        data_out: *mut *mut u8,
        len_out: *mut usize,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_open_partition_reader(
        handle: *mut celeborn_ffi_handle,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
        reader_out: *mut *mut celeborn_ffi_partition_reader,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    /// Reads up to `buf_len` bytes into `buf`. Writes the number of bytes
    /// actually read into `*bytes_read`; `0` indicates EOF (`std::io::Read`
    /// semantics).
    pub fn celeborn_ffi_read_partition_chunk(
        reader: *mut celeborn_ffi_partition_reader,
        buf: *mut u8,
        buf_len: usize,
        bytes_read: *mut usize,
        err_out: *mut *mut c_char,
    ) -> celeborn_ffi_status;

    pub fn celeborn_ffi_close_partition_reader(reader: *mut celeborn_ffi_partition_reader);
}

/// Take ownership of a C error string and convert it to an owned `String`.
///
/// Releases the heap allocation via [`celeborn_ffi_free_error`].
///
/// # Safety
/// `err` must be either null or a valid pointer returned by a
/// `celeborn_ffi_*` function via its `err_out` parameter. After calling
/// this, the pointer is dangling.
pub unsafe fn take_error(err: *mut c_char) -> Option<String> {
    if err.is_null() {
        return None;
    }
    let msg = std::ffi::CStr::from_ptr(err).to_string_lossy().into_owned();
    celeborn_ffi_free_error(err);
    Some(msg)
}
