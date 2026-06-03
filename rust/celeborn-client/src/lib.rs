//! Rust-friendly wrapper around `celeborn-client-sys` (raw C ABI bindings
//! to `libceleborn_client.{so,dylib}`).

use std::marker::PhantomData;
use std::os::raw::c_char;
use std::ptr;

use celeborn_client_sys as sys;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("celeborn ffi error: {0}")]
    Ffi(String),
    #[error("invalid argument: {0}")]
    InvalidArg(&'static str),
    #[error("celeborn ffi returned a null handle")]
    NullHandle,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Build an [`Error::Ffi`] from a C error pointer (consumes the heap
/// allocation). Returns a generic message if the pointer is null.
unsafe fn ffi_error(err: *mut c_char) -> Error {
    Error::Ffi(
        sys::take_error(err)
            .unwrap_or_else(|| "celeborn ffi returned no error message".to_string()),
    )
}

/// Configuration for connecting to a Celeborn LifecycleManager.
pub struct Config {
    pub app_id: String,
    /// Max push buffer size in bytes. 0 means use cpp default (64kB).
    pub push_buffer_max_size: i32,
    /// Compression codec: "NONE", "LZ4", or "ZSTD".
    pub shuffle_compression_codec: String,
}

impl Config {
    pub fn new(app_id: String) -> Self {
        Self {
            app_id,
            push_buffer_max_size: 0,
            shuffle_compression_codec: "NONE".to_string(),
        }
    }
}

/// A Rust-friendly Celeborn shuffle client backed by the C++ implementation.
///
/// The handle is intentionally leaked on `Drop` (after a best-effort
/// shutdown) to work around a folly `EventBase` use-after-free that
/// triggers when `TransportClient` is destroyed concurrently with
/// `IOThreadPoolExecutor::join()`. Use `shutdown()` for explicit teardown.
pub struct ShuffleClient {
    handle: *mut sys::celeborn_ffi_handle,
}

// SAFETY: The underlying C++ `ShuffleClientImpl` owns its own thread pool and
// synchronizes all shared state internally:
//   - All shared maps (partition locations, push states, reducer file groups,
//     mapper-end sets) are `folly`-backed concurrent maps guarded by
//     `Synchronized`.
//   - `registerShuffle` is serialized per-shuffleId with a dedicated mutex plus
//     double-checked lookup.
//   - A fresh compressor instance is created per `pushData` call, so no
//     non-thread-safe compressor is shared across concurrent operations.
// `pushData` and `readPartition` are therefore safe to invoke concurrently from
// multiple threads on the same client, which is why every method below takes
// `&self`. The handle is an opaque raw pointer with no Rust-level aliasing
// concerns, so the client is both `Send` and `Sync` and can be shared via
// `Arc<ShuffleClient>` for parallel push/read.
//
// Note: the `PartitionReader` returned by `open_partition` wraps a single
// `CelebornInputStream` that holds mutable read state and is NOT thread-safe;
// it keeps `&mut self` on its `Read` impl so a single reader cannot be read
// concurrently from multiple threads. Opening multiple independent readers in
// parallel is fine.
unsafe impl Send for ShuffleClient {}
unsafe impl Sync for ShuffleClient {}

impl ShuffleClient {
    /// Connect to a running LifecycleManager at `lm_host:lm_port`.
    pub fn connect(config: Config, lm_host: &str, lm_port: i32) -> Result<Self> {
        if config.app_id.is_empty() {
            return Err(Error::InvalidArg("app_id is empty"));
        }
        if lm_port <= 0 {
            return Err(Error::InvalidArg("lm_port must be > 0"));
        }
        let valid_codecs = ["NONE", "LZ4", "ZSTD"];
        if !valid_codecs.contains(&config.shuffle_compression_codec.as_str()) {
            return Err(Error::InvalidArg(
                "shuffle_compression_codec must be NONE, LZ4, or ZSTD",
            ));
        }

        let mut err: *mut c_char = ptr::null_mut();
        let handle = unsafe {
            sys::celeborn_ffi_create_client(
                config.app_id.as_ptr() as *const c_char,
                config.app_id.len(),
                config.push_buffer_max_size,
                config.shuffle_compression_codec.as_ptr() as *const c_char,
                config.shuffle_compression_codec.len(),
                &mut err,
            )
        };
        if handle.is_null() {
            return Err(unsafe { ffi_error(err) });
        }

        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_setup_lifecycle_manager(
                handle,
                lm_host.as_ptr() as *const c_char,
                lm_host.len(),
                lm_port,
                &mut err,
            )
        };
        if status != sys::CELEBORN_FFI_OK {
            // Intentional leak of `handle`: do not call any destructor that
            // would tear down the folly EventBase state.
            return Err(unsafe { ffi_error(err) });
        }

        Ok(Self { handle })
    }

    /// Push data for a specific partition.
    ///
    /// Takes `&self` so the client can be shared via `Arc<ShuffleClient>` and
    /// pushed to concurrently from multiple threads; the underlying C++ client
    /// synchronizes internally.
    pub fn push_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<()> {
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_push_data(
                self.handle,
                shuffle_id,
                map_id,
                attempt_id,
                partition_id,
                data.as_ptr(),
                data.len(),
                num_mappers,
                num_partitions,
                &mut err,
            )
        };
        if status == sys::CELEBORN_FFI_OK {
            Ok(())
        } else {
            Err(unsafe { ffi_error(err) })
        }
    }

    /// Signal that a mapper has finished writing all its partitions.
    pub fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<()> {
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_mapper_end(
                self.handle,
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                &mut err,
            )
        };
        if status == sys::CELEBORN_FFI_OK {
            Ok(())
        } else {
            Err(unsafe { ffi_error(err) })
        }
    }

    /// Update reducer file group metadata for a given shuffle.
    pub fn update_reducer_file_group(&self, shuffle_id: i32) -> Result<()> {
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_update_reducer_file_group(self.handle, shuffle_id, &mut err)
        };
        if status == sys::CELEBORN_FFI_OK {
            Ok(())
        } else {
            Err(unsafe { ffi_error(err) })
        }
    }

    /// Read all data for a partition with full control over parameters.
    ///
    /// Takes `&self` so multiple partitions can be read in parallel from
    /// different threads sharing one `Arc<ShuffleClient>`.
    pub fn read_partition(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
    ) -> Result<Vec<u8>> {
        let mut data_out: *mut u8 = ptr::null_mut();
        let mut len_out: usize = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_read_partition_full(
                self.handle,
                shuffle_id,
                partition_id,
                attempt_number,
                start_map_index,
                end_map_index,
                &mut data_out,
                &mut len_out,
                &mut err,
            )
        };
        if status != sys::CELEBORN_FFI_OK {
            return Err(unsafe { ffi_error(err) });
        }
        // Copy the C-allocated buffer into a Rust-owned Vec, then release
        // the C buffer with the matching deallocator.
        let out = unsafe { std::slice::from_raw_parts(data_out, len_out).to_vec() };
        unsafe { sys::celeborn_ffi_free_buffer(data_out) };
        Ok(out)
    }

    /// Convenience: read all map outputs for a partition.
    #[inline]
    pub fn read_partition_all(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        num_mappers: i32,
    ) -> Result<Vec<u8>> {
        self.read_partition(shuffle_id, partition_id, 0, 0, num_mappers)
    }

    /// Open a streaming reader for a partition. The returned [`PartitionReader`]
    /// implements [`std::io::Read`], so the caller can wrap it in a
    /// [`std::io::BufReader`] and process bytes without materializing the
    /// whole partition in memory.
    ///
    /// The reader borrows `&self`, so multiple partitions can be opened and
    /// read in parallel from threads sharing one `Arc<ShuffleClient>`. Each
    /// individual [`PartitionReader`] is single-threaded (its `Read` impl takes
    /// `&mut self`), so a given reader must not be read from multiple threads
    /// at once.
    pub fn open_partition(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
    ) -> Result<PartitionReader<'_>> {
        let mut reader_out: *mut sys::celeborn_ffi_partition_reader = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_open_partition_reader(
                self.handle,
                shuffle_id,
                partition_id,
                attempt_number,
                start_map_index,
                end_map_index,
                &mut reader_out,
                &mut err,
            )
        };
        if status != sys::CELEBORN_FFI_OK {
            return Err(unsafe { ffi_error(err) });
        }
        if reader_out.is_null() {
            return Err(Error::NullHandle);
        }
        Ok(PartitionReader {
            inner: reader_out,
            _client: PhantomData,
        })
    }

    /// Convenience: stream all map outputs for a partition.
    #[inline]
    pub fn open_partition_all(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        num_mappers: i32,
    ) -> Result<PartitionReader<'_>> {
        self.open_partition(shuffle_id, partition_id, 0, 0, num_mappers)
    }

    /// Explicitly shut down the client. Preferred over relying on Drop.
    ///
    /// After calling `celeborn_ffi_shutdown`, the underlying C++ handle is
    /// intentionally leaked (see the type-level docs).
    pub fn shutdown(mut self) -> Result<()> {
        let mut err: *mut c_char = ptr::null_mut();
        let status =
            unsafe { sys::celeborn_ffi_shutdown(self.handle, &mut err) };
        // Null the handle so Drop does not call shutdown a second time.
        self.handle = ptr::null_mut();
        if status == sys::CELEBORN_FFI_OK {
            Ok(())
        } else {
            Err(unsafe { ffi_error(err) })
        }
    }
}

impl Drop for ShuffleClient {
    fn drop(&mut self) {
        if self.handle.is_null() {
            return;
        }
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe { sys::celeborn_ffi_shutdown(self.handle, &mut err) };
        if status != sys::CELEBORN_FFI_OK {
            let msg = unsafe { sys::take_error(err) }
                .unwrap_or_else(|| "no error message".to_string());
            log::error!(
                "celeborn_ffi_shutdown failed during Drop: {msg}; \
                 caller should explicitly call ShuffleClient::shutdown() before drop"
            );
        }
        // Intentional leak — no celeborn_ffi_destroy call. See type docs.
        self.handle = ptr::null_mut();
    }
}

/// Streaming reader for a single partition, returned by
/// [`ShuffleClient::open_partition`]. Implements [`std::io::Read`]; wrap in
/// a [`std::io::BufReader`] to avoid one FFI call per byte.
pub struct PartitionReader<'client> {
    inner: *mut sys::celeborn_ffi_partition_reader,
    _client: PhantomData<&'client ShuffleClient>,
}

// SAFETY: the underlying C++ `CelebornInputStream` is owned exclusively by this
// reader and only touched through `&mut self` (see the `Read` impl), so it can
// be moved to another thread. It is deliberately NOT `Sync`: a single reader
// holds mutable read state and must not be read from multiple threads at once.
unsafe impl<'client> Send for PartitionReader<'client> {}

impl<'client> std::io::Read for PartitionReader<'client> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read: usize = 0;
        let mut err: *mut c_char = ptr::null_mut();
        let status = unsafe {
            sys::celeborn_ffi_read_partition_chunk(
                self.inner,
                buf.as_mut_ptr(),
                buf.len(),
                &mut bytes_read,
                &mut err,
            )
        };
        if status != sys::CELEBORN_FFI_OK {
            let msg = unsafe { sys::take_error(err) }
                .unwrap_or_else(|| "no error message".to_string());
            return Err(std::io::Error::other(msg));
        }
        Ok(bytes_read)
    }
}

impl<'client> Drop for PartitionReader<'client> {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { sys::celeborn_ffi_close_partition_reader(self.inner) };
            self.inner = ptr::null_mut();
        }
    }
}
