//! Rust-friendly wrapper around `celeborn-client-sys`.

use celeborn_client_sys::ffi;
use cxx::UniquePtr;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("celeborn ffi error: {0}")]
    Ffi(#[from] cxx::Exception),
    #[error("invalid argument: {0}")]
    InvalidArg(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

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
pub struct ShuffleClient {
    inner: UniquePtr<ffi::ShuffleClientHandle>,
}

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

        cxx::let_cxx_string!(app_id_cxx = &config.app_id);
        cxx::let_cxx_string!(codec_cxx = &config.shuffle_compression_codec);
        let mut handle =
            ffi::create_client(&app_id_cxx, config.push_buffer_max_size, &codec_cxx)?;

        cxx::let_cxx_string!(host_cxx = lm_host);
        ffi::setup_lifecycle_manager(handle.pin_mut(), &host_cxx, lm_port)?;

        Ok(Self { inner: handle })
    }

    /// Push data for a specific partition.
    pub fn push_data(
        &mut self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<()> {
        ffi::push_data(
            self.inner.pin_mut(),
            shuffle_id,
            map_id,
            attempt_id,
            partition_id,
            data,
            num_mappers,
            num_partitions,
        )?;
        Ok(())
    }

    /// Signal that a mapper has finished writing all its partitions.
    pub fn mapper_end(
        &mut self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<()> {
        ffi::mapper_end(self.inner.pin_mut(), shuffle_id, map_id, attempt_id, num_mappers)?;
        Ok(())
    }

    /// Update reducer file group metadata for a given shuffle.
    pub fn update_reducer_file_group(&mut self, shuffle_id: i32) -> Result<()> {
        ffi::update_reducer_file_group(self.inner.pin_mut(), shuffle_id)?;
        Ok(())
    }

    /// Read all data for a partition with full control over parameters.
    pub fn read_partition(
        &mut self,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
    ) -> Result<Vec<u8>> {
        let data = ffi::read_partition_full(
            self.inner.pin_mut(),
            shuffle_id,
            partition_id,
            attempt_number,
            start_map_index,
            end_map_index,
        )?;
        Ok(data)
    }

    /// Convenience: read all map outputs for a partition.
    #[inline]
    pub fn read_partition_all(
        &mut self,
        shuffle_id: i32,
        partition_id: i32,
        num_mappers: i32,
    ) -> Result<Vec<u8>> {
        self.read_partition(shuffle_id, partition_id, 0, 0, num_mappers)
    }

    /// Explicitly shut down the client. Preferred over relying on Drop.
    pub fn shutdown(mut self) -> Result<()> {
        let mut handle = std::mem::replace(
            &mut self.inner,
            UniquePtr::<ffi::ShuffleClientHandle>::null(),
        );
        match handle.as_mut() {
            Some(pinned) => {
                ffi::shutdown(pinned)?;
            }
            None => {}
        }
        Ok(())
    }
}

impl Drop for ShuffleClient {
    fn drop(&mut self) {
        if self.inner.is_null() {
            return;
        }
        if let Some(pinned) = self.inner.as_mut() {
            if let Err(e) = ffi::shutdown(pinned) {
                log::error!(
                    "ffi::shutdown failed during Drop: {e}; \
                     caller should explicitly call ShuffleClient::shutdown() before drop"
                );
            }
        }
    }
}
