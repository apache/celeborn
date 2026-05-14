#[cxx::bridge(namespace = "celeborn_ffi")]
pub mod ffi {
    unsafe extern "C++" {
        include!("wrapper.h");

        type ShuffleClientHandle;

        fn create_client(
            app_id: &CxxString,
            push_buffer_max_size: i32,
            shuffle_compression_codec: &CxxString,
        ) -> Result<UniquePtr<ShuffleClientHandle>>;

        fn setup_lifecycle_manager(
            handle: Pin<&mut ShuffleClientHandle>,
            host: &CxxString,
            port: i32,
        ) -> Result<()>;

        fn shutdown(handle: Pin<&mut ShuffleClientHandle>) -> Result<()>;

        fn push_data(
            handle: Pin<&mut ShuffleClientHandle>,
            shuffle_id: i32,
            map_id: i32,
            attempt_id: i32,
            partition_id: i32,
            data: &[u8],
            num_mappers: i32,
            num_partitions: i32,
        ) -> Result<()>;

        fn mapper_end(
            handle: Pin<&mut ShuffleClientHandle>,
            shuffle_id: i32,
            map_id: i32,
            attempt_id: i32,
            num_mappers: i32,
        ) -> Result<()>;

        fn update_reducer_file_group(
            handle: Pin<&mut ShuffleClientHandle>,
            shuffle_id: i32,
        ) -> Result<()>;

        fn read_partition_full(
            handle: Pin<&mut ShuffleClientHandle>,
            shuffle_id: i32,
            partition_id: i32,
            attempt_number: i32,
            start_map_index: i32,
            end_map_index: i32,
        ) -> Result<Vec<u8>>;
    }
}
