// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cxx::bridge(namespace = "celeborn_ffi")]
pub mod ffi {
    unsafe extern "C++" {
        include!("wrapper.h");

        type ShuffleClientHandle;
        type PartitionReaderHandle;

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

        fn open_partition_reader(
            handle: Pin<&mut ShuffleClientHandle>,
            shuffle_id: i32,
            partition_id: i32,
            attempt_number: i32,
            start_map_index: i32,
            end_map_index: i32,
        ) -> Result<UniquePtr<PartitionReaderHandle>>;

        fn read_partition_chunk(
            reader: Pin<&mut PartitionReaderHandle>,
            out: &mut [u8],
        ) -> Result<usize>;
    }
}
