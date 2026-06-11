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

//! Rust equivalent of cpp/celeborn/tests/DataSumWithReaderClient.cpp
//! Usage: data_sum_reader <lm_host> <lm_port> <app_id> <shuffle_id> <attempt_id>
//!        <num_mappers> <num_partitions> <result_file> <compress_codec>
//!
//! Concurrency: a single `Arc<ShuffleClient>` is shared across `num_partitions`
//! threads. Each thread opens and drains its own partition reader in parallel —
//! exercising the `&self` `open_partition` path. Each `PartitionReader` itself
//! stays single-threaded (its `Read` impl takes `&mut self`).
//!
//! Set RUST_LOG=info for diagnostic output.

use celeborn_client::{Config, ShuffleClient};
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::thread;

fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 10 {
        eprintln!(
            "Usage: {} <lm_host> <lm_port> <app_id> <shuffle_id> <attempt_id> \
             <num_mappers> <num_partitions> <result_file> <compress_codec>",
            args[0]
        );
        std::process::exit(1);
    }

    let lm_host = &args[1];
    let lm_port: i32 = args[2].parse().expect("lm_port must be an integer");
    let app_id = args[3].clone();
    let shuffle_id: i32 = args[4].parse().expect("shuffle_id must be an integer");
    let attempt_id: i32 = args[5].parse().expect("attempt_id must be an integer");
    let num_mappers: i32 = args[6].parse().expect("num_mappers must be an integer");
    let num_partitions: i32 = args[7].parse().expect("num_partitions must be an integer");
    let result_file = &args[8];
    let compress_codec = args[9].clone();

    println!(
        "lm_host={lm_host}, lm_port={lm_port}, app_id={app_id}, \
         shuffle_id={shuffle_id}, attempt_id={attempt_id}, \
         num_mappers={num_mappers}, num_partitions={num_partitions}, \
         result_file={result_file}, compress_codec={compress_codec}"
    );

    let mut config = Config::new(app_id);
    config.shuffle_compression_codec = compress_codec;

    let client =
        Arc::new(ShuffleClient::connect(config, lm_host, lm_port).expect("Failed to connect to LM"));

    client
        .update_reducer_file_group(shuffle_id)
        .expect("update_reducer_file_group failed");

    // Spawn one thread per partition. All threads share a single
    // `Arc<ShuffleClient>` and open their readers concurrently via the `&self`
    // API. Each thread returns its (partition_id, sum, data_count).
    let handles: Vec<_> = (0..num_partitions)
        .map(|partition_id| {
            let client = Arc::clone(&client);
            thread::spawn(move || {
                let reader = client
                    .open_partition(shuffle_id, partition_id, attempt_id, 0, num_mappers)
                    .expect("open_partition failed");
                let mut buf_reader = BufReader::with_capacity(64 * 1024, reader);

                let mut sum: i64 = 0;
                let mut current_number: i64 = 0;
                let mut data_count: usize = 0;
                let mut byte = [0u8; 1];

                loop {
                    let n = buf_reader.read(&mut byte).expect("read failed");
                    if n == 0 {
                        break;
                    }
                    let c = byte[0] as char;
                    match c {
                        '-' => {
                            sum += current_number;
                            current_number = 0;
                            data_count += 1;
                        }
                        '+' => {}
                        '0'..='9' => {
                            current_number = current_number * 10 + (c as i64 - '0' as i64);
                        }
                        _ => {
                            panic!("Unexpected character in partition data: '{c}'");
                        }
                    }
                }
                // Add the last number (data after last '-')
                sum += current_number;

                (partition_id, sum, data_count)
            })
        })
        .collect();

    let mut result = vec![0i64; num_partitions as usize];
    for handle in handles {
        let (partition_id, sum, data_count) = handle.join().expect("reader thread panicked");
        result[partition_id as usize] = sum;
        println!("partition {partition_id} sum result = {sum}, dataCnt = {data_count}");
    }

    let mut file = File::create(result_file).expect("Failed to create result file");
    for sum in &result {
        writeln!(file, "{sum}").expect("Failed to write result");
    }

    // All reader threads have joined, so this is the only remaining Arc handle.
    let client = Arc::try_unwrap(client)
        .unwrap_or_else(|_| panic!("ShuffleClient still has other Arc references"));
    client.shutdown().expect("shutdown failed");
    println!("Reader completed successfully.");
}
