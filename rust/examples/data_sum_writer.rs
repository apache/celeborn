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

//! Rust equivalent of cpp/celeborn/tests/DataSumWithWriterClient.cpp
//! Usage: data_sum_writer <lm_host> <lm_port> <app_id> <shuffle_id> <attempt_id>
//!        <num_mappers> <num_partitions> <result_file> <compress_codec>
//!
//! Concurrency: a single `Arc<ShuffleClient>` is shared across `num_mappers`
//! threads. Each thread owns one map task, pushes all of its partitions, and
//! issues `mapper_end`, all in parallel — exercising the `&self` push path that
//! makes the client safe to share for concurrent writes.
//!
//! Requires env_logger initialized for diagnostic output from celeborn-client Drop path.
//! Set RUST_LOG=info (or debug) for verbose output.

use celeborn_client::{Config, ShuffleClient};
use std::env;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicI64, Ordering};
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

    let max_data: i64 = 1_000_000;
    let num_data: usize = 1000;

    // Shared per-partition sums; multiple mapper threads accumulate concurrently.
    let result: Arc<Vec<AtomicI64>> =
        Arc::new((0..num_partitions).map(|_| AtomicI64::new(0)).collect());

    // Spawn one thread per map task. All threads share a single
    // `Arc<ShuffleClient>` and push concurrently via the `&self` API.
    let handles: Vec<_> = (0..num_mappers)
        .map(|map_id| {
            let client = Arc::clone(&client);
            let result = Arc::clone(&result);
            thread::spawn(move || {
                // Seed the RNG per map task so every mapper thread produces a
                // DISTINCT byte stream. A shared/fixed seed would make all
                // threads push identical data, which masks data races in the
                // concurrent `&self` push path this example is meant to stress.
                let mut rng_state: u64 = 0x9E37_79B9_7F4A_7C15 ^ (map_id as u64).wrapping_add(1);
                for partition_id in 0..num_partitions {
                    let mut partition_data = String::new();
                    let mut partition_sum: i64 = 0;
                    for _ in 0..num_data {
                        let data = rand_simple(&mut rng_state, max_data);
                        partition_sum += data;
                        partition_data.push('-');
                        partition_data.push_str(&data.to_string());
                    }
                    result[partition_id as usize].fetch_add(partition_sum, Ordering::Relaxed);

                    client
                        .push_data(
                            shuffle_id,
                            map_id,
                            attempt_id,
                            partition_id,
                            partition_data.as_bytes(),
                            num_mappers,
                            num_partitions,
                        )
                        .expect("push_data failed");
                }
                client
                    .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
                    .expect("mapper_end failed");
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("mapper thread panicked");
    }

    let result: Vec<i64> = result.iter().map(|v| v.load(Ordering::Relaxed)).collect();
    for (partition_id, sum) in result.iter().enumerate() {
        println!("partition {partition_id} sum result = {sum}");
    }

    let mut file = File::create(result_file).expect("Failed to create result file");
    for sum in &result {
        writeln!(file, "{sum}").expect("Failed to write result");
    }

    // All mapper threads have joined, so this is the only remaining Arc handle.
    let client = Arc::try_unwrap(client)
        .unwrap_or_else(|_| panic!("ShuffleClient still has other Arc references"));
    client.shutdown().expect("shutdown failed");
    println!("Writer completed successfully.");
}

/// Simple xorshift pseudo-random number generator. The caller owns the
/// `state`, so each mapper thread can seed it independently and generate a
/// distinct byte stream — necessary for the concurrency stress this example
/// performs (a shared/fixed seed would make all threads emit identical data
/// and hide races in the `&self` push path).
fn rand_simple(state: &mut u64, max_val: i64) -> i64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    ((x >> 1) as i64) % max_val
}
