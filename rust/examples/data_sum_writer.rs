//! Rust equivalent of cpp/celeborn/tests/DataSumWithWriterClient.cpp
//! Usage: data_sum_writer <lm_host> <lm_port> <app_id> <shuffle_id> <attempt_id>
//!        <num_mappers> <num_partitions> <result_file> <compress_codec>
//!
//! Requires env_logger initialized for diagnostic output from celeborn-client Drop path.
//! Set RUST_LOG=info (or debug) for verbose output.

use celeborn_client::{Config, ShuffleClient};
use std::env;
use std::fs::File;
use std::io::Write;

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

    let mut client =
        ShuffleClient::connect(config, lm_host, lm_port).expect("Failed to connect to LM");

    let max_data: i64 = 1_000_000;
    let num_data: usize = 1000;
    let mut result = vec![0i64; num_partitions as usize];

    for map_id in 0..num_mappers {
        for partition_id in 0..num_partitions {
            let mut partition_data = String::new();
            for _ in 0..num_data {
                let data = rand_simple(max_data);
                result[partition_id as usize] += data;
                partition_data.push('-');
                partition_data.push_str(&data.to_string());
            }

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
    }

    for (partition_id, sum) in result.iter().enumerate() {
        println!("partition {partition_id} sum result = {sum}");
    }

    let mut file = File::create(result_file).expect("Failed to create result file");
    for sum in &result {
        writeln!(file, "{sum}").expect("Failed to write result");
    }

    client.shutdown().expect("shutdown failed");
    println!("Writer completed successfully.");
}

/// Simple pseudo-random number generator (deterministic per process, good enough for testing).
fn rand_simple(max_val: i64) -> i64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(12345);
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        ((x >> 1) as i64) % max_val
    })
}
