//! Rust equivalent of cpp/celeborn/tests/DataSumWithReaderClient.cpp
//! Usage: data_sum_reader <lm_host> <lm_port> <app_id> <shuffle_id> <attempt_id>
//!        <num_mappers> <num_partitions> <result_file> <compress_codec>
//!
//! Set RUST_LOG=info for diagnostic output.

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

    client
        .update_reducer_file_group(shuffle_id)
        .expect("update_reducer_file_group failed");

    let mut result = vec![0i64; num_partitions as usize];

    for partition_id in 0..num_partitions {
        let data = client
            .read_partition(shuffle_id, partition_id, attempt_id, 0, num_mappers)
            .expect("read_partition failed");

        let mut current_number: i64 = 0;
        let mut data_count: usize = 0;

        for &byte in &data {
            let c = byte as char;
            match c {
                '-' => {
                    result[partition_id as usize] += current_number;
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
        result[partition_id as usize] += current_number;

        println!(
            "partition {partition_id} sum result = {}, dataCnt = {data_count}",
            result[partition_id as usize]
        );
    }

    let mut file = File::create(result_file).expect("Failed to create result file");
    for sum in &result {
        writeln!(file, "{sum}").expect("Failed to write result");
    }

    client.shutdown().expect("shutdown failed");
    println!("Reader completed successfully.");
}
