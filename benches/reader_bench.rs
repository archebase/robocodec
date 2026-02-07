// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark file reading performance for each supported format.
//!
//! This benchmark measures the performance of reading and iterating over
//! messages in MCAP, ROS1 bag, and RRF2 formats.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use robocodec::RoboReader;
use robocodec::io::FormatReader;
use std::path::Path;

/// Benchmark opening a file and reading metadata.
///
/// This measures the overhead of:
/// - File format detection
/// - Opening the file
/// - Reading metadata (channels, message count, etc.)
fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("open");

    // Benchmark MCAP file opening
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        group.bench_function(BenchmarkId::new("mcap", "small"), |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                black_box(reader.channels());
            })
        });
    }

    // Benchmark larger MCAP file
    let mcap_large_path = "tests/fixtures/robocodec_test_16.mcap";
    if Path::new(mcap_large_path).exists() {
        group.bench_function(BenchmarkId::new("mcap", "large"), |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_large_path)).unwrap();
                black_box(reader.channels());
            })
        });
    }

    // Benchmark BAG file opening
    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if Path::new(bag_path).exists() {
        group.bench_function(BenchmarkId::new("bag", "small"), |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(bag_path)).unwrap();
                black_box(reader.channels());
            })
        });
    }

    // Benchmark larger BAG file
    let bag_large_path = "tests/fixtures/robocodec_test_15.bag";
    if Path::new(bag_large_path).exists() {
        group.bench_function(BenchmarkId::new("bag", "large"), |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(bag_large_path)).unwrap();
                black_box(reader.channels());
            })
        });
    }

    group.finish();
}

/// Benchmark iterating over all messages in a file.
///
/// This measures the throughput of reading messages including:
/// - Decompression
/// - Deserialization
/// - Message iteration
fn bench_read_messages(c: &mut Criterion) {
    // Small MCAP file
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        let reader = RoboReader::open(mcap_path).unwrap();
        let file_size = reader.file_size();
        let message_count = reader.message_count();

        let mut group = c.benchmark_group("read_messages");

        if file_size > 0 && message_count > 0 {
            group.throughput(Throughput::Bytes(file_size));
            group.bench_function(BenchmarkId::new("mcap", "small"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                    black_box(count);
                })
            });
        }
    }

    // Larger MCAP file
    let mcap_large_path = "tests/fixtures/robocodec_test_16.mcap";
    if Path::new(mcap_large_path).exists() {
        let reader = RoboReader::open(mcap_large_path).unwrap();
        let file_size = reader.file_size();
        let message_count = reader.message_count();

        let mut group = c.benchmark_group("read_messages");

        if file_size > 0 && message_count > 0 {
            group.throughput(Throughput::Bytes(file_size));
            group.sample_size(20); // Reduce samples for large files
            group.bench_function(BenchmarkId::new("mcap", "large"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_large_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                    black_box(count);
                })
            });
        }
    }

    // Small BAG file
    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if Path::new(bag_path).exists() {
        let reader = RoboReader::open(bag_path).unwrap();
        let file_size = reader.file_size();
        let message_count = reader.message_count();

        let mut group = c.benchmark_group("read_messages");

        if file_size > 0 && message_count > 0 {
            group.throughput(Throughput::Bytes(file_size));
            group.bench_function(BenchmarkId::new("bag", "small"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(bag_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                    black_box(count);
                })
            });
        }
    }

    // Larger BAG file
    let bag_large_path = "tests/fixtures/robocodec_test_15.bag";
    if Path::new(bag_large_path).exists() {
        let reader = RoboReader::open(bag_large_path).unwrap();
        let file_size = reader.file_size();
        let message_count = reader.message_count();

        let mut group = c.benchmark_group("read_messages");

        if file_size > 0 && message_count > 0 {
            group.throughput(Throughput::Bytes(file_size));
            group.sample_size(20); // Reduce samples for large files
            group.bench_function(BenchmarkId::new("bag", "large"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(bag_large_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                    black_box(count);
                })
            });
        }
    }
}

/// Benchmark channel lookup operations.
///
/// This measures the performance of finding channels by topic name.
fn bench_channel_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("channel_lookup");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        let reader = RoboReader::open(mcap_path).unwrap();

        // Get the first topic name for benchmarking
        if let Some(first_channel) = reader.channels().values().next() {
            let topic = first_channel.topic.clone();

            group.bench_function("mcap_single_topic", |b| {
                b.iter(|| {
                    black_box(reader.channel_by_topic(black_box(&topic)));
                })
            });
        }
    }

    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if Path::new(bag_path).exists() {
        let reader = RoboReader::open(bag_path).unwrap();

        // Get the first topic name for benchmarking
        if let Some(first_channel) = reader.channels().values().next() {
            let topic = first_channel.topic.clone();

            group.bench_function("bag_single_topic", |b| {
                b.iter(|| {
                    black_box(reader.channel_by_topic(black_box(&topic)));
                })
            });
        }
    }

    group.finish();
}

/// Benchmark metadata extraction.
///
/// This measures the performance of extracting file metadata
/// without reading messages.
fn bench_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        group.bench_function("mcap", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                black_box(reader.file_info());
            })
        });
    }

    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if Path::new(bag_path).exists() {
        group.bench_function("bag", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(bag_path)).unwrap();
                black_box(reader.file_info());
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_open,
    bench_read_messages,
    bench_channel_lookup,
    bench_metadata
);
criterion_main!(benches);
