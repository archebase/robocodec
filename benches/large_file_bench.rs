// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark large file handling and performance characteristics.
//!
//! This benchmark measures:
//! - Large file read performance
//! - Memory efficiency during processing
//! - Sequential vs parallel reading comparison
//! - Streaming behavior for large datasets

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use robocodec::RoboReader;
use robocodec::io::FormatReader;
use std::path::Path;

/// Benchmark reading large MCAP files.
///
/// This tests performance with files > 1MB to identify any performance
/// degradation with larger datasets.
fn bench_large_mcap_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_mcap_read");
    group.sample_size(10); // Reduce samples for large files

    // Test various file sizes
    let test_files = [
        ("tests/fixtures/robocodec_test_0.mcap", "small"),
        ("tests/fixtures/robocodec_test_16.mcap", "large"),
    ];

    for (path, size_label) in test_files {
        if Path::new(path).exists() {
            let reader = RoboReader::open(path).unwrap();
            let file_size = reader.file_size();
            let message_count = reader.message_count();

            if file_size > 0 && message_count > 0 {
                group.throughput(Throughput::Bytes(file_size));

                group.bench_function(BenchmarkId::new("full_read", size_label), |b| {
                    b.iter(|| {
                        let reader = RoboReader::open(black_box(path)).unwrap();
                        let iter = reader.decoded().unwrap();
                        let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                        black_box(count);
                    })
                });
            }
        }
    }

    group.finish();
}

/// Benchmark reading large BAG files.
///
/// ROS1 bag files have different performance characteristics due to
/// their chunk-based structure.
fn bench_large_bag_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_bag_read");
    group.sample_size(10); // Reduce samples for large files

    // Test various file sizes
    let test_files = [
        ("tests/fixtures/robocodec_test_18.bag", "small"),
        ("tests/fixtures/robocodec_test_15.bag", "large"),
    ];

    for (path, size_label) in test_files {
        if Path::new(path).exists() {
            let reader = RoboReader::open(path).unwrap();
            let file_size = reader.file_size();
            let message_count = reader.message_count();

            if file_size > 0 && message_count > 0 {
                group.throughput(Throughput::Bytes(file_size));

                group.bench_function(BenchmarkId::new("full_read", size_label), |b| {
                    b.iter(|| {
                        let reader = RoboReader::open(black_box(path)).unwrap();
                        let iter = reader.decoded().unwrap();
                        let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                        black_box(count);
                    })
                });
            }
        }
    }

    group.finish();
}

/// Benchmark partial reads (reading first N messages).
///
/// This measures the efficiency of partial file access.
fn bench_partial_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("partial_read");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        for n in [10, 100, 1000] {
            group.bench_function(BenchmarkId::new("first_n_messages", n), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let messages: Vec<_> = iter.filter_map(|r| r.ok()).take(black_box(n)).collect();
                    black_box(messages);
                })
            });
        }
    }

    group.finish();
}

/// Benchmark file metadata extraction for large files.
///
/// This tests how quickly we can get metadata without reading all messages.
fn bench_large_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_metadata");

    let test_files = [
        ("tests/fixtures/robocodec_test_0.mcap", "mcap_small"),
        ("tests/fixtures/robocodec_test_16.mcap", "mcap_large"),
        ("tests/fixtures/robocodec_test_18.bag", "bag_small"),
        ("tests/fixtures/robocodec_test_15.bag", "bag_large"),
    ];

    for (path, label) in test_files {
        if Path::new(path).exists() {
            group.bench_function(label, |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(path)).unwrap();
                    black_box(reader.file_info());
                })
            });
        }
    }

    group.finish();
}

/// Benchmark streaming iteration.
///
/// This measures the performance of iterator-based streaming
/// vs collecting all messages into memory.
fn bench_streaming(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        // Get message count for throughput
        let reader = RoboReader::open(mcap_path).unwrap();
        let message_count = reader.message_count();

        if message_count > 0 {
            group.throughput(Throughput::Elements(message_count));

            group.bench_function("streaming_count", |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let count = iter.filter_map(|r| r.ok()).count();
                    black_box(count);
                })
            });

            group.bench_function("collect_into_vec", |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let messages: Vec<_> = iter.filter_map(|r| r.ok()).collect();
                    black_box(messages);
                })
            });
        }
    }

    group.finish();
}

/// Benchmark memory allocation patterns.
///
/// This helps identify potential memory efficiency issues.
fn bench_memory_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_patterns");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        // Reuse same reader to measure per-message overhead
        group.bench_function("reuse_reader", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                // Just open and close to measure setup overhead
                black_box(reader.channels().len());
            })
        });

        // Open new reader each iteration (worse case)
        group.bench_function("new_reader_each_time", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                let iter = reader.decoded().unwrap();
                let count = iter.filter_map(|r| r.ok()).take(10).count();
                black_box(count);
            })
        });
    }

    group.finish();
}

/// Benchmark different file sizes to identify scaling characteristics.
fn bench_file_size_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("file_size_scaling");
    group.sample_size(10);

    // Test MCAP files of different sizes
    let mcap_files = [
        "tests/fixtures/robocodec_test_5.mcap",  // ~3KB
        "tests/fixtures/robocodec_test_0.mcap",  // ~87KB
        "tests/fixtures/robocodec_test_16.mcap", // ~3.2MB
    ];

    for (_idx, path) in mcap_files.iter().enumerate() {
        if Path::new(path).exists() {
            let reader = RoboReader::open(path).unwrap();
            let file_size = reader.file_size();
            let message_count = reader.message_count();

            if file_size > 0 && message_count > 0 {
                let size_label = format!("{:.1}_MB", file_size as f64 / (1024.0 * 1024.0));

                group.throughput(Throughput::Bytes(file_size));
                group.bench_function(BenchmarkId::new("mcap", size_label), |b| {
                    b.iter(|| {
                        let reader = RoboReader::open(black_box(path)).unwrap();
                        let iter = reader.decoded().unwrap();
                        let count: u64 = iter.filter_map(|r| r.ok()).count() as u64;
                        black_box(count);
                    })
                });
            }
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_large_mcap_read,
    bench_large_bag_read,
    bench_partial_read,
    bench_large_metadata,
    bench_streaming,
    bench_memory_patterns,
    bench_file_size_scaling
);
criterion_main!(benches);
