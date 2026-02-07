// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Benchmark decoding throughput for different message types and encodings.
//!
//! This benchmark measures the performance of:
//! - Message decoding (CDR, Protobuf, JSON)
//! - Field extraction
//! - Message cloning and copying

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use robocodec::RoboReader;
use robocodec::io::FormatReader;
use std::path::Path;

/// Benchmark full message decoding throughput.
///
/// This measures the time to decode messages from various formats.
fn bench_decode_throughput(c: &mut Criterion) {
    // Small MCAP file - measure throughput
    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        let reader = RoboReader::open(mcap_path).unwrap();
        let message_count = reader.message_count();

        if message_count > 0 {
            let mut group = c.benchmark_group("decode_throughput");
            group.throughput(Throughput::Elements(message_count));

            group.bench_function(BenchmarkId::new("mcap", "small"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let messages: Vec<_> = iter.filter_map(|r| r.ok()).collect();
                    black_box(messages);
                })
            });

            group.finish();
        }
    }

    // Larger MCAP file
    let mcap_large_path = "tests/fixtures/robocodec_test_16.mcap";
    if Path::new(mcap_large_path).exists() {
        let reader = RoboReader::open(mcap_large_path).unwrap();
        let message_count = reader.message_count();

        if message_count > 0 {
            let mut group = c.benchmark_group("decode_throughput");
            group.throughput(Throughput::Elements(message_count));
            group.sample_size(20); // Reduce samples for large files

            group.bench_function(BenchmarkId::new("mcap", "large"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(mcap_large_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let messages: Vec<_> = iter.filter_map(|r| r.ok()).collect();
                    black_box(messages);
                })
            });

            group.finish();
        }
    }

    // BAG file
    let bag_path = "tests/fixtures/robocodec_test_18.bag";
    if Path::new(bag_path).exists() {
        let reader = RoboReader::open(bag_path).unwrap();
        let message_count = reader.message_count();

        if message_count > 0 {
            let mut group = c.benchmark_group("decode_throughput");
            group.throughput(Throughput::Elements(message_count));

            group.bench_function(BenchmarkId::new("bag", "small"), |b| {
                b.iter(|| {
                    let reader = RoboReader::open(black_box(bag_path)).unwrap();
                    let iter = reader.decoded().unwrap();
                    let messages: Vec<_> = iter.filter_map(|r| r.ok()).collect();
                    black_box(messages);
                })
            });

            group.finish();
        }
    }
}

/// Benchmark field access from decoded messages.
///
/// This measures the overhead of accessing fields from decoded messages.
fn bench_field_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_access");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        // Collect some sample messages for benchmarking
        let reader = RoboReader::open(mcap_path).unwrap();
        let sample_messages: Vec<_> = reader
            .decoded()
            .unwrap()
            .filter_map(|r| r.ok())
            .take(100)
            .collect();

        if !sample_messages.is_empty() {
            group.bench_function("read_first_field", |b| {
                b.iter(|| {
                    for msg in &sample_messages {
                        if let Some((name, value)) = msg.message.iter().next() {
                            black_box(name);
                            black_box(value);
                        }
                    }
                })
            });

            group.bench_function("iterate_all_fields", |b| {
                b.iter(|| {
                    for msg in &sample_messages {
                        for (name, value) in &msg.message {
                            black_box(name);
                            black_box(value);
                        }
                    }
                })
            });
        }
    }

    group.finish();
}

/// Benchmark message cloning operations.
///
/// This measures the cost of cloning decoded messages,
/// which is important for understanding copy-on-write overhead.
fn bench_message_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_clone");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        let reader = RoboReader::open(mcap_path).unwrap();
        let sample_messages: Vec<_> = reader
            .decoded()
            .unwrap()
            .filter_map(|r| r.ok())
            .take(10)
            .collect();

        if !sample_messages.is_empty() {
            group.bench_function("clone_single_message", |b| {
                let msg = &sample_messages[0];
                b.iter(|| {
                    black_box(msg.message.clone());
                })
            });

            group.bench_function("clone_message_batch", |b| {
                b.iter(|| {
                    let cloned: Vec<_> =
                        sample_messages.iter().map(|m| m.message.clone()).collect();
                    black_box(cloned);
                })
            });
        }
    }

    group.finish();
}

/// Benchmark value type operations.
///
/// This measures the performance of working with different CodecValue types.
fn bench_value_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("value_operations");

    // Benchmark string value access
    group.bench_function("access_string_value", |b| {
        let value = robocodec::CodecValue::String("test string value".to_string());
        b.iter(|| {
            if let robocodec::CodecValue::String(s) = black_box(&value) {
                black_box(s.len());
            }
        })
    });

    // Benchmark array value access
    group.bench_function("access_array_value", |b| {
        let value = robocodec::CodecValue::Array(vec![
            robocodec::CodecValue::Int64(1),
            robocodec::CodecValue::Int64(2),
            robocodec::CodecValue::Int64(3),
            robocodec::CodecValue::Int64(4),
            robocodec::CodecValue::Int64(5),
        ]);
        b.iter(|| {
            if let robocodec::CodecValue::Array(arr) = black_box(&value) {
                black_box(arr.len());
            }
        })
    });

    // Benchmark struct value access
    group.bench_function("access_struct_value", |b| {
        let mut fields = std::collections::HashMap::new();
        for i in 0..10 {
            fields.insert(
                format!("field_{}", i),
                robocodec::CodecValue::Int64(i as i64),
            );
        }
        let value = robocodec::CodecValue::Struct(fields);

        b.iter(|| {
            if let robocodec::CodecValue::Struct(fields) = black_box(&value) {
                black_box(fields.len());
            }
        })
    });

    group.finish();
}

/// Benchmark iteration overhead.
///
/// This compares the performance of different iteration patterns.
fn bench_iteration_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("iteration_patterns");

    let mcap_path = "tests/fixtures/robocodec_test_0.mcap";
    if Path::new(mcap_path).exists() {
        group.bench_function("count_messages", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                let iter = reader.decoded().unwrap();
                let count = iter.filter_map(|r| r.ok()).count();
                black_box(count);
            })
        });

        group.bench_function("collect_messages", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                let iter = reader.decoded().unwrap();
                let messages: Vec<_> = iter.filter_map(|r| r.ok()).collect();
                black_box(messages);
            })
        });

        group.bench_function("first_n_messages", |b| {
            b.iter(|| {
                let reader = RoboReader::open(black_box(mcap_path)).unwrap();
                let iter = reader.decoded().unwrap();
                let messages: Vec<_> = iter.filter_map(|r| r.ok()).take(10).collect();
                black_box(messages);
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_decode_throughput,
    bench_field_access,
    bench_message_clone,
    bench_value_operations,
    bench_iteration_patterns
);
criterion_main!(benches);
