# Performance Benchmarks

This directory contains performance benchmarks for robocodec using the [Criterion](https://github.com/bheisler/criterion.rs) benchmarking framework.

## Running Benchmarks

### Run All Benchmarks

```bash
cargo bench
```

### Run Specific Benchmark

```bash
# Reader benchmarks
cargo bench --bench reader_bench

# Decoder benchmarks
cargo bench --bench decoder_bench

# Rewriter benchmarks
cargo bench --bench rewriter_bench

# Large file benchmarks
cargo bench --bench large_file_bench
```

### Run Specific Benchmark Group

```bash
# Only run "open" benchmarks
cargo bench --bench reader_bench -- open

# Only run "decode_throughput" benchmarks
cargo bench --bench decoder_bench -- decode_throughput

# Only run "large_mcap_read" benchmarks
cargo bench --bench large_file_bench -- large_mcap_read
```

### Save Baseline

**Important:** You must specify which benchmark to run. The `--save-baseline` option is a Criterion flag passed to the benchmark binary, not to cargo itself.

```bash
# Save baseline for a specific benchmark
cargo bench --bench decoder_bench -- --save-baseline main

# Save baseline for all benchmarks (run each one)
cargo bench --bench decoder_bench -- --save-baseline main
cargo bench --bench reader_bench -- --save-baseline main
cargo bench --bench rewriter_bench -- --save-baseline main
cargo bench --bench large_file_bench -- --save-baseline main
```

**Note:** Do NOT use `cargo bench -- --save-baseline main` without `--bench <name>` - this will fail because it attempts to run unit tests (which don't use Criterion).

### Compare Against Baseline

```bash
# Compare a specific benchmark against baseline
cargo bench --bench decoder_bench -- --baseline main

# Compare all benchmarks against baseline
for bench in decoder_bench reader_bench rewriter_bench large_file_bench; do
    cargo bench --bench $bench -- --baseline main
done
```

### Using cargo-criterion (Optional)

For enhanced baseline management and comparison reports:

```bash
# Install cargo-criterion
cargo install cargo-criterion

# Run all benchmarks with automatic baseline handling
cargo criterion

# Save and compare baselines easily
cargo criterion -- --save-baseline main
cargo criterion -- --baseline main
```

## Benchmark Files

### `reader_bench.rs`

Benchmarks for file reading performance by format.

**Benchmarks:**
- `open` - File opening and format detection overhead
- `read_messages` - Full file read throughput (with MB/s metrics)
- `channel_lookup` - Channel lookup by topic name
- `metadata` - Metadata extraction performance

**What it measures:**
- I/O performance
- Decompression overhead
- Format detection speed
- Iterator overhead

### `decoder_bench.rs`

Benchmarks for message decoding throughput.

**Benchmarks:**
- `decode_throughput` - Messages decoded per second
- `field_access` - Field access performance
- `message_clone` - Message cloning overhead
- `value_operations` - CodecValue operations
- `iteration_patterns` - Different iteration patterns

**What it measures:**
- Deserialization performance
- Memory allocation patterns
- Field extraction overhead
- Copy-on-write behavior

### `rewriter_bench.rs`

Benchmarks for format conversion and rewriting.

**Benchmarks:**
- `format_conversion` - MCAP <-> BAG conversion performance
- `rewriter_setup` - Rewriter initialization overhead
- `message_copy` - Message copying during rewrite
- `channel_extraction` - Channel info extraction
- `stats_collection` - Statistics collection overhead

**What it measures:**
- Format conversion overhead
- Channel mapping performance
- Message throughput during rewrite

### `large_file_bench.rs`

Benchmarks for large file handling and scaling.

**Benchmarks:**
- `large_mcap_read` - Large MCAP file performance
- `large_bag_read` - Large BAG file performance
- `partial_read` - Partial file access (first N messages)
- `large_metadata` - Metadata extraction for large files
- `streaming` - Iterator streaming vs. collecting
- `memory_patterns` - Memory allocation patterns
- `file_size_scaling` - Performance scaling by file size

**What it measures:**
- Scaling characteristics
- Memory efficiency
- Streaming behavior
- Cache effects

## Understanding Benchmark Results

Criterion produces HTML reports in `target/criterion/`:

```bash
open target/criterion/report/index.html
```

### Key Metrics

- **Time** - Measured time per iteration
- **Throughput** - Bytes or elements processed per second
- **Comparison** - Performance change from baseline
- **Variance** - Consistency of measurements

### Interpreting Results

**Good indicators:**
- Higher throughput (MB/s or messages/s)
- Lower time per iteration
- Low variance (< 5%)
- Consistent performance across file sizes

**Warning signs:**
- Performance regression (> 10% slower)
- High variance (> 10%)
- Poor scaling with file size
- Memory allocation spikes

## CI Integration

Benchmarks are intentionally **not** run in CI by default because:
1. They take significant time
2. Results can vary across different machines
3. CI environments may not be representative

However, you can optionally run benchmarks in CI:

```yaml
# .github/workflows/bench.yml
- name: Run benchmarks
  run: cargo bench -- --save-baseline ci
```

## Test Fixtures

Benchmarks use files from `tests/fixtures/`:
- `robocodec_test_*.mcap` - MCAP format test files
- `robocodec_test_*.bag` - ROS1 bag format test files

Files are selected based on size:
- **Small** (< 100 KB) - Microbenchmarks, low overhead
- **Large** (> 1 MB) - Realistic workloads

## Adding New Benchmarks

When adding a new benchmark:

1. Use `black_box()` to prevent compiler optimizations
2. Set appropriate `sample_size` for long-running benchmarks
3. Use `Throughput` for meaningful metrics (bytes/s, messages/s)
4. Follow naming convention: `bench_<category>`

Example:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_my_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("my_category");

    group.bench_function("my_benchmark", |b| {
        b.iter(|| {
            // Your code here
            black_box(result);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_my_operation);
criterion_main!(benches);
```

## Performance Regression Detection

To detect regressions:

1. **Establish baseline** before major changes:
   ```bash
   cargo bench -- --save-baseline before
   ```

2. **Make changes** to code

3. **Compare against baseline**:
   ```bash
   cargo bench -- --baseline before
   ```

4. **Review HTML report** for significant changes

**Red flags:**
- > 10% slower in any benchmark
- Increased memory allocations
- Higher variance (less stable performance)

## Troubleshooting

### Benchmarks are too slow

Reduce `sample_size`:
```rust
group.sample_size(10);
```

### Inconsistent results

- Close other applications
- Use `--sample-size` to increase iterations
- Check thermal throttling on laptops

### "No such file or directory" error

Ensure test fixtures exist:
```bash
ls tests/fixtures/
```

Fixtures are generated by tests in `tests/` directory.

## Best Practices

1. **Run before committing** performance changes
2. **Save baselines** for important milestones
3. **Document regressions** with issue links
4. **Profile first** - use `cargo flamegraph` before optimizing
5. **Benchmark real workloads** - avoid synthetic tests

## Resources

- [Criterion.rs User Guide](https://bheisler.github.io/criterion.rs/book/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [cargo-flamegraph](https://github.com/flamegraph-rs/flamegraph)
