#!/bin/bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

# Run coverage tests with MinIO S3 support
# This script starts MinIO, runs coverage tests, then stops MinIO

set -e

echo "=== Starting MinIO for S3 coverage tests ==="
docker compose up -d

# Wait for MinIO to be healthy
echo "=== Waiting for MinIO to be ready ==="
for i in {1..60}; do
    if docker compose ps | grep -q "healthy"; then
        echo "MinIO is healthy!"
        break
    fi
    echo "Waiting for MinIO... ($i/60)"
    sleep 2
done

# Verify bucket exists
if ! curl -f http://localhost:9000/test-fixtures 2>/dev/null; then
    echo "ERROR: Bucket not found"
    docker compose logs minio minio-init
    docker compose down -v
    exit 1
fi

echo "=== Running coverage tests with S3 ==="
# Export S3 environment variables
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_ENDPOINT=http://localhost:9000

# Run coverage
cargo llvm-cov --package robocodec --lcov --output-path lcov_s3.info

echo "=== Coverage report generated: lcov_s3.info ==="

# Show coverage for transport readers
echo ""
echo "=== Transport Reader Coverage (with S3 tests) ==="
grep -A2000 "SF:.*/bag/transport_reader.rs" lcov_s3.info | grep -E "^(LF|LH):" | awk -F: '{print $2}' | tr '\n' ' ' | awk '{printf "BAG: %.1f%% (%d/%d)\n", ($2/$1*100), $2, $1}' 2>/dev/null || echo "BAG: Could not calculate"
grep -A2000 "SF:.*/rrd/transport_reader.rs" lcov_s3.info | grep -E "^(LF|LH):" | awk -F: '{print $2}' | tr '\n' ' ' | awk '{printf "RRD: %.1f%% (%d/%d)\n", ($2/$1*100), $2, $1}' 2>/dev/null || echo "RRD: Could not calculate"

echo ""
echo "=== Stopping MinIO ==="
docker compose down -v

echo "=== Done ==="
