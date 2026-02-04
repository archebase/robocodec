#!/bin/bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

# Upload test fixtures to MinIO for S3 streaming tests.
#
# Usage:
#   ./scripts/upload-fixtures-to-minio.sh
#
# Environment variables:
#   MINIO_ENDPOINT    - MinIO endpoint (default: http://localhost:9000)
#   MINIO_BUCKET      - Bucket name (default: test-fixtures)
#   MINIO_USER        - Access key (default: minioadmin)
#   MINIO_PASSWORD    - Secret key (default: minioadmin)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-test-fixtures}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASSWORD="${MINIO_PASSWORD:-minioadmin}"
FIXTURES_DIR="$PROJECT_ROOT/tests/fixtures"

echo "Uploading fixtures to MinIO..."
echo "  Endpoint: $MINIO_ENDPOINT"
echo "  Bucket: $MINIO_BUCKET"
echo "  Fixtures: $FIXTURES_DIR"

# Check if MinIO is running
if ! curl -sf "$MINIO_ENDPOINT/minio/health/live" > /dev/null; then
    echo "Error: MinIO is not running at $MINIO_ENDPOINT"
    echo "Start MinIO with: docker compose up -d"
    exit 1
fi

# Check if mc (MinIO client) is available
if ! command -v mc &> /dev/null; then
    echo "Error: mc (MinIO client) not found"
    echo "Install with: brew install minio/stable/mc  # macOS"
    echo "             or: wget https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc"
    exit 1
fi

# Configure mc alias
mc alias set robocodec-test "$MINIO_ENDPOINT" "$MINIO_USER" "$MINIO_PASSWORD" > /dev/null 2>&1 || true

# Create bucket if it doesn't exist
mc mb "robocodec-test/$MINIO_BUCKET" --ignore-existing

# Upload all MCAP and BAG fixtures
count=0
for file in "$FIXTURES_DIR"/*.mcap "$FIXTURES_DIR"/*.bag; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        echo "  Uploading $filename..."
        mc cp "$file" "robocodec-test/$MINIO_BUCKET/$filename"
        ((count++)) || true
    fi
done

echo ""
echo "Uploaded $count fixture files to MinIO"
echo ""
echo "Run tests with:"
echo "  MINIO_ENDPOINT=$MINIO_ENDPOINT MINIO_BUCKET=$MINIO_BUCKET cargo test --features s3 minio_tests"
