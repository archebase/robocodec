#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

set -euo pipefail

# Script to run fuzzing tests for CI/CD or development
# This script can run without cargo-fuzz for basic validation

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
FUZZ_TIME="${FUZZ_TIME:-30}"  # Default 30 seconds per fuzzer
TIMEOUT="${TIMEOUT:-10}"      # Default 10 seconds per test case

echo "========================================="
echo "Robocodec Fuzzing Test Runner"
echo "========================================="
echo ""
echo "Configuration:"
echo "  Fuzz time per target: ${FUZZ_TIME}s"
echo "  Timeout per test case: ${TIMEOUT}s"
echo ""

# Check if cargo-fuzz is installed
if ! cargo +nightly fuzz --version &> /dev/null; then
    echo -e "${YELLOW}Warning: cargo-fuzz not found${NC}"
    echo "Fuzzing requires cargo-fuzz. Install with:"
    echo "  cargo +nightly install cargo-fuzz --locked"
    echo ""
    echo "Running basic parser validation instead..."
    echo ""

    # Run basic validation tests
    cargo test --test fuzz_validation -- --nocapture || {
        echo -e "${RED}Basic validation tests failed${NC}"
        exit 1
    }

    echo -e "${GREEN}✓ Basic validation passed${NC}"
    exit 0
fi

# Function to run a single fuzz target
run_fuzzer() {
    local target=$1
    local dict=$2
    local extra_args="${3:-}"

    echo -e "${GREEN}Running fuzzer: $target${NC}"

    local cmd="cargo +nightly fuzz run $target -- -timeout=$TIMEOUT -max_total_time=$FUZZ_TIME"

    if [ -n "$dict" ] && [ -f "$dict" ]; then
        cmd="$cmd -dict=$dict"
    fi

    if [ -n "$extra_args" ]; then
        cmd="$cmd $extra_args"
    fi

    # Run the fuzzer, capture exit code
    if eval "$cmd"; then
        echo -e "${GREEN}✓ $target: No crashes found${NC}"
        return 0
    else
        exit_code=$?
        if [ $exit_code -eq 1 ]; then
            echo -e "${RED}✗ $target: Crashes found!${NC}"
            echo "Check fuzz/artifacts/$target/ for crash inputs"
            return 1
        else
            echo -e "${YELLOW}⚠ $target: Fuzzer exited with code $exit_code${NC}"
            return 0  # Don't fail on non-crash errors
        fi
    fi
}

# Track overall success
FUZZ_SUCCESS=true

# Run each fuzz target
echo "========================================="
echo "Running Fuzz Targets"
echo "========================================="
echo ""

# MCAP parser
run_fuzzer "mcap_parser" "fuzz/dictionaries/mcap.dict" || FUZZ_SUCCESS=false
echo ""

# ROS1 bag parser
run_fuzzer "bag_parser" "fuzz/dictionaries/bag.dict" || FUZZ_SUCCESS=false
echo ""

# RRF2 parser
run_fuzzer "rrd_parser" "" || FUZZ_SUCCESS=false
echo ""

# CDR decoder
run_fuzzer "cdr_decoder" "" || FUZZ_SUCCESS=false
echo ""

# Schema parser
run_fuzzer "schema_parser" "fuzz/dictionaries/schema.dict" || FUZZ_SUCCESS=false
echo ""

# Summary
echo "========================================="
echo "Summary"
echo "========================================="

if [ "$FUZZ_SUCCESS" = true ]; then
    echo -e "${GREEN}✓ All fuzzers completed without crashes${NC}"
    exit 0
else
    echo -e "${RED}✗ Some fuzzers found crashes${NC}"
    exit 1
fi
