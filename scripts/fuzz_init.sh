#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

set -euo pipefail

# Script to initialize fuzzing infrastructure for robocodec

echo "Initializing fuzzing infrastructure..."
echo ""

# Check if rustup is installed
if ! command -v rustup &> /dev/null; then
    echo "Error: rustup not found. Please install Rust from https://rustup.rs/"
    exit 1
fi

# Install nightly toolchain
echo "Installing nightly Rust toolchain..."
rustup install nightly
rustup component add llvm-tools-preview --toolchain nightly

# Install cargo-fuzz
echo "Installing cargo-fuzz..."
cargo +nightly install cargo-fuzz --locked

# Create corpus directories from existing fixtures
echo ""
echo "Setting up seed corpus from test fixtures..."
mkdir -p fuzz/corpus/mcap_parser
mkdir -p fuzz/corpus/bag_parser

# Copy MCAP fixtures if they exist
if [ -d "tests/fixtures" ]; then
    for mcap_file in tests/fixtures/*.mcap; do
        if [ -f "$mcap_file" ]; then
            echo "  Copying $(basename "$mcap_file") to MCAP corpus"
            cp "$mcap_file" fuzz/corpus/mcap_parser/
        fi
    done

    for bag_file in tests/fixtures/*.bag; do
        if [ -f "$bag_file" ]; then
            echo "  Copying $(basename "$bag_file") to bag corpus"
            cp "$bag_file" fuzz/corpus/bag_parser/
        fi
    done
fi

echo ""
echo "✓ Fuzzing infrastructure initialized!"
echo ""
echo "Quick start:"
echo "  make fuzz          # Run quick fuzzing check"
echo "  make fuzz-all      # Run all fuzz targets"
echo "  make fuzz-mcap     # Fuzz MCAP parser only"
echo "  make fuzz-bag      # Fuzz bag parser only"
echo ""
echo "For more information, see fuzz/README.md"
