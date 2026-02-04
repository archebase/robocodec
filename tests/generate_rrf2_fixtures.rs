// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test fixture generator for RRF2 files (rerun format).
//!
//! Run with: cargo run --bin generate_rrf2_fixtures

use std::fs;
use std::io::Write;
use std::path::Path;

// RRF2 constants
const RRF2_MAGIC: &[u8] = b"RRF2";
const RRF2_VERSION: [u8; 4] = [0, 0, 0, 1];
const COMPRESSION_OFF: u8 = 0;
const SERIALIZER_PROTOBUF: u8 = 2;

// Message kinds
const MSG_KIND_ARROW_MSG: u64 = 2;
const MSG_KIND_END: u64 = 0;

fn main() -> std::io::Result<()> {
    let out_dir = Path::new("tests/fixtures/rrd");
    fs::create_dir_all(out_dir)?;

    println!("Generating RRF2 test fixtures...");

    generate_small_uncompressed(out_dir)?;
    generate_medium_lz4(out_dir)?;
    generate_large_multichannel(out_dir)?;
    generate_with_timestamps(out_dir)?;

    println!("All RRF2 fixtures generated successfully!");
    Ok(())
}

fn write_rrf2_header<W: Write>(writer: &mut W) -> std::io::Result<()> {
    // StreamHeader: magic(4) + version(4) + options(4)
    writer.write_all(RRF2_MAGIC)?;
    writer.write_all(&RRF2_VERSION)?;
    writer.write_all(&[COMPRESSION_OFF, SERIALIZER_PROTOBUF, 0, 0])?;
    Ok(())
}

fn write_message<W: Write>(writer: &mut W, kind: u64, payload: &[u8]) -> std::io::Result<()> {
    // MessageHeader: kind(8) + len(8)
    writer.write_all(&kind.to_le_bytes())?;
    writer.write_all(&(payload.len() as u64).to_le_bytes())?;
    writer.write_all(payload)?;
    Ok(())
}

fn write_rrf2_footer<W: Write>(writer: &mut W) -> std::io::Result<()> {
    // StreamFooter: entries(20) + magic(4) + identifier(4) + num_entries(4)
    writer.write_all(&[0u8; 8])?; // start
    writer.write_all(&[0u8; 8])?; // len
    writer.write_all(&[0u8; 4])?; // crc
    writer.write_all(&[0u8; 8])?; // (filling 20 bytes total)
    writer.write_all(RRF2_MAGIC)?;
    writer.write_all(b"FOOT")?;
    writer.write_all(&(1u32).to_le_bytes())?; // num_entries
    Ok(())
}

fn generate_small_uncompressed(out_dir: &Path) -> std::io::Result<()> {
    let path = out_dir.join("small_uncompressed.rrd");
    let mut file = fs::File::create(&path)?;

    write_rrf2_header(&mut file)?;

    // Write 10 messages
    for i in 0..10 {
        let payload = format!("message {}", i);
        write_message(&mut file, MSG_KIND_ARROW_MSG, payload.as_bytes())?;
    }

    // End marker
    write_message(&mut file, MSG_KIND_END, &[])?;

    write_rrf2_footer(&mut file)?;

    println!("  Created: small_uncompressed.rrd (10 messages)");
    Ok(())
}

fn generate_medium_lz4(out_dir: &Path) -> std::io::Result<()> {
    let path = out_dir.join("medium_lz4.rrd");
    let mut file = fs::File::create(&path)?;

    // Write header with LZ4 compression
    file.write_all(RRF2_MAGIC)?;
    file.write_all(&RRF2_VERSION)?;
    file.write_all(&[1u8, SERIALIZER_PROTOBUF, 0, 0])?; // LZ4 + protobuf + reserved

    // Write 100 compressed messages
    for i in 0..100 {
        let data = format!("sensor data {}", i);
        let compressed = lz4_flex::block::compress(data.as_bytes());
        write_message(&mut file, MSG_KIND_ARROW_MSG, &compressed)?;
    }

    // End marker
    write_message(&mut file, MSG_KIND_END, &[])?;

    write_rrf2_footer(&mut file)?;

    println!("  Created: medium_lz4.rrd (100 messages, LZ4 compressed)");
    Ok(())
}

fn generate_large_multichannel(out_dir: &Path) -> std::io::Result<()> {
    let path = out_dir.join("large_multichannel.rrd");
    let mut file = fs::File::create(&path)?;

    write_rrf2_header(&mut file)?;

    // Write 200 messages across 4 channels
    for i in 0..200 {
        let channel_id = i % 4;
        let payload = format!("channel {} message {}", channel_id, i);
        write_message(&mut file, MSG_KIND_ARROW_MSG, payload.as_bytes())?;
    }

    write_message(&mut file, MSG_KIND_END, &[])?;
    write_rrf2_footer(&mut file)?;

    println!("  Created: large_multichannel.rrd (200 messages)");
    Ok(())
}

fn generate_with_timestamps(out_dir: &Path) -> std::io::Result<()> {
    let path = out_dir.join("timestamps.rrd");
    let mut file = fs::File::create(&path)?;

    write_rrf2_header(&mut file)?;

    // Write 25 messages with timestamp-like data
    for i in 0..25 {
        let payload = format!("timestamped message {}", i);
        write_message(&mut file, MSG_KIND_ARROW_MSG, payload.as_bytes())?;
    }

    write_message(&mut file, MSG_KIND_END, &[])?;
    write_rrf2_footer(&mut file)?;

    println!("  Created: timestamps.rrd (25 messages)");
    Ok(())
}
