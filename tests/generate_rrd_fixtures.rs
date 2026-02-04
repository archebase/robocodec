// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Test fixture generator for RRD files.
//!
//! Run with: cargo run --bin generate_rrd_fixtures

use std::fs;
use std::path::Path;

use robocodec::FormatWriter;
use robocodec::core::Result;
use robocodec::io::formats::rrd::writer::RrdWriter;
use robocodec::io::metadata::RawMessage;

/// Generate RRD test fixtures in the tests/fixtures/rrd directory.
fn main() -> Result<()> {
    let out_dir = Path::new("tests/fixtures/rrd");
    fs::create_dir_all(out_dir)?;

    println!("Generating RRD test fixtures...");

    // 1. Small uncompressed RRD file
    generate_small_uncompressed(out_dir)?;

    // 2. Medium LZ4-compressed RRD file
    generate_medium_lz4(out_dir)?;

    // 3. Large RRD file with multiple channels
    generate_large_multichannel(out_dir)?;

    // 4. RRD with timestamps
    generate_with_timestamps(out_dir)?;

    println!("All RRD fixtures generated successfully!");
    Ok(())
}

fn generate_small_uncompressed(out_dir: &Path) -> Result<()> {
    let path = out_dir.join("small_uncompressed.rrd");
    let mut writer = RrdWriter::create(&path)?;

    let channel_id = writer.add_channel("/test_topic", "std_msgs/String", "json", None)?;

    for i in 0..10 {
        let data = format!("message {}", i);
        let message = RawMessage {
            channel_id,
            log_time: 1000 + i as u64,
            publish_time: 1000 + i as u64,
            data: data.into_bytes(),
            sequence: Some(i as u64),
        };
        writer.write(&message)?;
    }

    writer.finish()?;
    println!("  Created: small_uncompressed.rrd (10 messages, uncompressed)");
    Ok(())
}

fn generate_medium_lz4(out_dir: &Path) -> Result<()> {
    let path = out_dir.join("medium_lz4.rrd");
    let mut writer = RrdWriter::create(&path)?;

    let channel_id = writer.add_channel("/sensor/data", "sensor_msgs/Imu", "cdr", None)?;

    for i in 0..100 {
        let mut data = vec![0u8; 100];
        data[0..4].copy_from_slice(&(i as u32).to_le_bytes());
        data[4..8].copy_from_slice(&((i * 2) as u32).to_le_bytes());

        let message = RawMessage {
            channel_id,
            log_time: 1_000_000_000 + i as u64 * 1_000_000,
            publish_time: 1_000_000_000 + i as u64 * 1_000_000,
            data,
            sequence: Some(i as u64),
        };
        writer.write(&message)?;
    }

    writer.finish()?;
    println!("  Created: medium_lz4.rrd (100 messages)");
    Ok(())
}

fn generate_large_multichannel(out_dir: &Path) -> Result<()> {
    let path = out_dir.join("large_multichannel.rrd");
    let mut writer = RrdWriter::create(&path)?;

    // Create multiple channels
    let channels = [
        ("/camera/image_raw", "sensor_msgs/Image", "cdr"),
        ("/lidar/points", "sensor_msgs/PointCloud2", "cdr"),
        ("/odom", "nav_msgs/Odometry", "cdr"),
        ("/cmd_vel", "geometry_msgs/Twist", "cdr"),
    ];

    let channel_ids: Vec<u16> = channels
        .iter()
        .map(|(topic, msg_type, encoding)| writer.add_channel(topic, msg_type, encoding, None))
        .collect::<Result<Vec<_>>>()?;

    // Write messages to each channel
    for (idx, &channel_id) in channel_ids.iter().enumerate() {
        for i in 0..50 {
            let mut data = vec![0u8; 50 + (idx * 10)];
            data[0] = idx as u8;
            data[1] = i as u8;

            let message = RawMessage {
                channel_id,
                log_time: (idx * 1_000_000_000) as u64 + i as u64 * 10_000_000,
                publish_time: (idx * 1_000_000_000) as u64 + i as u64 * 10_000_000,
                data,
                sequence: Some(i as u64),
            };
            writer.write(&message)?;
        }
    }

    writer.finish()?;
    println!("  Created: large_multichannel.rrd (4 channels, 200 messages total)");
    Ok(())
}

fn generate_with_timestamps(out_dir: &Path) -> Result<()> {
    let path = out_dir.join("timestamps.rrd");
    let mut writer = RrdWriter::create(&path)?;

    let channel_id = writer.add_channel("/trajectory", "geometry_msgs/PoseStamped", "cdr", None)?;

    // Create a trajectory with realistic timestamps
    let base_time = 1_700_000_000_000_000_000u64; // 2023 in nanoseconds

    for i in 0..25 {
        let mut data = vec![0u8; 32];
        // Position x, y, z
        data[0..8].copy_from_slice(&(i as f64 * 0.1).to_le_bytes());
        data[8..16].copy_from_slice(&((i as f64 * 0.1).sin()).to_le_bytes());
        data[16..24].copy_from_slice(&((i as f64 * 0.1).cos()).to_le_bytes());

        let message = RawMessage {
            channel_id,
            log_time: base_time + (i as u64 * 100_000_000), // 100ms intervals
            publish_time: base_time + (i as u64 * 100_000_000),
            data,
            sequence: Some(i as u64),
        };
        writer.write(&message)?;
    }

    writer.finish()?;
    println!("  Created: timestamps.rrd (25 messages with nanosecond timestamps)");
    Ok(())
}
