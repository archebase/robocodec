// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! CLI integration tests.
//!
//! These tests run the actual robocodec binary and verify its behavior.

use std::{
    path::PathBuf,
    process::{Command, Output},
};

/// Get the path to the built robocodec binary
fn robocodec_bin() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    // The test binary is in target/debug/deps/
    // The robocodec binary is in target/debug/
    path.pop(); // deps
    path.pop(); // debug or release
    path.push("robocodec");
    path
}

/// Get the path to a test fixture file
fn fixture_path(name: &str) -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("fixtures")
        .join(name)
}

/// Run robocodec with arguments
fn run(args: &[&str]) -> Output {
    let bin = robocodec_bin();
    Command::new(&bin)
        .args(args)
        .output()
        .unwrap_or_else(|_| panic!("Failed to run {:?}", bin))
}

/// Run robocodec and assert success
fn run_ok(args: &[&str]) -> String {
    let output = run(args);
    assert!(
        output.status.success(),
        "Command failed: {:?}\nstdout: {}\nstderr: {}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Run robocodec and assert failure
fn run_err(args: &[&str]) -> String {
    let output = run(args);
    assert!(
        !output.status.success(),
        "Command should have failed but succeeded: {:?}",
        args
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

// ============================================================================
// Basic CLI Tests
// ============================================================================

#[test]
fn test_cli_help() {
    let output = run_ok(&["--help"]);
    assert!(output.contains("Robotics data format toolkit"));
    assert!(output.contains("Inspect"));
    assert!(output.contains("Convert"));
    assert!(output.contains("Extract"));
    assert!(output.contains("Search"));
    assert!(output.contains("Schema"));
}

#[test]
fn test_cli_version() {
    let output = run_ok(&["--version"]);
    assert!(output.contains("robocodec"));
}

#[test]
fn test_cli_no_args() {
    // Running without arguments shows help but exits with error code
    let output = run(&[]);
    // Clap shows help when no subcommand is provided, but exits with 1
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("Usage:")
            || String::from_utf8_lossy(&output.stdout).contains("USAGE")
            || String::from_utf8_lossy(&output.stderr).contains("Usage:")
            || String::from_utf8_lossy(&output.stderr).contains("USAGE")
    );
}

#[test]
fn test_cli_invalid_subcommand() {
    let stderr = run_err(&["nonexistent"]);
    assert!(stderr.contains("unrecognized") || stderr.contains("unknown"));
}

// ============================================================================
// Inspect Info Tests
// ============================================================================

#[test]
fn test_inspect_info_mcap() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "info", &path_str]);

    assert!(output.contains("Format:"));
    assert!(output.contains("Channels:"));
}

#[test]
fn test_inspect_info_bag() {
    let path = fixture_path("robocodec_test_15.bag");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "info", &path_str]);

    assert!(output.contains("Format:"));
    assert!(output.contains("Bag"));
}

#[test]
fn test_inspect_info_nonexistent_file() {
    let stderr = run_err(&["inspect", "info", "/nonexistent/file.mcap"]);
    assert!(stderr.contains("Error"));
}

// ============================================================================
// Inspect Topics Tests
// ============================================================================

#[test]
fn test_inspect_topics() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "topics", &path_str]);

    assert!(output.contains("Topics in"));
}

#[test]
fn test_inspect_topics_with_filter() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let _output = run_ok(&["inspect", "topics", &path_str, "--filter", "tf"]);

    // Filter should work - output may be empty or show filtered topics
}

#[test]
fn test_inspect_topics_with_counts() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "topics", &path_str, "--counts"]);

    assert!(output.contains("Messages:"));
}

// ============================================================================
// Inspect Schema Tests
// ============================================================================

#[test]
fn test_inspect_schema() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "schema", &path_str]);

    // Should show at least one schema
    assert!(output.contains("===") || output.contains("Topic:"));
}

#[test]
fn test_inspect_schema_with_filter() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let _output = run_ok(&["inspect", "schema", &path_str, "Point"]);

    // Should filter results
}

// ============================================================================
// Inspect Stats Tests
// ============================================================================

#[test]
fn test_inspect_stats() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let output = run_ok(&["inspect", "stats", &path_str]);

    assert!(output.contains("Statistics for"));
    assert!(output.contains("Total messages:"));
    assert!(output.contains("Channels:"));
}

// ============================================================================
// Convert Command Tests
// ============================================================================

#[test]
fn test_convert_help() {
    let output = run_ok(&["convert", "--help"]);
    assert!(output.contains("Convert between formats"));
    assert!(output.contains("to-mcap") || output.contains("to-bag"));
}

#[test]
fn test_convert_bag_to_mcap() {
    let input = fixture_path("robocodec_test_15.bag");
    if !input.exists() {
        return;
    }

    let output_path = std::env::temp_dir().join("test_cli_convert.mcap");
    let _guard = TempGuard(output_path.clone());

    let input_str = input.to_string_lossy().to_string();
    let output_str = output_path.to_string_lossy().to_string();

    let _output = run_ok(&["convert", "to-mcap", &input_str, &output_str]);

    // Should create output file
    assert!(output_path.exists(), "Output file should be created");
}

#[test]
fn test_convert_nonexistent_input() {
    let stderr = run_err(&[
        "convert",
        "to-mcap",
        "/nonexistent/input.bag",
        "/tmp/output.mcap",
    ]);
    assert!(stderr.contains("Error"));
}

// ============================================================================
// Extract Command Tests
// ============================================================================

#[test]
fn test_extract_help() {
    let output = run_ok(&["extract", "--help"]);
    assert!(output.contains("Extract subsets of data"));
    assert!(output.contains("topics"));
}

#[test]
fn test_extract_topics() {
    let input = fixture_path("robocodec_test_0.mcap");
    if !input.exists() {
        return;
    }

    let output_path = std::env::temp_dir().join("test_cli_extract.mcap");
    let _guard = TempGuard(output_path.clone());

    let input_str = input.to_string_lossy().to_string();
    let _output_str = output_path.to_string_lossy().to_string();

    // Extract first topic (need to know a topic name first)
    let _output = run_ok(&["inspect", "topics", &input_str]);
    // For now, just verify the command doesn't crash
    // Actual extraction would require knowing a valid topic name
}

// ============================================================================
// Search Command Tests
// ============================================================================

#[test]
fn test_search_help() {
    let _output = run_ok(&["search", "--help"]);
    // Verify help is available
}

// ============================================================================
// Schema Command Tests
// ============================================================================

#[test]
fn test_schema_help() {
    let output = run_ok(&["schema", "--help"]);
    assert!(output.contains("Schema operations"));
    assert!(output.contains("list"));
}

#[test]
fn test_schema_list() {
    let path = fixture_path("robocodec_test_0.mcap");
    if !path.exists() {
        return;
    }

    let path_str = path.to_string_lossy().to_string();
    let _output = run_ok(&["schema", "list", &path_str]);

    // Should list schemas
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_missing_required_arg() {
    let stderr = run_err(&["inspect", "info"]);
    assert!(stderr.contains("required") || stderr.contains("missing") || stderr.contains("usage"));
}

#[test]
fn test_invalid_file_format() {
    // Create a temporary invalid file
    let temp_file = std::env::temp_dir().join("invalid_test.mcap");
    std::fs::write(&temp_file, b"invalid magic bytes").ok();
    let _guard = TempGuard(temp_file.clone());

    let path_str = temp_file.to_string_lossy().to_string();
    let stderr = run_err(&["inspect", "info", &path_str]);

    assert!(stderr.contains("Error") || stderr.contains("Failed"));
}

// ============================================================================
// Multiple File Format Tests
// ============================================================================

#[test]
fn test_inspect_multiple_formats() {
    let mcap_path = fixture_path("robocodec_test_0.mcap");
    let bag_path = fixture_path("robocodec_test_15.bag");

    let mut tested = 0;

    if mcap_path.exists() {
        let path_str = mcap_path.to_string_lossy().to_string();
        let output = run_ok(&["inspect", "info", &path_str]);
        assert!(output.contains("Channels:"));
        tested += 1;
    }

    if bag_path.exists() {
        let path_str = bag_path.to_string_lossy().to_string();
        let output = run_ok(&["inspect", "info", &path_str]);
        assert!(output.contains("Channels:"));
        tested += 1;
    }

    assert!(tested > 0, "At least one fixture should exist");
}

// ============================================================================
// Cleanup Guard
// ============================================================================

struct TempGuard(PathBuf);

impl Drop for TempGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
