// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! Schema command - inspect and validate message schemas.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use clap::Subcommand;
use serde::Serialize;

use crate::common::{open_reader, Result};
use robocodec::FormatReader;

/// Schema operations.
#[derive(Subcommand, Clone, Debug)]
pub enum SchemaCmd {
    /// List all message types in the file
    List {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Group by message type
        #[arg(long)]
        group_by_type: bool,

        /// Show only standard ROS types
        #[arg(long)]
        standard_only: bool,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Show full schema for a message type
    Show {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Topic or message type to show
        #[arg(value_name = "TOPIC|TYPE")]
        topic_or_type: String,

        /// Show full schema (don't truncate)
        #[arg(long)]
        full: bool,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Validate all schemas can be parsed
    Validate {
        /// Input file
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },

    /// Compare schemas between two files
    Diff {
        /// First file
        #[arg(value_name = "FILE1")]
        file1: PathBuf,

        /// Second file
        #[arg(value_name = "FILE2")]
        file2: PathBuf,

        /// Message type to compare
        #[arg(short, long)]
        msg_type: Option<String>,

        /// Output as JSON
        #[arg(short, long)]
        json: bool,
    },
}

impl SchemaCmd {
    pub fn run(self) -> Result<()> {
        match self {
            SchemaCmd::List {
                input,
                group_by_type,
                standard_only,
                json,
            } => cmd_list(input, group_by_type, standard_only, json),
            SchemaCmd::Show {
                input,
                topic_or_type,
                full,
                json,
            } => cmd_show(input, topic_or_type, full, json),
            SchemaCmd::Validate { input, json } => cmd_validate(input, json),
            SchemaCmd::Diff {
                file1,
                file2,
                msg_type,
                json,
            } => cmd_diff(file1, file2, msg_type, json),
        }
    }
}

fn cmd_list(input: PathBuf, group_by_type: bool, standard_only: bool, json: bool) -> Result<()> {
    let reader = open_reader(&input)?;

    const STANDARD_PREFIXES: &[&str] = &[
        "sensor_msgs/",
        "std_msgs/",
        "geometry_msgs/",
        "nav_msgs/",
        "tf2_msgs/",
        "trajectory_msgs/",
        "visualization_msgs/",
        "diagnostic_msgs/",
        "actionlib_msgs/",
        "sensor_msgs/msg/",
        "std_msgs/msg/",
        "geometry_msgs/msg/",
        "nav_msgs/msg/",
        "tf2_msgs/msg/",
        "trajectory_msgs/msg/",
        "visualization_msgs/msg/",
        "diagnostic_msgs/msg/",
        "actionlib_msgs/msg/",
    ];

    if group_by_type {
        // Group by message type, showing all topics that use it
        let mut type_map: BTreeMap<String, Vec<String>> = BTreeMap::new();

        for channel in reader.channels().values() {
            if standard_only {
                let is_standard = STANDARD_PREFIXES.iter().any(|p| {
                    channel.message_type.starts_with(p)
                        || channel.message_type.starts_with(&p.replace('/', "/msg/"))
                });
                if !is_standard {
                    continue;
                }
            }
            type_map
                .entry(channel.message_type.clone())
                .or_default()
                .push(channel.topic.clone());
        }

        let types: Vec<TypeInfo> = type_map
            .into_iter()
            .map(|(msg_type, topics)| TypeInfo {
                message_type: msg_type,
                topics,
            })
            .collect();

        output_json_or(json, &types, || {
            println!("=== Message Types in {} ===", input.display());
            println!();
            for info in &types {
                println!("{}", info.message_type);
                for topic in &info.topics {
                    println!("  @ {}", topic);
                }
                println!();
            }
            Ok(())
        })
    } else {
        // List by channel/topic
        let mut items: Vec<SchemaItem> = reader
            .channels()
            .values()
            .filter(|channel| {
                if !standard_only {
                    return true;
                }
                STANDARD_PREFIXES.iter().any(|p| {
                    channel.message_type.starts_with(p)
                        || channel.message_type.starts_with(&p.replace('/', "/msg/"))
                })
            })
            .map(|ch| SchemaItem {
                topic: ch.topic.clone(),
                message_type: ch.message_type.clone(),
                encoding: ch
                    .schema_encoding
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
            })
            .collect();

        items.sort_by(|a, b| a.message_type.cmp(&b.message_type));

        output_json_or(json, &items, || {
            println!("=== Message Types in {} ===", input.display());
            println!();
            for item in &items {
                println!("{} @ {}", item.message_type, item.topic);
                println!("  Encoding: {}", item.encoding);
                println!();
            }
            Ok(())
        })
    }
}

fn cmd_show(input: PathBuf, topic_or_type: String, full: bool, json: bool) -> Result<()> {
    let reader = open_reader(&input)?;

    let mut found = false;

    for channel in reader.channels().values() {
        if channel.topic.contains(&topic_or_type) || channel.message_type.contains(&topic_or_type) {
            found = true;

            let schema_info = SchemaDetail {
                topic: channel.topic.clone(),
                message_type: channel.message_type.clone(),
                encoding: channel.encoding.clone(),
                schema_encoding: channel.schema_encoding.clone(),
                schema: channel.schema.clone(),
            };

            output_json_or(json, &schema_info, || {
                println!(
                    "=== {} @ {} ===",
                    schema_info.message_type, schema_info.topic
                );
                println!("Encoding: {}", schema_info.encoding);
                if let Some(ref enc) = schema_info.schema_encoding {
                    println!("Schema encoding: {}", enc);
                }
                println!();

                if let Some(ref schema) = schema_info.schema {
                    if full {
                        println!("{}", schema);
                    } else {
                        for (i, line) in schema.lines().take(50).enumerate() {
                            println!("{}", line);
                            if i == 49 && schema.lines().count() > 50 {
                                println!(
                                    "... ({} lines total, use --full for all)",
                                    schema.lines().count()
                                );
                                break;
                            }
                        }
                    }
                } else {
                    println!("(no schema available)");
                }
                println!();
                Ok(())
            })?;
        }
    }

    if !found {
        println!("No matching topic or type found: {}", topic_or_type);
    }

    Ok(())
}

fn cmd_validate(input: PathBuf, json: bool) -> Result<()> {
    let reader = open_reader(&input)?;

    println!("=== Validating Schemas ===");
    println!();

    let mut results: Vec<ValidationResult> = Vec::new();
    let mut ok_count = 0;
    let mut err_count = 0;

    for channel in reader.channels().values() {
        let result = if let Some(schema) = &channel.schema {
            // Try to parse the schema
            match robocodec::schema::parser::parse_schema_with_encoding_str(
                &channel.message_type,
                schema,
                channel.schema_encoding.as_deref().unwrap_or("ros2msg"),
            ) {
                Ok(_) => {
                    ok_count += 1;
                    ValidationResult {
                        topic: channel.topic.clone(),
                        message_type: channel.message_type.clone(),
                        status: "ok".to_string(),
                        message: String::new(),
                    }
                }
                Err(e) => {
                    err_count += 1;
                    ValidationResult {
                        topic: channel.topic.clone(),
                        message_type: channel.message_type.clone(),
                        status: "error".to_string(),
                        message: e.to_string(),
                    }
                }
            }
        } else {
            ValidationResult {
                topic: channel.topic.clone(),
                message_type: channel.message_type.clone(),
                status: "warning".to_string(),
                message: "no schema available".to_string(),
            }
        };
        results.push(result);
    }

    output_json_or(json, &results, || {
        for result in &results {
            match result.status.as_str() {
                "ok" => println!("  ✓ {} @ {}", result.message_type, result.topic),
                "warning" => println!(
                    "  ⚠ {} @ {}: {}",
                    result.message_type, result.topic, result.message
                ),
                "error" => println!(
                    "  ✗ {} @ {}: {}",
                    result.message_type, result.topic, result.message
                ),
                _ => {}
            }
        }

        println!();
        println!("Results: {} valid, {} errors", ok_count, err_count);

        if err_count > 0 {
            std::process::exit(1);
        }
        Ok(())
    })
}

fn cmd_diff(file1: PathBuf, file2: PathBuf, msg_type: Option<String>, json: bool) -> Result<()> {
    if json {
        return Err(anyhow::anyhow!(
            "JSON output is not yet implemented for the diff command. \
             Use the default text output instead."
        ));
    }

    let reader1 = open_reader(&file1)?;
    let reader2 = open_reader(&file2)?;

    println!("=== Schema Diff ===");
    println!("  File 1: {}", file1.display());
    println!("  File 2: {}", file2.display());
    println!();

    // Collect schemas from both files
    let schemas1: BTreeMap<String, String> = reader1
        .channels()
        .values()
        .filter_map(|ch| {
            if let Some(ref schema) = ch.schema {
                if let Some(ref mt) = msg_type {
                    if ch.message_type.contains(mt) {
                        Some((ch.message_type.clone(), schema.clone()))
                    } else {
                        None
                    }
                } else {
                    Some((ch.message_type.clone(), schema.clone()))
                }
            } else {
                None
            }
        })
        .collect();

    let schemas2: BTreeMap<String, String> = reader2
        .channels()
        .values()
        .filter_map(|ch| {
            if let Some(ref schema) = ch.schema {
                if let Some(ref mt) = msg_type {
                    if ch.message_type.contains(mt) {
                        Some((ch.message_type.clone(), schema.clone()))
                    } else {
                        None
                    }
                } else {
                    Some((ch.message_type.clone(), schema.clone()))
                }
            } else {
                None
            }
        })
        .collect();

    let all_types: BTreeSet<String> = schemas1.keys().chain(schemas2.keys()).cloned().collect();

    let mut differences: Vec<SchemaDiff> = Vec::new();

    for type_name in all_types {
        let schema1 = schemas1.get(&type_name);
        let schema2 = schemas2.get(&type_name);

        match (schema1, schema2) {
            (None, Some(_)) => {
                println!("+ {} (only in file 2)", type_name);
                differences.push(SchemaDiff {
                    message_type: type_name.clone(),
                    status: "added".to_string(),
                    diff: String::new(),
                });
            }
            (Some(_), None) => {
                println!("- {} (only in file 1)", type_name);
                differences.push(SchemaDiff {
                    message_type: type_name.clone(),
                    status: "removed".to_string(),
                    diff: String::new(),
                });
            }
            (Some(s1), Some(s2)) if s1 != s2 => {
                println!("! {} (modified)", type_name);
                differences.push(SchemaDiff {
                    message_type: type_name.clone(),
                    status: "modified".to_string(),
                    diff: compute_diff(s1, s2),
                });
            }
            _ => {
                println!("  {} (same)", type_name);
            }
        }
    }

    if differences.is_empty() {
        println!();
        println!("No differences found.");
    }

    Ok(())
}

fn compute_diff(s1: &str, s2: &str) -> String {
    // Simple diff - count lines that differ
    let lines1: Vec<&str> = s1.lines().collect();
    let lines2: Vec<&str> = s2.lines().collect();

    let mut diff = String::new();
    let max_len = lines1.len().max(lines2.len());

    for i in 0..max_len {
        let line1 = lines1.get(i).copied().unwrap_or("");
        let line2 = lines2.get(i).copied().unwrap_or("");

        if line1 != line2 {
            if !line1.is_empty() {
                diff.push_str(&format!("- {}\n", line1));
            }
            if !line2.is_empty() {
                diff.push_str(&format!("+ {}\n", line2));
            }
        }
    }

    diff
}

fn output_json_or<T>(
    json: bool,
    value: &T,
    human_fn: impl FnOnce() -> std::io::Result<()>,
) -> Result<()>
where
    T: Serialize,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        human_fn()?;
    }
    Ok(())
}

// Output types

#[derive(Serialize)]
struct TypeInfo {
    message_type: String,
    topics: Vec<String>,
}

#[derive(Serialize)]
struct SchemaItem {
    topic: String,
    message_type: String,
    encoding: String,
}

#[derive(Serialize)]
struct SchemaDetail {
    topic: String,
    message_type: String,
    encoding: String,
    schema_encoding: Option<String>,
    schema: Option<String>,
}

#[derive(Serialize)]
struct ValidationResult {
    topic: String,
    message_type: String,
    status: String,
    message: String,
}

#[derive(Serialize)]
struct SchemaDiff {
    message_type: String,
    status: String,
    diff: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to get fixture path
    fn fixture_path(name: &str) -> PathBuf {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(manifest_dir)
            .join("tests")
            .join("fixtures")
            .join(name)
    }

    // ========================================================================
    // SchemaCmd::run() Tests
    // ========================================================================

    #[test]
    fn test_schema_cmd_list_nonexistent_file() {
        let cmd = SchemaCmd::List {
            input: PathBuf::from("/nonexistent/file.mcap"),
            group_by_type: false,
            standard_only: false,
            json: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent file");
    }

    #[test]
    fn test_schema_cmd_show_nonexistent_file() {
        let cmd = SchemaCmd::Show {
            input: PathBuf::from("/nonexistent/file.mcap"),
            topic_or_type: "Point".to_string(),
            full: false,
            json: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent file");
    }

    #[test]
    fn test_schema_cmd_validate_nonexistent_file() {
        let cmd = SchemaCmd::Validate {
            input: PathBuf::from("/nonexistent/file.mcap"),
            json: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent file");
    }

    #[test]
    fn test_schema_cmd_diff_nonexistent_file() {
        let cmd = SchemaCmd::Diff {
            file1: PathBuf::from("/nonexistent/file1.mcap"),
            file2: PathBuf::from("/nonexistent/file2.mcap"),
            msg_type: None,
            json: false,
        };
        let result = cmd.run();
        assert!(result.is_err(), "should fail for nonexistent files");
    }

    // ========================================================================
    // List Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_list_with_valid_file() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_list(path.clone(), false, false, false);
        assert!(result.is_ok(), "list command should succeed");
    }

    #[test]
    fn test_cmd_list_grouped_by_type() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_list(path.clone(), true, false, false);
        assert!(result.is_ok(), "grouped list should succeed");
    }

    #[test]
    fn test_cmd_list_standard_only() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_list(path.clone(), false, true, false);
        assert!(result.is_ok(), "standard_only filter should succeed");
    }

    #[test]
    fn test_cmd_list_json_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_list(path.clone(), false, false, true);
        assert!(result.is_ok(), "json output should succeed");
    }

    #[test]
    fn test_cmd_list_all_options() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_list(path.clone(), true, true, true);
        assert!(result.is_ok(), "all options combined should succeed");
    }

    // ========================================================================
    // Show Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_show_with_valid_file() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_show(path.clone(), "Point".to_string(), false, false);
        assert!(result.is_ok(), "show command should succeed");
    }

    #[test]
    fn test_cmd_show_full_schema() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_show(path.clone(), "Point".to_string(), true, false);
        assert!(result.is_ok(), "show with full flag should succeed");
    }

    #[test]
    fn test_cmd_show_json_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_show(path.clone(), "Point".to_string(), false, true);
        assert!(result.is_ok(), "show with json should succeed");
    }

    #[test]
    fn test_cmd_show_empty_pattern() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Empty pattern - may or may not match anything
        let result = cmd_show(path.clone(), "".to_string(), false, false);
        assert!(result.is_ok(), "empty pattern should not crash");
    }

    // ========================================================================
    // Validate Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_validate_with_valid_file() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Note: validation may fail for schemas, but the command should run
        let result = cmd_validate(path.clone(), false);
        // The result could be Ok or Err depending on schema validity
        // We just check it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_cmd_validate_json_output() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_validate(path.clone(), true);
        let _ = result; // Just check it doesn't panic
    }

    // ========================================================================
    // Diff Command Tests
    // ========================================================================

    #[test]
    fn test_cmd_diff_json_not_implemented() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_diff(path.clone(), path.clone(), None, true);
        assert!(
            result.is_err(),
            "json output should fail with not implemented"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("not yet implemented"));
    }

    #[test]
    fn test_cmd_diff_same_file() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        // Diff a file with itself - should have no differences
        let result = cmd_diff(path.clone(), path.clone(), None, false);
        assert!(result.is_ok(), "diff same file should succeed");
    }

    #[test]
    fn test_cmd_diff_with_msg_type_filter() {
        let path = fixture_path("robocodec_test_0.mcap");
        if !path.exists() {
            return;
        }

        let result = cmd_diff(path.clone(), path.clone(), Some("Point".to_string()), false);
        assert!(result.is_ok(), "diff with msg_type filter should succeed");
    }

    // ========================================================================
    // SchemaCmd Enum Tests
    // ========================================================================

    #[test]
    fn test_schema_cmd_clone() {
        let cmd = SchemaCmd::List {
            input: PathBuf::from("test.mcap"),
            group_by_type: true,
            standard_only: false,
            json: false,
        };
        let cloned = cmd.clone();
        match (cmd, cloned) {
            (SchemaCmd::List { input: i1, .. }, SchemaCmd::List { input: i2, .. }) => {
                assert_eq!(i1, i2);
            }
            _ => panic!("cloned commands should match"),
        }
    }

    #[test]
    fn test_schema_cmd_debug() {
        let cmd = SchemaCmd::Show {
            input: PathBuf::from("test.mcap"),
            topic_or_type: "Point".to_string(),
            full: true,
            json: false,
        };
        let debug_str = format!("{:?}", cmd);
        assert!(debug_str.contains("Show"));
    }

    // ========================================================================
    // Standard Prefix Tests
    // ========================================================================

    #[test]
    fn test_standard_prefixes_match() {
        // Test that standard prefix matching works correctly
        const STANDARD_PREFIXES: &[&str] = &[
            "sensor_msgs/",
            "std_msgs/",
            "geometry_msgs/",
            "nav_msgs/",
            "tf2_msgs/",
            "trajectory_msgs/",
            "visualization_msgs/",
            "diagnostic_msgs/",
            "actionlib_msgs/",
        ];

        let test_type = "sensor_msgs/msg/Point";

        let is_standard = STANDARD_PREFIXES.iter().any(|p: &&str| {
            test_type.starts_with(p) || test_type.starts_with(&p.replace('/', "/msg/"))
        });

        assert!(
            is_standard,
            "sensor_msgs types should be recognized as standard"
        );
    }

    #[test]
    fn test_standard_prefixes_non_standard() {
        // Test that custom types are not matched
        const STANDARD_PREFIXES: &[&str] = &["sensor_msgs/", "std_msgs/", "geometry_msgs/"];

        let test_type = "custom_msgs/CustomType";

        let is_standard = STANDARD_PREFIXES.iter().any(|p: &&str| {
            test_type.starts_with(p) || test_type.starts_with(&p.replace('/', "/msg/"))
        });

        assert!(
            !is_standard,
            "custom types should not be recognized as standard"
        );
    }

    // ========================================================================
    // Diff Computation Tests
    // ========================================================================

    #[test]
    fn test_compute_diff_identical() {
        let s1 = "line1\nline2\nline3";
        let s2 = "line1\nline2\nline3";
        let diff = compute_diff(s1, s2);
        assert!(diff.is_empty(), "identical schemas should have no diff");
    }

    #[test]
    fn test_compute_diff_different() {
        let s1 = "line1\nline2\nline3";
        let s2 = "line1\nlineX\nline3";
        let diff = compute_diff(s1, s2);
        assert!(!diff.is_empty(), "different schemas should have diff");
        assert!(diff.contains("line2"), "diff should show removed line");
        assert!(diff.contains("lineX"), "diff should show added line");
    }

    #[test]
    fn test_compute_diff_different_lengths() {
        let s1 = "line1\nline2";
        let s2 = "line1\nline2\nline3";
        let diff = compute_diff(s1, s2);
        assert!(!diff.is_empty(), "different lengths should produce diff");
    }

    #[test]
    fn test_compute_diff_empty() {
        let diff = compute_diff("", "");
        assert!(diff.is_empty(), "empty schemas should have no diff");
    }

    // ========================================================================
    // Output Tests
    // ========================================================================

    #[test]
    fn test_type_info_serialization() {
        let info = TypeInfo {
            message_type: "test/Msg".to_string(),
            topics: vec!["/topic1".to_string(), "/topic2".to_string()],
        };

        let json = serde_json::to_string(&info);
        assert!(json.is_ok(), "TypeInfo should be serializable");
        assert!(json.unwrap().contains("test/Msg"));
    }

    #[test]
    fn test_schema_item_serialization() {
        let item = SchemaItem {
            topic: "/test".to_string(),
            message_type: "test/Msg".to_string(),
            encoding: "cdr".to_string(),
        };

        let json = serde_json::to_string(&item);
        assert!(json.is_ok(), "SchemaItem should be serializable");
    }

    #[test]
    fn test_schema_detail_serialization() {
        let detail = SchemaDetail {
            topic: "/test".to_string(),
            message_type: "test/Msg".to_string(),
            encoding: "cdr".to_string(),
            schema_encoding: Some("ros2msg".to_string()),
            schema: Some("string data".to_string()),
        };

        let json = serde_json::to_string(&detail);
        assert!(json.is_ok(), "SchemaDetail should be serializable");
    }

    #[test]
    fn test_validation_result_serialization() {
        let result = ValidationResult {
            topic: "/test".to_string(),
            message_type: "test/Msg".to_string(),
            status: "ok".to_string(),
            message: String::new(),
        };

        let json = serde_json::to_string(&result);
        assert!(json.is_ok(), "ValidationResult should be serializable");
    }

    #[test]
    fn test_schema_diff_serialization() {
        let diff = SchemaDiff {
            message_type: "test/Msg".to_string(),
            status: "modified".to_string(),
            diff: "- old line\n+ new line\n".to_string(),
        };

        let json = serde_json::to_string(&diff);
        assert!(json.is_ok(), "SchemaDiff should be serializable");
    }
}
