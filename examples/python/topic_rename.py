#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""topic_rename: Rename topics and message types using the fluent TransformBuilder API.

This example demonstrates the fluent API for transforming robotics data:
- Chain multiple transformations together
- Rename individual topics
- Use wildcard patterns for bulk topic renaming
- Rename message types
- Apply topic-specific type renames

The fluent API allows you to build complex transformation pipelines
with clean, readable method chaining.
"""

import sys
import json

import robocodec
from robocodec import (
    RoboReader,
    RoboRewriter,
    TransformBuilder,
    RobocodecError,
)


def print_separator(char: str = "=") -> None:
    """Print a separator line."""
    print(char * 60)


def show_transformation_summary(input_path: str, output_path: str, stats) -> None:
    """Print a summary of the transformation."""
    print()
    print_separator()
    print("📉 Transformation Summary")
    print_separator()
    print(f"  Input:  {input_path}")
    print(f"  Output: {output_path}")
    print()
    print("  Statistics:")
    print(f"    Messages processed:   {stats.message_count:,}")
    print(f"    Topics renamed:       {stats.topics_renamed}")
    print(f"    Types renamed:        {stats.types_renamed}")
    print(f"    Re-encoded messages:  {stats.reencoded_count:,}")
    print(f"    Passthrough messages: {stats.passthrough_count:,}")

    if stats.decode_failures > 0:
        print(f"    ⚠️  Decode failures:     {stats.decode_failures}")
    if stats.encode_failures > 0:
        print(f"    ⚠️  Encode failures:     {stats.encode_failures}")


def example_simple_rename(input_path: str, output_path: str) -> None:
    """Example 1: Simple topic rename."""
    print("\n🔄 Example 1: Simple Topic Rename")
    print_separator("-")

    # Build a simple transformation pipeline
    builder = (
        TransformBuilder()
        .with_topic_rename("/old_camera/image_raw", "/camera/image_rect")
        .with_topic_rename("/old_camera/camera_info", "/camera/camera_info")
    )

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    print("  Renamed topics:")
    print("    /old_camera/image_raw → /camera/image_rect")
    print("    /old_camera/camera_info → /camera/camera_info")

    show_transformation_summary(input_path, output_path, stats)


def example_wildcard_rename(input_path: str, output_path: str) -> None:
    """Example 2: Wildcard-based topic renaming."""
    print("\n🔄 Example 2: Wildcard Topic Rename")
    print_separator("-")

    # Use wildcards to rename multiple topics at once
    builder = (
        TransformBuilder()
        # Rename all topics under /robot1/ to /robot/
        .with_topic_rename_wildcard("/robot1/*", "/robot/*")
        # Rename all topics under /old/sensors/ to /sensors/
        .with_topic_rename_wildcard("/old/sensors/*", "/sensors/*")
    )

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    print("  Wildcard patterns:")
    print("    /robot1/* → /robot/*")
    print("    /old/sensors/* → /sensors/*")

    show_transformation_summary(input_path, output_path, stats)


def example_type_rename(input_path: str, output_path: str) -> None:
    """Example 3: Message type renaming."""
    print("\n🔄 Example 3: Message Type Rename")
    print_separator("-")

    # Rename message types (useful for package migrations)
    builder = (
        TransformBuilder()
        .with_type_rename("old_msgs/Point", "geometry_msgs/Point")
        .with_type_rename("old_msgs/Pose", "geometry_msgs/Pose")
        # Wildcard type renaming
        .with_type_rename_wildcard("legacy_msgs/*", "modern_msgs/*")
    )

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    print("  Type renames:")
    print("    old_msgs/Point → geometry_msgs/Point")
    print("    old_msgs/Pose → geometry_msgs/Pose")
    print("    legacy_msgs/* → modern_msgs/*")

    show_transformation_summary(input_path, output_path, stats)


def example_topic_type_rename(input_path: str, output_path: str) -> None:
    """Example 4: Topic-specific type renaming."""
    print("\n🔄 Example 4: Topic-Specific Type Rename")
    print_separator("-")

    # Change the message type for a specific topic only
    builder = (
        TransformBuilder()
        # Only this topic gets a different message type
        .with_topic_type_rename(
            "/custom/imu",
            "old_imu_msgs/ImuData",
            "sensor_msgs/Imu"
        )
    )

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    print("  Topic-specific type rename:")
    print("    /custom/imu: old_imu_msgs/ImuData → sensor_msgs/Imu")

    show_transformation_summary(input_path, output_path, stats)


def example_complex_pipeline(input_path: str, output_path: str) -> None:
    """Example 5: Complex transformation pipeline."""
    print("\n🔄 Example 5: Complex Transformation Pipeline")
    print_separator("-")

    # Combine multiple transformation types
    builder = (
        TransformBuilder()
        # Simple topic renames
        .with_topic_rename("/cam1/image_raw", "/camera/image_color")
        .with_topic_rename("/cam1/camera_info", "/camera/camera_info")
        # Wildcard topic renames
        .with_topic_rename_wildcard("/robot_front/*", "/front/*")
        .with_topic_rename_wildcard("/robot_back/*", "/back/*")
        # Type renames
        .with_type_rename("custom_pkg/Vector3", "geometry_msgs/Vector3")
        .with_type_rename("custom_pkg/Quaternion", "geometry_msgs/Quaternion")
        # Topic-specific type rename
        .with_topic_type_rename(
            "/gps/fix",
            "old_gps/GPSFix",
            "sensor_msgs/NavSatFix"
        )
    )

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    print("  Combined transformations:")
    print("    Topic renames: 4")
    print("    Type renames: 3")
    print("    Wildcard patterns: 2")

    show_transformation_summary(input_path, output_path, stats)


def example_custom_config(input_path: str, output_path: str, config_file: str) -> None:
    """Example 6: Load transformations from a JSON config file."""
    print("\n🔄 Example 6: Custom Configuration from JSON")
    print_separator("-")

    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  ❌ Error loading config: {e}")
        return

    builder = TransformBuilder()

    # Apply topic renames from config
    for rename in config.get("topic_renames", []):
        builder = builder.with_topic_rename(rename["from"], rename["to"])
        print(f"    Topic: {rename['from']} → {rename['to']}")

    # Apply wildcard topic renames
    for rename in config.get("topic_wildcards", []):
        builder = builder.with_topic_rename_wildcard(rename["pattern"], rename["target"])
        print(f"    Wildcard: {rename['pattern']} → {rename['target']}")

    # Apply type renames
    for rename in config.get("type_renames", []):
        builder = builder.with_type_rename(rename["from"], rename["to"])
        print(f"    Type: {rename['from']} → {rename['to']}")

    # Apply topic-specific type renames
    for rename in config.get("topic_type_renames", []):
        builder = builder.with_topic_type_rename(
            rename["topic"],
            rename["from_type"],
            rename["to_type"]
        )
        print(f"    Topic+Type: {rename['topic']} ({rename['from_type']} → {rename['to_type']})")

    rewriter = RoboRewriter.with_transforms(input_path, builder)
    stats = rewriter.rewrite(output_path)

    show_transformation_summary(input_path, output_path, stats)


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 3:
        print("Usage: python topic_rename.py <input_file> <output_file> [example] [config.json]")
        print()
        print("Transform robotics data using the fluent TransformBuilder API.")
        print()
        print("Examples:")
        print("  1 - Simple topic rename")
        print("  2 - Wildcard topic rename")
        print("  3 - Message type rename")
        print("  4 - Topic-specific type rename")
        print("  5 - Complex transformation pipeline")
        print("  6 - Custom configuration from JSON file")
        print()
        print("If no example is specified, example 1 is used.")
        print()
        print("Usage examples:")
        print("  python topic_rename.py data.mcap output.mcap")
        print("  python topic_rename.py data.mcap output.mcap 2")
        print("  python topic_rename.py data.mcap output.mcap 6 config.json")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Select which example to run
    example = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    print(f"📦 Robocodec TransformBuilder Example {example}")
    print_separator()

    try:
        if example == 1:
            example_simple_rename(input_path, output_path)
        elif example == 2:
            example_wildcard_rename(input_path, output_path)
        elif example == 3:
            example_type_rename(input_path, output_path)
        elif example == 4:
            example_topic_type_rename(input_path, output_path)
        elif example == 5:
            example_complex_pipeline(input_path, output_path)
        elif example == 6:
            if len(sys.argv) < 5:
                print("❌ Error: Example 6 requires a config.json file path")
                sys.exit(1)
            example_custom_config(input_path, output_path, sys.argv[4])
        else:
            print(f"❌ Error: Invalid example number: {example}")
            sys.exit(1)
    except RobocodecError as e:
        print(f"\n❌ Transformation failed: {e}")
        print(f"   Kind: {e.kind}")
        if e.context:
            print(f"   Context: {e.context}")
        sys.exit(1)

    print()
    print_separator()
    print("✅ Done!")


if __name__ == "__main__":
    main()
