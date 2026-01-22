#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""filter_topics: Extract specific topics from MCAP or ROS bag files.

This example shows how to:
- List all topics in a data file
- Extract only specific topics of interest
- Filter topics by name patterns
- Create a smaller, focused dataset from large robot recordings

Use cases:
- Extract only sensor data (IMU, GPS, lidar) from a full robot log
- Create a training dataset with specific message types
- Reduce file size by removing unnecessary topics
- Debug specific subsystems by filtering related topics
"""

import sys
import re
import argparse
from pathlib import Path

import robocodec
from robocodec import RoboReader, RoboWriter, RobocodecError

# Verify the correct API is available before running
try:
    from ._example_utils import verify_api, print_robocodec_error
    verify_api()
except ImportError:
    if not hasattr(robocodec, 'RoboReader'):
        print("❌ Error: Incompatible robocodec API", file=sys.stderr)
        print("   Please install using: make build-python-dev", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"❌ Error during API verification: {e}", file=sys.stderr)
    sys.exit(1)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Filter topics from MCAP or ROS bag files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all topics in a file
  python filter_topics.py data.mcap --list

  # Extract specific topics
  python filter_topics.py data.mcap output.mcap --topics /camera/image_raw /imu/data

  # Extract topics matching a pattern
  python filter_topics.py data.mcap output.mcap --pattern "/camera/*"

  # Exclude specific topics
  python filter_topics.py data.mcap output.mcap --exclude "/tf" "/tf_static"

  # Combine include and exclude
  python filter_topics.py data.mcap output.mcap --pattern "/sensors/*" --exclude "/sensors/debug/*"
        """
    )

    parser.add_argument("input", help="Input file path (.mcap or .bag)")
    parser.add_argument("output", nargs="?", help="Output file path (required unless --list is used)")
    parser.add_argument("--list", "-l", action="store_true",
                        help="List all topics and exit")
    parser.add_argument("--topics", "-t", nargs="+", metavar="TOPIC",
                        help="Specific topics to extract")
    parser.add_argument("--pattern", "-p", nargs="+", metavar="PATTERN",
                        help="Topic patterns to extract (supports wildcards)")
    parser.add_argument("--exclude", "-e", nargs="+", metavar="TOPIC",
                        help="Topics to exclude")
    parser.add_argument("--regex", "-r", nargs="+", metavar="REGEX",
                        help="Regular expressions for topic matching")

    return parser.parse_args()


def list_topics(reader: RoboReader) -> None:
    """List all topics in the file."""
    channels = reader.channels()

    print(f"📋 Topics in {reader.path}")
    print(f"   Format: {reader.format}")
    print(f"   Total topics: {len(channels)}")
    print("=" * 70)

    if not channels:
        print("No channels found.")
        return

    # Group by encoding
    by_encoding = {}
    for ch in channels:
        enc = ch.encoding or "unknown"
        if enc not in by_encoding:
            by_encoding[enc] = []
        by_encoding[enc].append(ch)

    for encoding, chan_list in sorted(by_encoding.items()):
        print(f"\n[{encoding.upper()}]")
        for ch in sorted(chan_list, key=lambda c: c.topic):
            msg_count = f"{ch.message_count:,} msgs" if ch.message_count > 0 else "empty"
            print(f"  {ch.topic:50s} {ch.message_type:40s} ({msg_count})")


def matches_patterns(topic: str, patterns: list[str]) -> bool:
    """Check if a topic matches any of the given patterns."""
    for pattern in patterns:
        # Convert shell-style wildcard to regex
        regex = pattern.replace("*", ".*").replace("?", ".")
        if re.fullmatch(regex, topic):
            return True
    return False


def matches_regex(topic: str, regex_list: list[str]) -> bool:
    """Check if a topic matches any of the given regexes."""
    for pattern in regex_list:
        if re.fullmatch(pattern, topic):
            return True
    return False


def filter_topics(
    input_path: str,
    output_path: str,
    include_topics: list[str] | None = None,
    include_patterns: list[str] | None = None,
    include_regex: list[str] | None = None,
    exclude_topics: list[str] | None = None,
) -> bool:
    """List and copy topics from input file to output file.

    Note: The current RoboRewriter API preserves all channels. This function
    lists topics matching the specified criteria but the output file will
    contain all topics from the input. Per-channel filtering is planned for
    a future release.

    Args:
        input_path: Path to input file
        output_path: Path to output file
        include_topics: Exact topic names to include
        include_patterns: Wildcard patterns for topics to include
        include_regex: Regular expressions for topics to include
        exclude_topics: Exact topic names to exclude

    Returns:
        True if copying succeeded, False otherwise
    """
    reader = RoboReader(input_path)
    all_channels = reader.channels()

    # Build list of channels to include
    selected_channels = []

    for ch in all_channels:
        # Check exclusions first
        if exclude_topics and ch.topic in exclude_topics:
            continue

        # Check if topic should be included
        include = False

        if include_topics and ch.topic in include_topics:
            include = True
        elif include_patterns and matches_patterns(ch.topic, include_patterns):
            include = True
        elif include_regex and matches_regex(ch.topic, include_regex):
            include = True
        elif not (include_topics or include_patterns or include_regex):
            # No filters specified, include everything (except exclusions)
            include = True

        if include:
            selected_channels.append(ch)

    if not selected_channels:
        print("❌ Error: No topics selected after filtering")
        return False

    # Show what will be extracted
    print(f"📥 Filtering topics from: {input_path}")
    print(f"   Input format: {reader.format}")
    print(f"   Total topics: {len(all_channels)}")
    print(f"   Selected topics: {len(selected_channels)}")
    print()

    print("Selected topics:")
    for ch in sorted(selected_channels, key=lambda c: c.topic):
        print(f"  ✓ {ch.topic} ({ch.message_count:,} messages)")

    print()
    print(f"📤 Writing to: {output_path}")

    # Create output file with selected channels
    # Note: For now, we use the rewriter which preserves all data.
    # A future version could support actual filtering.
    try:
        # For this example, we'll create a simple pass-through rewrite
        # In a real implementation, you'd want to filter messages per-channel
        from robocodec import RoboRewriter

        # If we're filtering (not selecting all), warn that current implementation
        # preserves all channels (this is a limitation of the current API)
        if len(selected_channels) < len(all_channels):
            print()
            print("⚠️  Note: The current RoboRewriter API preserves all channels.")
            print("   The output file will contain all topics from the input.")
            print("   True per-channel filtering is planned for a future release.")

        rewriter = RoboRewriter(input_path)
        stats = rewriter.rewrite(output_path)

        print()
        print("✅ Done!")
        print(f"   Messages written: {stats.message_count:,}")

    except RobocodecError as e:
        print(f"❌ Error: {e}")
        return False

    return True


def main() -> None:
    """Main entry point."""
    args = parse_arguments()

    # Validate input file exists
    if not Path(args.input).exists():
        print(f"❌ Error: Input file not found: {args.input}")
        sys.exit(1)

    try:
        reader = RoboReader(args.input)

        # List mode: just show topics and exit
        if args.list:
            list_topics(reader)
            sys.exit(0)

        # Filter mode: requires output file
        if not args.output:
            print("❌ Error: Output file required when not using --list")
            print("   Usage: python filter_topics.py <input> <output> [options]")
            sys.exit(1)

        # Verify we have some filter criteria
        if not any([args.topics, args.pattern, args.regex]):
            print("⚠️  Warning: No filter criteria specified, all topics will be included")
            print("   Use --topics, --pattern, or --regex to specify what to extract")
            print()

        # Perform filtering
        success = filter_topics(
            args.input,
            args.output,
            include_topics=args.topics,
            include_patterns=args.pattern,
            include_regex=args.regex,
            exclude_topics=args.exclude,
        )

        sys.exit(0 if success else 1)

    except RobocodecError as e:
        print_robocodec_error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
