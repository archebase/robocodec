#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""inspect_mcap: Inspect MCAP files and display metadata.

This example shows how to:
- Open an MCAP file with automatic format detection
- List all topics and their message types
- Display message counts and file statistics
- Query specific channels by topic name
"""

import sys
from datetime import datetime

import robocodec
from robocodec import RoboReader, RobocodecError

# Verify the correct API is available before running
try:
    from ._example_utils import verify_api, print_robocodec_error
    verify_api()
except ImportError:
    # Skip if utils module not available (e.g., running from different location)
    if not hasattr(robocodec, 'RoboReader'):
        print("❌ Error: Incompatible robocodec API", file=sys.stderr)
        print("   Please install using: make build-python-dev", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"❌ Error during API verification: {e}", file=sys.stderr)
    sys.exit(1)

def format_timestamp(nanos: int) -> str:
    """Convert nanoseconds since Unix epoch to readable datetime."""
    if nanos == 0:
        return "N/A"
    try:
        seconds = nanos / 1_000_000_000
        dt = datetime.fromtimestamp(seconds)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return f"<invalid timestamp: {nanos} ns>"


def format_size(bytes: int) -> str:
    """Format bytes in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024.0:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.1f} TB"


def inspect_mcap(file_path: str) -> None:
    """Inspect an MCAP or ROS bag file and print its contents."""
    print(f"📂 Inspecting: {file_path}")
    print("=" * 60)

    try:
        # RoboReader auto-detects format from file extension
        reader = RoboReader(file_path)

        # Print file overview
        print(f"\n📊 File Overview")
        print(f"  Format:           {reader.format}")
        print(f"  Size:             {format_size(reader.file_size)}")
        print(f"  Total messages:   {reader.message_count:,}")
        print(f"  Start time:       {format_timestamp(reader.start_time)}")
        print(f"  End time:         {format_timestamp(reader.end_time)}")

        # Calculate duration if timestamps are available
        if reader.start_time > 0 and reader.end_time > 0:
            duration_sec = (reader.end_time - reader.start_time) / 1_000_000_000
            print(f"  Duration:         {duration_sec:.2f} seconds")

        # List all channels (topics)
        channels = reader.channels()
        print(f"\n📡 Channels ({len(channels)} total)")
        print("-" * 60)

        # Group by encoding type
        by_encoding = {"cdr": [], "protobuf": [], "json": [], "other": []}
        for ch in channels:
            enc = ch.encoding if ch.encoding in by_encoding else "other"
            by_encoding[enc].append(ch)

        # Display channels grouped by encoding
        for encoding, chan_list in by_encoding.items():
            if chan_list:
                print(f"\n  [{encoding.upper()}]")
                for ch in sorted(chan_list, key=lambda c: c.message_count, reverse=True):
                    print(f"    📌 {ch.topic}")
                    print(f"       Type: {ch.message_type}")
                    print(f"       Messages: {ch.message_count:,}")
                    if ch.callerid:
                        print(f"       Caller ID: {ch.callerid}")

        # Show channel details for a specific topic if provided
        if len(sys.argv) > 2:
            topic_query = sys.argv[2]
            print(f"\n🔍 Details for topic matching: {topic_query}")
            matching_channels = reader.channels_by_topic(topic_query)
            for ch in matching_channels:
                print(f"\n  Channel ID:    {ch.id}")
                print(f"  Topic:         {ch.topic}")
                print(f"  Message Type:  {ch.message_type}")
                print(f"  Encoding:      {ch.encoding}")
                print(f"  Schema:        {ch.schema or 'N/A'}")
                print(f"  Msg Count:     {ch.message_count:,}")

    except RobocodecError as e:
        print_robocodec_error(e)
        sys.exit(1)


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python inspect_mcap.py <file.mcap|file.bag> [topic_query]")
        print()
        print("Examples:")
        print("  python inspect_mcap.py data.mcap")
        print("  python inspect_mcap.py data.mcap /camera/image_raw")
        sys.exit(1)

    inspect_mcap(sys.argv[1])


if __name__ == "__main__":
    main()
