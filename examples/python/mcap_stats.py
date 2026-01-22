#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""mcap_stats: Generate comprehensive statistics from MCAP or ROS bag files.

This example shows how to:
- Calculate message frequencies per topic
- Analyze data distribution across channels
- Generate human-readable statistics reports
- Export statistics to JSON for further analysis

Use cases:
- Understand what data is in your robot logs
- Identify high-frequency topics that might affect performance
- Generate reports for documentation or debugging
"""

import sys
import json
from datetime import datetime
from collections import defaultdict
from pathlib import Path

import robocodec
from robocodec import RoboReader, RobocodecError

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


def format_size(bytes: int) -> str:
    """Format bytes in human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"


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


def format_duration(nanos: int) -> str:
    """Format duration in nanoseconds as human-readable string."""
    if nanos == 0:
        return "N/A"
    seconds = nanos / 1_000_000_000

    if seconds < 1:
        milliseconds = seconds * 1000
        return f"{milliseconds:.2f} ms"
    elif seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes ({seconds:.0f}s)"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours ({seconds:.0f}s)"


def calculate_frequency(message_count: int, duration_nanos: int) -> float:
    """Calculate message frequency in Hz."""
    if duration_nanos == 0 or message_count == 0:
        return 0.0
    duration_sec = duration_nanos / 1_000_000_000
    return message_count / duration_sec


class StatisticsReport:
    """Generate and display statistics from robotics data files."""

    def __init__(self, reader: RoboReader):
        """Initialize statistics report from a RoboReader."""
        self.reader = reader
        self.channels = reader.channels()
        self.duration = reader.end_time - reader.start_time
        self._analyze()

    def _analyze(self) -> None:
        """Analyze the data and compute statistics."""
        # Group channels by encoding
        self.by_encoding = defaultdict(list)
        for ch in self.channels:
            self.by_encoding[ch.encoding].append(ch)

        # Calculate totals per encoding
        self.encoding_stats = {}
        for enc, channels in self.by_encoding.items():
            total_msgs = sum(ch.message_count for ch in channels)
            self.encoding_stats[enc] = {
                "channels": len(channels),
                "messages": total_msgs,
            }

        # Find topics with highest message counts
        self.top_topics = sorted(
            self.channels,
            key=lambda c: c.message_count,
            reverse=True
        )[:10]

    def print_summary(self) -> None:
        """Print a summary of the file."""
        print()
        print("=" * 70)
        print("📊 ROBOTICS DATA STATISTICS REPORT")
        print("=" * 70)
        print()
        print("File Information")
        print("-" * 70)
        print(f"  Path:              {self.reader.path}")
        print(f"  Format:            {self.reader.format}")
        print(f"  Size:              {format_size(self.reader.file_size)}")
        print(f"  Start time:        {format_timestamp(self.reader.start_time)}")
        print(f"  End time:          {format_timestamp(self.reader.end_time)}")
        print(f"  Duration:          {format_duration(self.duration)}")
        print()

        print("Message Statistics")
        print("-" * 70)
        print(f"  Total messages:    {self.reader.message_count:,}")
        print(f"  Total channels:    {len(self.channels)}")

        if self.duration > 0:
            overall_freq = self.reader.message_count / (self.duration / 1_000_000_000)
            print(f"  Overall frequency: {overall_freq:.2f} Hz")
        print()

    def print_encoding_breakdown(self) -> None:
        """Print breakdown by encoding type."""
        print("Encoding Breakdown")
        print("-" * 70)
        print(f"  {'Encoding':<15} {'Channels':<12} {'Messages':<15} {'%':<10}")
        print("-" * 70)

        for enc in sorted(self.encoding_stats.keys(), reverse=True):
            stats = self.encoding_stats[enc]
            pct = (stats['messages'] / self.reader.message_count * 100
                   if self.reader.message_count > 0 else 0)
            print(f"  {enc.upper():<15} {stats['channels']:<12} "
                  f"{stats['messages']:<15,} {pct:<10.1f}")
        print()

    def print_topic_summary(self, limit: int = 20) -> None:
        """Print summary of all topics."""
        print(f"Topic Summary (showing top {limit} by message count)")
        print("-" * 70)
        print(f"  {'Topic':<35} {'Type':<25} {'Freq':<10} {'Msgs':<12}")
        print("-" * 70)

        sorted_channels = sorted(
            self.channels,
            key=lambda c: c.message_count,
            reverse=True
        )[:limit]

        for ch in sorted_channels:
            freq = calculate_frequency(ch.message_count, self.duration)
            freq_str = f"{freq:.2f} Hz" if freq > 0 else "N/A"

            # Truncate long names
            topic = ch.topic[:32] + "..." if len(ch.topic) > 35 else ch.topic
            msg_type = ch.message_type[:23] + "..." if len(ch.message_type) > 26 else ch.message_type

            print(f"  {topic:<35} {msg_type:<25} {freq_str:<10} {ch.message_count:<12,}")
        print()

    def print_high_frequency_topics(self, threshold_hz: float = 10.0) -> None:
        """Print topics with high message frequency."""
        print(f"High-Frequency Topics (>{threshold_hz} Hz)")
        print("-" * 70)

        high_freq = []
        for ch in self.channels:
            freq = calculate_frequency(ch.message_count, self.duration)
            if freq > threshold_hz:
                high_freq.append((ch, freq))

        high_freq.sort(key=lambda x: x[1], reverse=True)

        if not high_freq:
            print(f"  No topics found above {threshold_hz} Hz threshold")
        else:
            print(f"  {'Topic':<40} {'Frequency':<15} {'Messages':<12}")
            print("-" * 70)
            for ch, freq in high_freq:
                topic = ch.topic[:38] + "..." if len(ch.topic) > 40 else ch.topic
                print(f"  {topic:<40} {freq:<15.2f} {ch.message_count:<12,}")
        print()

    def print_message_type_summary(self) -> None:
        """Print summary grouped by message type."""
        print("Message Type Summary")
        print("-" * 70)

        by_type = defaultdict(lambda: {"count": 0, "topics": set()})
        for ch in self.channels:
            by_type[ch.message_type]["count"] += ch.message_count
            by_type[ch.message_type]["topics"].add(ch.topic)

        sorted_types = sorted(
            by_type.items(),
            key=lambda x: x[1]["count"],
            reverse=True
        )

        print(f"  {'Message Type':<40} {'Messages':<12} {'Topics':<8}")
        print("-" * 70)

        for msg_type, stats in sorted_types[:20]:
            type_name = msg_type[:38] + "..." if len(msg_type) > 40 else msg_type
            print(f"  {type_name:<40} {stats['count']:<12,} {len(stats['topics']):<8}")
        print()

    def to_json(self) -> dict:
        """Export statistics as a dictionary for JSON serialization."""
        return {
            "file": {
                "path": self.reader.path,
                "format": self.reader.format,
                "size_bytes": self.reader.file_size,
                "start_time_ns": self.reader.start_time,
                "end_time_ns": self.reader.end_time,
                "duration_ns": self.duration,
            },
            "summary": {
                "total_messages": self.reader.message_count,
                "total_channels": len(self.channels),
            },
            "channels": [
                {
                    "id": ch.id,
                    "topic": ch.topic,
                    "message_type": ch.message_type,
                    "encoding": ch.encoding,
                    "message_count": ch.message_count,
                    "frequency_hz": calculate_frequency(ch.message_count, self.duration),
                }
                for ch in self.channels
            ],
            "encoding_breakdown": {
                enc: {
                    "channels": stats["channels"],
                    "messages": stats["messages"],
                }
                for enc, stats in self.encoding_stats.items()
            },
        }

    def export_json(self, output_path: str) -> None:
        """Export statistics to a JSON file."""
        data = self.to_json()
        try:
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)
            print(f"✅ Statistics exported to: {output_path}")
        except PermissionError:
            print(f"❌ Error: Permission denied writing to {output_path}", file=sys.stderr)
            print("   Check that you have write access to the output directory.", file=sys.stderr)
            sys.exit(1)
        except IsADirectoryError:
            print(f"❌ Error: {output_path} is a directory, not a file", file=sys.stderr)
            print("   Specify a file path, not a directory.", file=sys.stderr)
            sys.exit(1)
        except (OSError, json.JSONDecodeError, TypeError) as e:
            print(f"❌ Error: Failed to export statistics to {output_path}", file=sys.stderr)
            print(f"   {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python mcap_stats.py <file.mcap|file.bag> [options]")
        print()
        print("Generate comprehensive statistics from robotics data files.")
        print()
        print("Options:")
        print("  --export <path>     Export statistics to JSON file")
        print("  --topics <n>        Show top N topics (default: 20)")
        print("  --freq <hz>         High frequency threshold (default: 10 Hz)")
        print()
        print("Examples:")
        print("  python mcap_stats.py data.mcap")
        print("  python mcap_stats.py data.mcap --export stats.json")
        print("  python mcap_stats.py data.mcap --topics 50 --freq 30")
        sys.exit(1)

    input_path = sys.argv[1]

    # Parse optional arguments
    export_path = None
    topic_limit = 20
    freq_threshold = 10.0

    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--export" and i + 1 < len(sys.argv):
            export_path = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--topics" and i + 1 < len(sys.argv):
            try:
                topic_limit = int(sys.argv[i + 1])
            except ValueError:
                print(f"❌ Error: --topics requires a valid integer, got '{sys.argv[i + 1]}'", file=sys.stderr)
                sys.exit(1)
            i += 2
        elif sys.argv[i] == "--freq" and i + 1 < len(sys.argv):
            try:
                freq_threshold = float(sys.argv[i + 1])
            except ValueError:
                print(f"❌ Error: --freq requires a valid number, got '{sys.argv[i + 1]}'", file=sys.stderr)
                sys.exit(1)
            i += 2
        elif sys.argv[i].startswith("--"):
            print(f"❌ Error: Unknown option: {sys.argv[i]}", file=sys.stderr)
            print("   Use --help for usage information.", file=sys.stderr)
            sys.exit(1)
        else:
            i += 1

    try:
        reader = RoboReader(input_path)
        report = StatisticsReport(reader)

        # Print all report sections
        report.print_summary()
        report.print_encoding_breakdown()
        report.print_topic_summary(limit=topic_limit)
        report.print_high_frequency_topics(threshold_hz=freq_threshold)
        report.print_message_type_summary()

        # Export to JSON if requested
        if export_path:
            report.export_json(export_path)

    except RobocodecError as e:
        print_robocodec_error(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
