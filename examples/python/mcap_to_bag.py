#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""mcap_to_bag: Convert between robotics data formats.

This example shows how to:
- Convert MCAP files to ROS1 bag format
- Convert ROS1 bag files to MCAP format
- Use RoboRewriter for format conversion
- Display conversion statistics
"""

import sys
import os

import robocodec
from robocodec import RoboReader, RoboRewriter, RobocodecError

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


def detect_output_format(input_path: str, output_path: str) -> str:
    """Determine output format from file extension."""
    ext = os.path.splitext(output_path)[1].lower()
    if ext == ".mcap":
        return "MCAP"
    elif ext == ".bag":
        return "BAG"
    else:
        return "Unknown"


def convert_file(input_path: str, output_path: str, validate: bool = True, skip_failures: bool = True) -> bool:
    """Convert between MCAP and ROS bag formats.

    Args:
        input_path: Path to input file (.mcap or .bag)
        output_path: Path to output file
        validate: Whether to validate schemas during conversion
        skip_failures: Whether to skip messages that fail to decode

    Returns:
        True if conversion succeeded, False otherwise
    """
    # Verify input file exists
    if not os.path.exists(input_path):
        print(f"❌ Error: Input file not found: {input_path}")
        return False

    # Verify output directory exists and is writable
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        print(f"❌ Error: Output directory does not exist: {output_dir}", file=sys.stderr)
        print("   Create the directory first or choose a different output path.", file=sys.stderr)
        return False

    if output_dir and not os.access(output_dir, os.W_OK):
        print(f"❌ Error: No write permission for output directory: {output_dir}", file=sys.stderr)
        return False

    # Show what we're converting
    input_reader = RoboReader(input_path)
    output_format = detect_output_format(input_path, output_path)

    print(f"🔄 Converting robotics data")
    print("=" * 50)
    print(f"  Input:  {input_path} ({input_reader.format})")
    print(f"  Output: {output_path} ({output_format})")
    print(f"  Messages: {input_reader.message_count:,}")
    print()

    # Create rewriter with options
    rewriter = RoboRewriter(
        input_path,
        validate_schemas=validate,
        skip_decode_failures=skip_failures
    )

    # Perform the conversion
    print("⏳ Converting...")
    try:
        stats = rewriter.rewrite(output_path)
    except RobocodecError as e:
        print_robocodec_error(e)
        return False

    # Display results
    print("✅ Conversion complete!")
    print()
    print("📊 Statistics:")
    print(f"  Messages processed:    {stats.message_count:,}")
    print(f"  Channels processed:    {stats.channel_count}")
    print(f"  Re-encoded messages:   {stats.reencoded_count:,}")
    print(f"  Passthrough messages:  {stats.passthrough_count:,}")

    if stats.decode_failures > 0:
        print(f"  ⚠️  Decode failures:      {stats.decode_failures}")
    if stats.encode_failures > 0:
        print(f"  ⚠️  Encode failures:      {stats.encode_failures}")

    # Verify output file was created
    if os.path.exists(output_path):
        output_size = os.path.getsize(output_path)
        input_size = os.path.getsize(input_path)
        ratio = (output_size / input_size) * 100 if input_size > 0 else 0

        print()
        print(f"📁 Output file size: {output_size:,} bytes ({ratio:.1f}% of input)")

        # Verify the output can be read
        try:
            output_reader = RoboReader(output_path)
            print(f"✅ Output verified: {output_reader.format} format, {output_reader.message_count:,} messages")
        except RobocodecError as e:
            print("⚠️  Warning: Output file created but could not be verified")
            print_robocodec_error(e)
            return False

    return True


def main() -> None:
    """Main entry point."""
    if len(sys.argv) < 3:
        print("Usage: python mcap_to_bag.py <input_file> <output_file>")
        print()
        print("Convert between robotics data formats:")
        print("  MCAP (.mcap) <-> ROS1 bag (.bag)")
        print()
        print("Examples:")
        print("  python mcap_to_bag.py data.mcap data.bag")
        print("  python mcap_to_bag.py old_data.bag new_data.mcap")
        print()
        print("Options:")
        print("  --validate       Enable schema validation (default: enabled)")
        print("  --no-validate   Disable schema validation")
        print("  --fail-on-error  Fail the conversion if decode fails (default: skip failures)")
        sys.exit(1)

    # Parse arguments
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Optional flags
    validate = "--no-validate" not in sys.argv
    skip_failures = "--fail-on-error" not in sys.argv

    # Convert and exit with appropriate code
    success = convert_file(input_path, output_path, validate, skip_failures)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
