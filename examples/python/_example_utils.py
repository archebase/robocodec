#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Shared utilities for robocodec examples.

This module provides helper functions for example scripts, including
API verification to ensure the correct robocodec package is installed.
"""

import sys
import os
from pathlib import Path


def verify_api() -> None:
    """Verify that the correct robocodec API is available.

    Checks for the expected classes (RoboReader, RoboWriter, etc.) and
    provides helpful error messages if the wrong package is installed.

    Raises:
        SystemExit: If the required API is not available
    """
    try:
        import robocodec
    except ImportError:
        print("❌ Error: robocodec package not found", file=sys.stderr)
        print(file=sys.stderr)
        print("To install robocodec in development mode:", file=sys.stderr)
        print("  cd /path/to/robocodec", file=sys.stderr)
        print("  make build-python-dev", file=sys.stderr)
        print(file=sys.stderr)
        print("Or activate the virtual environment:", file=sys.stderr)
        print("  source .venv/bin/activate", file=sys.stderr)
        print("  python examples/python/inspect_mcap.py data.mcap", file=sys.stderr)
        sys.exit(1)

    # Check for the correct API (RoboReader, not Reader)
    required_classes = [
        "RoboReader",
        "RoboWriter",
        "RoboRewriter",
        "RobocodecError",
        "TransformBuilder",
    ]

    missing = [name for name in required_classes if not hasattr(robocodec, name)]

    if missing:
        print("❌ Error: Incompatible robocodec API detected", file=sys.stderr)
        print(file=sys.stderr)
        print(f"Missing classes: {', '.join(missing)}", file=sys.stderr)
        print(file=sys.stderr)
        print("This usually means you're using a system-wide installation", file=sys.stderr)
        print("instead of the local development build.", file=sys.stderr)
        print(file=sys.stderr)
        print("To fix this:", file=sys.stderr)
        print("  1. Build and install in development mode:", file=sys.stderr)
        print("     make build-python-dev", file=sys.stderr)
        print(file=sys.stderr)
        print("  2. Run examples using the virtual environment Python:", file=sys.stderr)
        print("     .venv/bin/python3 examples/python/inspect_mcap.py data.mcap", file=sys.stderr)
        print(file=sys.stderr)
        print("  3. Or activate the venv first:", file=sys.stderr)
        print("     source .venv/bin/activate", file=sys.stderr)
        print("     python examples/python/inspect_mcap.py data.mcap", file=sys.stderr)
        sys.exit(1)


def get_test_data_path(filename: str) -> str:
    """Get the path to a test data file.

    Args:
        filename: Name of the test file (e.g., 'robocodec_test_14.mcap')

    Returns:
        Absolute path to the test file

    Raises:
        SystemExit: If the test file cannot be found
    """
    # Try common test fixture locations
    possible_paths = [
        Path(__file__).parent.parent.parent / "tests" / "fixtures" / filename,
        Path.cwd() / "tests" / "fixtures" / filename,
        Path(__file__).parent / "fixtures" / filename,
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    print(f"⚠️  Warning: Test file '{filename}' not found", file=sys.stderr)
    print(f"    Searched in:", file=sys.stderr)
    for p in possible_paths:
        print(f"      - {p}", file=sys.stderr)
    return filename  # Return original filename as fallback


def print_example_header(title: str) -> None:
    """Print a formatted header for an example.

    Args:
        title: The title of the example
    """
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)
    print()


def auto_verify_at_import() -> None:
    """Automatically verify API when this module is imported.

    This is called automatically on import to ensure examples fail
    fast with a helpful message if the API is incorrect.
    """
    # Only verify if we're being run as a script, not imported
    if __name__ != "_example_utils":
        verify_api()
