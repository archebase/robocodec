# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Pytest configuration and fixtures for robocodec tests."""

import pytest
import tempfile
import os


@pytest.fixture
def temp_mcap_file():
    """Create a temporary file path that can be used for testing."""
    fd, path = tempfile.mkstemp(suffix=".mcap")
    os.close(fd)
    yield path
    # Clean up
    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def temp_bag_file():
    """Create a temporary .bag file path for testing."""
    fd, path = tempfile.mkstemp(suffix=".bag")
    os.close(fd)
    yield path
    # Clean up
    try:
        os.unlink(path)
    except OSError:
        pass
