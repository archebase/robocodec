# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Robocodec: Robotics data format library for MCAP and ROS bag files.

This library provides Python bindings for robocodec, enabling reading and
writing MCAP and ROS1 bag files with automatic format detection.
"""

from robocodec._robocodec import (
    RoboReader,
    RoboWriter,
    RoboRewriter,
    TransformBuilder,
    RobocodecError,
    ChannelInfo,
    RewriteStats,
)

__all__ = [
    "RoboReader",
    "RoboWriter",
    "RoboRewriter",
    "TransformBuilder",
    "RobocodecError",
    "ChannelInfo",
    "RewriteStats",
]

__version__ = "0.1.0"
