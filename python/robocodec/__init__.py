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
    ChannelInfo,
    RewriteStats,
    RobocodecError,
)


# Add convenience properties to the Rust exception class.
# We add these via the class rather than subclassing to ensure that
# exceptions raised from Rust can be caught by `except RobocodecError`.
def _kind(self) -> str:
    """Error kind/category."""
    # Args are (kind, context, message)
    if len(self.args) >= 3 and self.args[0] is not None:
        return self.args[0]
    return "Error"


def _context(self):
    """Context information (e.g., schema name, codec name)."""
    # Args are (kind, context, message)
    if len(self.args) >= 3:
        return self.args[1]
    if len(self.args) == 2:
        return self.args[1]
    return None


def _message(self) -> str:
    """Human-readable error message."""
    # Args are (kind, context, message)
    if len(self.args) >= 3 and self.args[2] is not None:
        return self.args[2]
    if len(self.args) >= 1 and self.args[0] is not None:
        # Fall back to first arg if format doesn't match
        return str(self.args[0])
    return ""


def _str(self):
    """String representation combining context and message."""
    ctx = _context(self)
    msg = _message(self)
    if ctx:
        return f"{ctx}: {msg}"
    return msg


def _repr(self):
    """Developer-focused representation."""
    return f"RobocodecError(kind={_kind(self)!r}, message={_message(self)!r})"


# Add properties and methods to the exception class
RobocodecError.kind = property(_kind)
RobocodecError.context = property(_context)
RobocodecError.message = property(_message)
RobocodecError.__str__ = _str
RobocodecError.__repr__ = _repr

# Update the docstring
RobocodecError.__doc__ = """Exception for robocodec errors with structured attributes.

Attributes
----------
kind : str
    Error kind/category (e.g., "ParseError", "InvalidSchema")
context : str or None
    Context information (e.g., schema name, codec name)
message : str
    Human-readable error message

Example
-------
>>> try:
...     reader = RoboReader("nonexistent.mcap")
... except RobocodecError as e:
...     print(f"Error kind: {e.kind}")
...     print(f"Message: {e.message}")
"""


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
