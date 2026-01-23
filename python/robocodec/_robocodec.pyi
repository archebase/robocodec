# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Type stubs for robocodec._robocodec native extension.

This file provides type hints for IDE autocomplete and type checkers.
"""

from typing import Optional

class RobocodecError(Exception):
    """Exception raised for robocodec errors.

    Attributes:
        kind: Error kind/category (e.g., "ParseError", "InvalidSchema")
        context: Context information (e.g., schema name, codec name)
        message: Human-readable error message
    """

    kind: str
    context: Optional[str]
    message: str
    def __init__(self, kind: str, context: Optional[str], message: str) -> None: ...

class ChannelInfo:
    """Channel information from a robotics data file.

    Attributes:
        id: Unique channel ID within the file
        topic: Topic name (e.g., "/joint_states", "/tf")
        message_type: Message type name (e.g., "sensor_msgs/msg/JointState")
        encoding: Encoding format (e.g., "cdr", "protobuf", "json")
        schema: Schema definition (message definition text)
        schema_encoding: Schema encoding (e.g., "ros2msg", "protobuf")
        message_count: Number of messages in this channel
        callerid: Caller ID (ROS1 specific, identifies the publishing node)
    """

    id: int
    topic: str
    message_type: str
    encoding: str
    schema: Optional[str]
    schema_encoding: Optional[str]
    message_count: int
    callerid: Optional[str]
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class RewriteStats:
    """Statistics from a rewrite operation.

    Attributes:
        message_count: Total messages processed
        channel_count: Total channels processed
        decode_failures: Messages that failed to decode
        encode_failures: Messages that failed to encode
        reencoded_count: Messages that were successfully re-encoded
        passthrough_count: Messages passed through without re-encoding
        topics_renamed: Number of topics renamed (if transforms were applied)
        types_renamed: Number of types renamed (if transforms were applied)
    """

    message_count: int
    channel_count: int
    decode_failures: int
    encode_failures: int
    reencoded_count: int
    passthrough_count: int
    topics_renamed: int
    types_renamed: int
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class RoboReader:
    """Unified robotics data reader with auto-detection.

    RoboReader automatically detects the file format (MCAP or ROS1 bag)
    from the file extension and provides a consistent API for reading.
    """

    def __init__(self, path: str) -> None: ...
    @property
    def message_count(self) -> int: ...
    @property
    def start_time(self) -> Optional[int]: ...
    @property
    def end_time(self) -> Optional[int]: ...
    @property
    def path(self) -> str: ...
    @property
    def format(self) -> str: ...
    @property
    def file_size(self) -> int: ...
    def channels(self) -> list[ChannelInfo]: ...
    def channel_by_topic(self, topic: str) -> Optional[ChannelInfo]: ...
    def channels_by_topic(self, topic: str) -> list[ChannelInfo]: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class RoboWriter:
    """Unified robotics data writer with auto-detection.

    RoboWriter automatically detects the output format from the file
    extension (.mcap or .bag) and provides a consistent API for writing.
    """

    def __init__(self, path: str) -> None: ...
    @property
    def message_count(self) -> int: ...
    @property
    def channel_count(self) -> int: ...
    @property
    def path(self) -> str: ...
    @property
    def format(self) -> str: ...
    def add_channel(
        self,
        topic: str,
        message_type: str,
        encoding: str,
        schema: Optional[str] = None,
    ) -> int: ...
    def finish(self) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class TransformBuilder:
    """Builder for creating topic/type transformation pipelines.

    TransformBuilder provides a fluent API for creating transformations
    that can be applied during rewrite operations.
    """

    def __init__(self) -> None: ...
    def with_topic_rename(self, from_: str, to: str) -> TransformBuilder: ...
    def with_topic_rename_wildcard(
        self, pattern: str, target: str
    ) -> TransformBuilder: ...
    def with_type_rename(self, from_: str, to: str) -> TransformBuilder: ...
    def with_type_rename_wildcard(
        self, pattern: str, target: str
    ) -> TransformBuilder: ...
    def with_topic_type_rename(
        self, topic: str, source_type: str, target_type: str
    ) -> TransformBuilder: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class RoboRewriter:
    """Unified rewriter with format auto-detection and transform support.

    RoboRewriter reads a robotics data file, applies optional transformations
    (topic/type renaming), and writes to an output file.
    """

    def __init__(
        self,
        input_path: str,
        *,
        validate_schemas: bool = True,
        skip_decode_failures: bool = True,
    ) -> None: ...
    @classmethod
    def with_transforms(
        cls,
        input_path: str,
        transform_builder: TransformBuilder,
        *,
        validate_schemas: bool = True,
        skip_decode_failures: bool = True,
    ) -> RoboRewriter: ...
    @property
    def input_path(self) -> str: ...
    @property
    def validate_schemas(self) -> bool: ...
    @property
    def skip_decode_failures(self) -> bool: ...
    @property
    def has_transforms(self) -> bool: ...
    def rewrite(self, output_path: str) -> RewriteStats: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
