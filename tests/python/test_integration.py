# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Integration tests for robocodec Python bindings."""

import os
import pytest
import tempfile

import robocodec
from robocodec import (
    RoboReader,
    RoboWriter,
    RoboRewriter,
    TransformBuilder,
    RobocodecError,
    ChannelInfo,
    RewriteStats,
)

# Get the fixtures directory
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "fixtures")


class TestRoboReaderIntegration:
    """Integration tests for RoboReader with actual files."""

    def test_read_mcap_file(self):
        """Test reading an actual MCAP file."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)

        # Verify basic properties
        assert reader.path == test_file
        assert reader.format == "MCAP"
        assert reader.file_size > 0

        # Get channels
        channels = reader.channels()
        assert isinstance(channels, list)
        assert len(channels) > 0

        # Verify channel structure
        channel = channels[0]
        assert isinstance(channel, ChannelInfo)
        assert hasattr(channel, "id")
        assert hasattr(channel, "topic")
        assert hasattr(channel, "message_type")
        assert hasattr(channel, "encoding")
        assert hasattr(channel, "message_count")

    def test_read_bag_file(self):
        """Test reading an actual ROS bag file."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_15.bag")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)

        # Verify basic properties
        assert reader.path == test_file
        assert reader.format == "BAG"
        assert reader.file_size > 0

        # Get channels
        channels = reader.channels()
        assert isinstance(channels, list)

    def test_channel_by_topic(self):
        """Test retrieving a channel by topic name."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)
        channels = reader.channels()

        if len(channels) > 0:
            topic = channels[0].topic
            channel = reader.channel_by_topic(topic)
            assert channel is not None
            assert channel.topic == topic

    def test_channels_by_topic(self):
        """Test retrieving multiple channels by topic name."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)

        # Test with non-existent topic
        channels = reader.channels_by_topic("/nonexistent/topic")
        assert isinstance(channels, list)

    def test_reader_repr(self):
        """Test reader string representation."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)
        repr_str = repr(reader)
        assert "RoboReader" in repr_str
        assert "MCAP" in repr_str or "BAG" in repr_str


class TestRoboWriterIntegration:
    """Integration tests for RoboWriter with actual file creation."""

    def test_write_and_finish_mcap(self):
        """Test writing and finishing an MCAP file."""
        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer = RoboWriter(tmp_path)
            assert writer.path == tmp_path
            assert writer.format == "MCAP"

            # Add a channel
            channel_id = writer.add_channel(
                "/test/topic",
                "std_msgs/String",
                "cdr",
                "string data"
            )
            assert isinstance(channel_id, int)
            assert channel_id >= 0

            # Finish writing
            writer.finish()

            # Verify file was created
            assert os.path.exists(tmp_path)
            assert os.path.getsize(tmp_path) > 0

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_write_bag_file(self):
        """Test writing a bag file."""
        with tempfile.NamedTemporaryFile(suffix=".bag", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer = RoboWriter(tmp_path)
            assert writer.format == "BAG"

            # Add a channel and finish
            writer.add_channel("/chatter", "std_msgs/String", "cdr", None)
            writer.finish()

            # Verify file was created
            assert os.path.exists(tmp_path)

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_writer_properties(self):
        """Test writer properties."""
        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer = RoboWriter(tmp_path)
            assert writer.message_count == 0
            assert writer.channel_count == 0

            writer.add_channel("/test1", "std_msgs/String", "cdr", None)
            assert writer.channel_count == 1

            writer.add_channel("/test2", "std_msgs/String", "cdr", None)
            assert writer.channel_count == 2

        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestRoboRewriterIntegration:
    """Integration tests for RoboRewriter with actual files."""

    def test_simple_rewrite(self):
        """Test a simple rewrite operation."""
        input_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(input_file):
            pytest.skip(f"Test fixture not found: {input_file}")

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            output_path = tmp.name

        try:
            rewriter = RoboRewriter(input_file)
            stats = rewriter.rewrite(output_path)

            # Verify stats structure
            assert isinstance(stats, RewriteStats)
            assert hasattr(stats, "message_count")
            assert hasattr(stats, "channel_count")
            assert hasattr(stats, "decode_failures")
            assert hasattr(stats, "encode_failures")

            # Verify output file was created
            assert os.path.exists(output_path)

            # Verify the output file can be read
            reader = RoboReader(output_path)
            assert reader.message_count > 0

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_rewrite_with_transforms(self):
        """Test rewrite with topic/type transformations."""
        input_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(input_file):
            pytest.skip(f"Test fixture not found: {input_file}")

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            output_path = tmp.name

        try:
            # Create transform builder
            builder = TransformBuilder()
            builder = builder.with_topic_rename("/old", "/new")
            builder = builder.with_type_rename("OldType", "NewType")

            # Create rewriter with transforms
            rewriter = RoboRewriter.with_transforms(input_file, builder)
            assert rewriter.has_transforms is True

            # Perform rewrite
            stats = rewriter.rewrite(output_path)
            assert isinstance(stats, RewriteStats)

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_rewriter_properties(self):
        """Test rewriter properties."""
        input_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(input_file):
            pytest.skip(f"Test fixture not found: {input_file}")

        rewriter = RoboRewriter(
            input_file,
            validate_schemas=True,
            skip_decode_failures=True
        )

        assert rewriter.input_path == input_file
        assert rewriter.validate_schemas is True
        assert rewriter.skip_decode_failures is True
        assert rewriter.has_transforms is False


class TestTransformBuilderIntegration:
    """Integration tests for TransformBuilder."""

    def test_transform_builder_chaining(self):
        """Test that TransformBuilder methods can be chained."""
        builder = (
            TransformBuilder()
            .with_topic_rename("/old1", "/new1")
            .with_topic_rename("/old2", "/new2")
            .with_type_rename("OldType1", "NewType1")
            .with_type_rename("OldType2", "NewType2")
            .with_topic_rename_wildcard("/foo/*", "/bar/*")
            .with_type_rename_wildcard("old/*", "new/*")
            .with_topic_type_rename("/specific", "old.msg.Type", "new.msg.Type")
        )

        # Verify the builder was created successfully
        assert "TransformBuilder" in repr(builder)

    def test_transform_builder_with_rewriter(self):
        """Test TransformBuilder integration with RoboRewriter."""
        input_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(input_file):
            pytest.skip(f"Test fixture not found: {input_file}")

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            output_path = tmp.name

        try:
            builder = (
                TransformBuilder()
                .with_topic_rename("/test", "/renamed")
                .with_type_rename("TestMsg", "RenamedMsg")
            )

            rewriter = RoboRewriter.with_transforms(input_file, builder)
            assert rewriter.has_transforms is True

            # This should not raise an error
            rewriter.rewrite(output_path)

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)


class TestErrorHandling:
    """Integration tests for error handling."""

    def test_nonexistent_file_reader(self):
        """Test that reading a non-existent file raises RobocodecError."""
        with pytest.raises(RobocodecError):
            RoboReader("/nonexistent/path/to/file.mcap")

    def test_nonexistent_file_rewriter(self):
        """Test that rewriting a non-existent file raises RobocodecError."""
        with pytest.raises(RobocodecError):
            RoboRewriter("/nonexistent/path/to/file.mcap")

    def test_invalid_extension_writer(self):
        """Test creating a writer with an unknown extension."""
        # This should create the writer but may fail at finish
        with tempfile.NamedTemporaryFile(suffix=".unknown", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            writer = RoboWriter(tmp_path)
            assert writer.format == "Unknown"
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_error_attributes(self):
        """Test that error attributes are accessible."""
        try:
            RoboReader("/nonexistent/file.mcap")
            pytest.fail("Should have raised RobocodecError")
        except RobocodecError as e:
            # Verify structured error attributes are available
            assert hasattr(e, "kind")
            assert hasattr(e, "context")
            assert hasattr(e, "message")
            assert e.kind is not None
            assert e.message is not None


class TestChannelInfo:
    """Integration tests for ChannelInfo."""

    def test_channel_info_attributes(self):
        """Test ChannelInfo attributes from real file."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)
        channels = reader.channels()

        if len(channels) > 0:
            channel = channels[0]

            # Test all attributes exist
            assert isinstance(channel.id, int)
            assert isinstance(channel.topic, str)
            assert isinstance(channel.message_type, str)
            assert isinstance(channel.encoding, str)
            assert isinstance(channel.message_count, int)

            # Test optional attributes
            assert channel.schema is None or isinstance(channel.schema, str)
            assert channel.schema_encoding is None or isinstance(channel.schema_encoding, str)
            assert channel.callerid is None or isinstance(channel.callerid, str)

    def test_channel_info_repr(self):
        """Test ChannelInfo string representation."""
        test_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(test_file):
            pytest.skip(f"Test fixture not found: {test_file}")

        reader = RoboReader(test_file)
        channels = reader.channels()

        if len(channels) > 0:
            channel = channels[0]
            repr_str = repr(channel)
            assert "ChannelInfo" in repr_str


class TestRewriteStats:
    """Integration tests for RewriteStats."""

    def test_rewrite_stats_from_actual_rewrite(self):
        """Test RewriteStats from actual rewrite operation."""
        input_file = os.path.join(FIXTURES_DIR, "robocodec_test_0.mcap")
        if not os.path.exists(input_file):
            pytest.skip(f"Test fixture not found: {input_file}")

        with tempfile.NamedTemporaryFile(suffix=".mcap", delete=False) as tmp:
            output_path = tmp.name

        try:
            rewriter = RoboRewriter(input_file)
            stats = rewriter.rewrite(output_path)

            # Test all attributes are accessible
            assert isinstance(stats.message_count, int)
            assert isinstance(stats.channel_count, int)
            assert isinstance(stats.decode_failures, int)
            assert isinstance(stats.encode_failures, int)
            assert isinstance(stats.reencoded_count, int)
            assert isinstance(stats.passthrough_count, int)
            assert isinstance(stats.topics_renamed, int)
            assert isinstance(stats.types_renamed, int)

            # Test repr
            repr_str = repr(stats)
            assert "RewriteStats" in repr_str

        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)


class TestModuleStructure:
    """Tests for the module structure and exports."""

    def test_module_version(self):
        """Test that the module has a version attribute."""
        assert hasattr(robocodec, "__version__")
        assert isinstance(robocodec.__version__, str)

    def test_all_exports(self):
        """Test that __all__ is properly defined."""
        assert hasattr(robocodec, "__all__")
        expected = [
            "RoboReader",
            "RoboWriter",
            "RoboRewriter",
            "TransformBuilder",
            "RobocodecError",
            "ChannelInfo",
            "RewriteStats",
        ]
        for name in expected:
            assert name in robocodec.__all__
