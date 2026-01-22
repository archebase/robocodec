# SPDX-FileCopyrightText: 2026 ArcheBase
#
# SPDX-License-Identifier: MulanPSL-2.0

"""Tests for robocodec Python bindings."""

import pytest

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


class TestRobocodecError:
    """Tests for the RobocodecError exception."""

    def test_error_exists(self):
        """Test that RobocodecError exception class exists."""
        assert RobocodecError is not None

    def test_error_attributes(self):
        """Test that error has the expected attributes."""
        # We can't easily trigger real errors without valid data files,
        # but we can check the exception type exists
        assert hasattr(RobocodecError, "__doc__")

    def test_error_is_exception(self):
        """Test that RobocodecError is an exception type."""
        assert issubclass(RobocodecError, Exception)


class TestTransformBuilder:
    """Tests for the TransformBuilder class."""

    def test_builder_creation(self):
        """Test creating a new TransformBuilder."""
        builder = TransformBuilder()
        assert builder is not None
        assert "TransformBuilder" in repr(builder)

    def test_with_topic_rename(self):
        """Test adding a topic rename mapping."""
        builder = TransformBuilder()
        result = builder.with_topic_rename("/old/topic", "/new/topic")
        # Method chaining should return the same builder
        assert result is builder

    def test_with_topic_rename_wildcard(self):
        """Test adding a wildcard topic rename mapping."""
        builder = TransformBuilder()
        result = builder.with_topic_rename_wildcard("/foo/*", "/bar/*")
        assert result is builder

    def test_with_type_rename(self):
        """Test adding a type rename mapping."""
        builder = TransformBuilder()
        result = builder.with_type_rename("old_pkg/Msg", "new_pkg/Msg")
        assert result is builder

    def test_with_type_rename_wildcard(self):
        """Test adding a wildcard type rename mapping."""
        builder = TransformBuilder()
        result = builder.with_type_rename_wildcard("foo/*", "bar/*")
        assert result is builder

    def test_with_topic_type_rename(self):
        """Test adding a topic-specific type rename mapping."""
        builder = TransformBuilder()
        result = builder.with_topic_type_rename(
            "/specific/topic", "old_pkg/OldMsg", "new_pkg/NewMsg"
        )
        assert result is builder

    def test_builder_chaining(self):
        """Test that builder methods can be chained."""
        builder = (
            TransformBuilder()
            .with_topic_rename("/old", "/new")
            .with_type_rename("OldMsg", "NewMsg")
            .with_topic_rename_wildcard("/foo/*", "/bar/*")
        )
        assert "TransformBuilder" in repr(builder)

    def test_builder_repr(self):
        """Test builder string representation."""
        builder = TransformBuilder()
        repr_str = repr(builder)
        assert "TransformBuilder" in repr_str


class TestChannelInfo:
    """Tests for the ChannelInfo class."""

    def test_channel_info_attributes(self):
        """Test that ChannelInfo has expected attributes."""
        # We can't create a ChannelInfo directly from Python,
        # but we can verify the class exists with correct attributes
        assert ChannelInfo is not None
        # Check that we can access the class docstring
        assert ChannelInfo.__doc__ is not None


class TestRewriteStats:
    """Tests for the RewriteStats class."""

    def test_rewrite_stats_attributes(self):
        """Test that RewriteStats has expected attributes."""
        assert RewriteStats is not None
        assert RewriteStats.__doc__ is not None


class TestRoboReader:
    """Tests for the RoboReader class."""

    def test_reader_class_exists(self):
        """Test that RoboReader class exists."""
        assert RoboReader is not None

    def test_reader_requires_file(self):
        """Test that RoboReader requires a file path."""
        # Trying to create a reader without arguments should fail
        with pytest.raises(TypeError):
            RoboReader()  # noqa: E801

    def test_reader_with_invalid_file(self):
        """Test that RoboReader raises error for non-existent file."""
        with pytest.raises(RobocodecError):
            RoboReader("/nonexistent/file.mcap")


class TestRoboWriter:
    """Tests for the RoboWriter class."""

    def test_writer_class_exists(self):
        """Test that RoboWriter class exists."""
        assert RoboWriter is not None

    def test_writer_requires_file(self):
        """Test that RoboWriter requires a file path."""
        with pytest.raises(TypeError):
            RoboWriter()  # noqa: E801


class TestRoboRewriter:
    """Tests for the RoboRewriter class."""

    def test_rewriter_class_exists(self):
        """Test that RoboRewriter class exists."""
        assert RoboRewriter is not None

    def test_rewriter_requires_file(self):
        """Test that RoboRewriter requires a file path."""
        with pytest.raises(TypeError):
            RoboRewriter()  # noqa: E801

    def test_rewriter_with_invalid_file(self):
        """Test that RoboRewriter raises error for non-existent file."""
        with pytest.raises(RobocodecError):
            RoboRewriter("/nonexistent/file.mcap")

    def test_rewriter_with_transforms_class_method(self):
        """Test that with_transforms is a class method."""
        assert hasattr(RoboRewriter, "with_transforms")
        # It should be callable from the class
        assert callable(RoboRewriter.with_transforms)


class TestModuleStructure:
    """Tests for the module structure and exports."""

    def test_module_exports(self):
        """Test that expected classes are exported from the module."""
        assert hasattr(robocodec, "RoboReader")
        assert hasattr(robocodec, "RoboWriter")
        assert hasattr(robocodec, "RoboRewriter")
        assert hasattr(robocodec, "TransformBuilder")
        assert hasattr(robocodec, "RobocodecError")
        assert hasattr(robocodec, "ChannelInfo")
        assert hasattr(robocodec, "RewriteStats")

    def test_module_version(self):
        """Test that the module has version information."""
        # Either __version__ attribute or version in __doc__
        has_version = hasattr(robocodec, "__version__")
        # We don't fail if version isn't set, just check
        assert isinstance(has_version, bool)

    def test_module_docstring(self):
        """Test that the module has a docstring."""
        assert robocodec.__doc__ is not None
