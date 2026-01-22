# Robocodec Python Examples

This directory contains practical examples for using the robocodec Python library to work with robotics data formats (MCAP and ROS bag files).

## Quick Start

### 1. Install the Library

From the project root, build and install in development mode:

```bash
cd /path/to/robocodec
make build-python-dev
```

### 2. Run an Example

```bash
# Using the virtual environment Python directly
.venv/bin/python3 examples/python/inspect_mcap.py tests/fixtures/robocodec_test_14.mcap

# Or activate the venv first
source .venv/bin/activate
python3 examples/python/inspect_mcap.py tests/fixtures/robocodec_test_14.mcap
```

## ⚠️ Important: Python Environment

These examples require the **locally-built** robocodec package, not a system-wide installation. The examples include automatic API verification and will show a helpful error message if the wrong package is detected.

**Symptoms of wrong environment:**
```
AttributeError: module 'robocodec' has no attribute 'RoboReader'
```

**Solution:**
```bash
# Always use the venv Python
.venv/bin/python3 examples/python/inspect_mcap.py data.mcap

# Or activate venv first
source .venv/bin/activate
python3 examples/python/inspect_mcap.py data.mcap
```

## Examples

### 1. inspect_mcap.py - Basic File Inspection

Inspect MCAP or ROS bag files and display metadata, topics, and message counts.

```bash
# Inspect a file and show all topics
python inspect_mcap.py data.mcap

# Get details for a specific topic
python inspect_mcap.py data.mcap /camera/image_raw
```

**What you'll learn:**
- Using `RoboReader` for automatic format detection
- Listing channels and their properties
- Accessing file metadata (size, timestamps, message counts)

---

### 2. mcap_to_bag.py - Format Conversion

Convert between MCAP and ROS1 bag formats.

```bash
# Convert MCAP to ROS bag
python mcap_to_bag.py data.mcap data.bag

# Convert ROS bag to MCAP
python mcap_to_bag.py old_data.bag new_data.mcap
```

**What you'll learn:**
- Using `RoboRewriter` for format conversion
- Understanding conversion statistics
- Handling decode/encode failures

---

### 3. topic_rename.py - Fluent Transformations

Demonstrates the fluent `TransformBuilder` API for renaming topics and message types.

```bash
# Example 1: Simple topic rename
python topic_rename.py input.mcap output.mcap 1

# Example 2: Wildcard topic rename
python topic_rename.py input.mcap output.mcap 2

# Example 3: Message type rename
python topic_rename.py input.mcap output.mcap 3

# Example 4: Topic-specific type rename
python topic_rename.py input.mcap output.mcap 4

# Example 5: Complex pipeline combining all transformations
python topic_rename.py input.mcap output.mcap 5

# Example 6: Load transformations from JSON config
python topic_rename.py input.mcap output.mcap 6 config.json
```

**What you'll learn:**
- Fluent API design with method chaining
- `TransformBuilder` for transformation pipelines
- Wildcard patterns for bulk renaming
- Topic-specific type transformations

**Sample config.json for Example 6:**
```json
{
  "topic_renames": [
    {"from": "/old/camera", "to": "/camera"}
  ],
  "topic_wildcards": [
    {"pattern": "/robot1/*", "target": "/robot/*"}
  ],
  "type_renames": [
    {"from": "old_msgs/Point", "to": "geometry_msgs/Point"}
  ],
  "topic_type_renames": [
    {
      "topic": "/imu",
      "from_type": "custom_msgs/Imu",
      "to_type": "sensor_msgs/Imu"
    }
  ]
}
```

---

### 4. filter_topics.py - Topic Filtering

Extract specific topics from large robot recordings.

```bash
# List all topics in a file
python filter_topics.py data.mcap --list

# Extract specific topics
python filter_topics.py data.mcap output.mcap --topics /camera/image_raw /imu/data

# Extract topics matching a pattern
python filter_topics.py data.mcap output.mcap --pattern "/camera/*"

# Exclude specific topics
python filter_topics.py data.mcap output.mcap --pattern "/sensors/*" --exclude "/sensors/debug/*"

# Use regex for complex matching
python filter_topics.py data.mcap output.mcap --regex "/camera/.*_raw"
```

**What you'll learn:**
- Listing and querying channels
- Pattern-based topic filtering
- Creating focused datasets from large logs

---

### 5. mcap_stats.py - Statistics Reports

Generate comprehensive statistics from robotics data files.

```bash
# Generate a full statistics report
python mcap_stats.py data.mcap

# Export statistics to JSON
python mcap_stats.py data.mcap --export stats.json

# Show more topics in the report
python mcap_stats.py data.mcap --topics 50

# Adjust high-frequency threshold
python mcap_stats.py data.mcap --freq 30
```

**What you'll learn:**
- Analyzing message frequencies per topic
- Generating human-readable reports
- Exporting data for further analysis

---

## API Overview

### Core Classes

| Class | Purpose |
|-------|---------|
| `RoboReader` | Read MCAP/ROS bag files with auto-detection |
| `RoboWriter` | Write MCAP/ROS bag files |
| `RoboRewriter` | Convert formats and apply transformations |
| `TransformBuilder` | Fluent API for building transformations |
| `ChannelInfo` | Channel/topic metadata |
| `RewriteStats` | Operation statistics |
| `RobocodecError` | Structured exception with context |

### Quick Reference

```python
import robocodec
from robocodec import RoboReader, RoboWriter, RoboRewriter, TransformBuilder

# Reading
reader = RoboReader("data.mcap")
print(f"Format: {reader.format}")
print(f"Messages: {reader.message_count}")
for channel in reader.channels():
    print(f"{channel.topic}: {channel.message_count} messages")

# Writing
writer = RoboWriter("output.mcap")
channel_id = writer.add_channel("/topic", "std_msgs/String", "cdr", None)
writer.finish()

# Transforming (fluent API)
builder = (TransformBuilder()
    .with_topic_rename("/old", "/new")
    .with_type_rename("OldMsg", "NewMsg")
    .with_topic_rename_wildcard("/foo/*", "/bar/*"))

rewriter = RoboRewriter.with_transforms("input.mcap", builder)
stats = rewriter.rewrite("output.mcap")
```

---

## Common Patterns

### Error Handling

```python
from robocodec import RobocodecError

try:
    reader = RoboReader("data.mcap")
except RobocodecError as e:
    print(f"Error: {e.message}")
    print(f"Kind: {e.kind}")
    print(f"Context: {e.context}")
```

### Listing Topics

```python
reader = RoboReader("data.mcap")
channels = reader.channels()

for ch in channels:
    print(f"{ch.topic} - {ch.message_type} ({ch.message_count} msgs)")
```

### Format Conversion

```python
# Automatically detects input format and converts based on output extension
rewriter = RoboRewriter("input.bag")
stats = rewriter.rewrite("output.mcap")
```

---

## Tips for Python Users

1. **No `--all-features` needed**: The Python bindings work without special feature flags
2. **Auto-detection**: `RoboReader` and `RoboWriter` detect format from file content/extension
3. **Method chaining**: All `TransformBuilder` methods return `self` for fluent chaining
4. **Structured errors**: `RobocodecError` provides `kind`, `context`, and `message` attributes
5. **Channel IDs**: Use returned channel IDs from `add_channel()` for writing messages

---

## Troubleshooting

### "Incompatible robocodec API detected"

**Error message:**
```
❌ Error: Incompatible robocodec API detected
Missing classes: RoboReader, RoboWriter, ...
```

**Cause:** You're using a system-wide `pip install` of robocodec instead of the local development build.

**Fix:**
```bash
# Uninstall the system package (optional)
pip uninstall robocodec

# Rebuild and install in development mode
cd /path/to/robocodec
make build-python-dev

# Run examples using the venv Python
.venv/bin/python3 examples/python/inspect_mcap.py data.mcap
```

### "module 'robocodec' has no attribute 'RoboReader'"

**Cause:** Same as above - wrong Python environment.

**Fix:** Always use the virtual environment Python:
```bash
# Direct path (recommended)
.venv/bin/python3 examples/python/inspect_mcap.py data.mcap

# Or activate first
source .venv/bin/activate
python3 examples/python/inspect_mcap.py data.mcap
```

### "No module named 'robocodec'"

**Cause:** The package hasn't been built/installed yet.

**Fix:**
```bash
make build-python-dev
```

### Examples can't find test data files

**Fix:** Use the test fixtures from the repository:
```bash
# List available test files
ls tests/fixtures/

# Run with a test file
.venv/bin/python3 examples/python/inspect_mcap.py tests/fixtures/robocodec_test_14.mcap
```
