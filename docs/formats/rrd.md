# RRD (Rerun Data) Format

## Overview

RRD (Rerun Data) is Rerun's native binary format for storing robotics time-series data.
Files use the `.rrd` extension and are optimized for Rerun's visualization workflow.

## Current Status: NOT SUPPORTED

Robocodec does **not** provide native read/write support for RRD files, and this is
an intentional design decision.

## Why Native RRD Support Is Not Provided

### 1. No Stable Public Specification

The RRD format does not have a formal, stable public specification. The format is
documented only through Rerun's source code, which is actively evolving.

### 2. Tight Coupling to Rerun's Data Model

The RRD format is fundamentally tied to Rerun's internal data structures:
- **Chunks**: Rerun's unit of storage (Arrow-encoded tables)
- **EntityPaths**: Rerun's hierarchical entity naming
- **Sorbet Schema**: Rerun's custom schema system
- **Archetypes**: Rerun's high-level data abstractions

Properly reading RRD requires implementing or depending on these subsystems.

### 3. Format Instability

The RRD format has undergone breaking changes in recent versions:
- `RRF0` - Initial version (incompatible)
- `RRF1` - Second version (incompatible)
- `RRF2` - Current version (still evolving)

Backwards compatibility is only guaranteed for one version (e.g., 0.23 can read 0.22 files).

### 4. Complexity vs. Benefit

Implementing full RRD support would require:
- Apache Arrow integration
- Protobuf message decoding
- Rerun's data model implementation
- LZ4 compression
- Custom framing protocol

This complexity outweighs the benefit given the lack of stable specification.

## Recommended Alternative: Format Conversion

For users who need to work with RRD files, we recommend using Rerun's built-in
conversion tools:

### Converting RRD to MCAP

Rerun provides CLI tools for working with RRD files. While native RRD→MCAP
conversion may not be directly available, users can:

1. **Use Rerun's Python SDK** to read RRD and export to a supported format
2. **Use Rerun Viewer** to load RRD files and export data
3. **Use Rerun's CLI** (`rerun rrd ...`) for manipulation and inspection

### Example Workflow

```bash
# Inspect an RRD file
rerun rrd info recording.rrd

# Convert using Rerun's Python SDK
python -c "
import rerun as rr
recording = rr.read_recording('recording.rrd')
# Export to MCAP or other format
"

# Then read the converted MCAP with robocodec
robocodec info converted.mcap
```

## References

- [Rerun Documentation](https://rerun.io/docs)
- [Rerun GitHub](https://github.com/rerun-io/rerun)
- [Rerun Architecture](https://github.com/rerun-io/rerun/blob/main/ARCHITECTURE.md)
- [RRD Backwards Compatibility](https://rerun.io/blog/release-0.23)

## Future Considerations

Native RRD support may be reconsidered if:

1. Rerun publishes a stable, versioned format specification
2. The format stabilizes (reaching 1.0 equivalent)
3. There is significant user demand for native support
4. A lightweight RRD decoding library becomes available

For now, using format conversion via Rerun's tools is the recommended approach.
