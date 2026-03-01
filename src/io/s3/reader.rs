// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader implementation.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};

use futures::stream::Stream;

use crate::core::{CodecError, CodecValue, DecodedMessage};
use crate::encoding::{CdrDecoder, JsonDecoder, ProtobufDecoder};
use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_ATTACHMENT, OP_ATTACHMENT_INDEX, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX,
    OP_DATA_END, OP_FOOTER, OP_HEADER, OP_MESSAGE, OP_MESSAGE_INDEX, OP_METADATA,
    OP_METADATA_INDEX, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};
use crate::io::metadata::{ChannelInfo, RawMessage, TimestampedDecodedMessage};
use crate::io::s3::{
    client::S3Client, config::S3ReaderConfig, error::FatalError, location::S3Location,
};
// Re-export streaming parsers from format modules
use crate::io::formats::bag::stream::{BagMessageRecord, StreamingBagParser};
use crate::io::formats::mcap::s3_adapter::McapS3Adapter;
use crate::io::formats::rrd::stream::{RrdMessageRecord, StreamingRrdParser};
use crate::io::streaming::StreamingParser;
use crate::io::traits::FormatReader;

/// State machine for S3 streaming reader.
///
/// The reader progresses through states as it:
/// 1. Initializes (discovers channels via two-tier approach)
/// 2. Streams message data
/// 3. Reaches end of file
#[derive(Debug, Clone)]
pub enum S3ReaderState {
    /// Initial state - about to initialize
    Initial,

    /// Ready to stream messages (channels discovered via two-tier approach)
    Ready {
        /// Channel info discovered during initialization
        channels: HashMap<u16, ChannelInfo>,
        /// Current position in the stream
        stream_position: u64,
        /// Total file size
        file_size: u64,
    },

    /// End of file reached
    Eof,

    /// Error state
    Error(String),
}

/// Schema information for MCAP summary parsing.
#[derive(Clone)]
pub struct SummarySchemaInfo {
    pub id: u16,
    pub name: String,
    pub encoding: String,
    pub data: Vec<u8>,
}

impl fmt::Display for S3ReaderState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3ReaderState::Initial => write!(f, "Initial"),
            S3ReaderState::Ready { .. } => write!(f, "Ready"),
            S3ReaderState::Eof => write!(f, "End of file"),
            S3ReaderState::Error(msg) => write!(f, "Error: {msg}"),
        }
    }
}

/// S3 streaming reader for robotics data files.
///
/// This reader provides sequential access to S3-hosted MCAP and BAG files
/// without requiring random access or local file storage.
pub struct S3Reader {
    /// S3 location being read
    location: S3Location,
    /// Configuration for the reader
    config: S3ReaderConfig,
    /// HTTP client for S3 operations
    client: S3Client,
    /// Current reader state
    state: S3ReaderState,
    /// Detected file format
    format: crate::io::metadata::FileFormat,
}

impl S3Reader {
    /// Open an S3 object for streaming with default configuration.
    ///
    /// This will:
    /// 1. Detect the file format from the key extension
    /// 2. Fetch and parse the header to discover channels
    /// 3. Prepare for streaming message data
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to open
    pub async fn open(location: S3Location) -> Result<Self, FatalError> {
        Self::open_with_config(location, S3ReaderConfig::default()).await
    }

    /// Open an S3 object for streaming with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `location` - The S3 location to open
    /// * `config` - Custom configuration for the reader
    pub async fn open_with_config(
        location: S3Location,
        config: S3ReaderConfig,
    ) -> Result<Self, FatalError> {
        // Detect format from extension
        let format = if location.is_mcap() {
            crate::io::metadata::FileFormat::Mcap
        } else if location.is_bag() {
            crate::io::metadata::FileFormat::Bag
        } else if location.is_rrd() {
            crate::io::metadata::FileFormat::Rrd
        } else {
            return Err(FatalError::InvalidFormat {
                expected: "MCAP, BAG, or RRD file",
                found: location.key().as_bytes().to_vec(),
            });
        };

        // Create client
        let client = S3Client::new(config.clone())?;

        // Create reader in initial state
        let mut reader = Self {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format,
        };

        // Initialize by scanning header
        reader.initialize().await?;

        Ok(reader)
    }

    /// Initialize the reader by fetching and parsing the header.
    async fn initialize(&mut self) -> Result<(), FatalError> {
        // Get file size first (needed for footer parsing)
        let file_size = self.client.object_size(&self.location).await?;

        // Use format-specific initialization with two-tier approach
        let (channels, stream_position) = match self.format {
            crate::io::metadata::FileFormat::Mcap => self.initialize_mcap(file_size).await?,
            crate::io::metadata::FileFormat::Bag => self.initialize_bag(file_size).await?,
            crate::io::metadata::FileFormat::Rrd => self.initialize_rrd(file_size).await?,
            _ => {
                return Err(FatalError::InvalidFormat {
                    expected: "MCAP, BAG, or RRD",
                    found: vec![],
                });
            }
        };

        // Move to ready state
        self.state = S3ReaderState::Ready {
            channels,
            stream_position,
            file_size,
        };

        Ok(())
    }

    /// Initialize MCAP reader using two-tier approach.
    async fn initialize_mcap(
        &mut self,
        file_size: u64,
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // Tier 1: Try footer-first approach (preferred)
        if let Ok(channels) = self.try_mcap_footer_first(file_size).await
            && !channels.is_empty()
        {
            return Ok((channels, 0));
        }

        // Tier 2: Fallback to scanning from beginning
        self.scan_mcap_for_metadata(file_size).await
    }

    /// Try footer-first approach for MCAP files.
    async fn try_mcap_footer_first(
        &mut self,
        file_size: u64,
    ) -> Result<HashMap<u16, ChannelInfo>, FatalError> {
        // Fetch the last part of the file to find footer
        // Footer is typically at most 1KB from the end
        let tail_size = 1024.min(file_size);
        let tail_data = self
            .client
            .fetch_tail(&self.location, tail_size, file_size)
            .await?;

        // Search backwards for MCAP magic
        let magic_pos = tail_data
            .windows(MCAP_MAGIC.len())
            .rposition(|w| w == MCAP_MAGIC);

        let magic_offset = match magic_pos {
            Some(pos) => pos,
            None => {
                // No trailing magic found, file might not have footer
                return Ok(HashMap::new());
            }
        };

        // Footer is 29 bytes before the trailing magic
        // Footer structure: summary_offset (8) + summary_section_start (8) + ...
        let footer_start = magic_offset.saturating_sub(29);
        if footer_start > tail_data.len() {
            // Footer spans beyond our tail fetch, need to fetch more
            // For now, return empty and fall back to scanning
            tracing::debug!(
                context = "scan_mcap_for_metadata",
                location = ?self.location,
                footer_start,
                tail_len = tail_data.len(),
                "Footer spans beyond tail fetch, falling back to scanning"
            );
            return Ok(HashMap::new());
        }

        // Parse footer to extract summary_offset
        let summary_offset = match self.parse_mcap_footer(&tail_data[footer_start..]) {
            Ok(offset) => offset,
            Err(e) => {
                // Footer parsing failed, fall back to scanning
                tracing::debug!(
                    context = "scan_mcap_for_metadata",
                    location = ?self.location,
                    error = %e,
                    "Footer parsing failed, falling back to scanning"
                );
                return Ok(HashMap::new());
            }
        };

        // Fetch and parse summary section
        self.parse_mcap_summary(summary_offset).await
    }

    /// Parse MCAP footer to extract summary offset.
    ///
    /// This is public for testing purposes only.
    pub fn parse_mcap_footer(&self, data: &[u8]) -> Result<u64, FatalError> {
        const FOOTER_MIN_LEN: usize = 8;

        if data.len() < FOOTER_MIN_LEN {
            return Err(FatalError::invalid_format("MCAP footer", data.to_vec()));
        }

        Ok(u64::from_le_bytes(
            data[0..8]
                .try_into()
                .expect("FOOTER_MIN_LEN ensures 8 bytes"),
        ))
    }

    /// Parse MCAP summary section to extract schemas and channels.
    async fn parse_mcap_summary(
        &mut self,
        summary_offset: u64,
    ) -> Result<HashMap<u16, ChannelInfo>, FatalError> {
        // Fetch a reasonable portion of the summary section
        // Summary contains schemas, channels, and chunk indices
        // For now, fetch up to 256KB which should be enough for most files
        let summary_fetch_size = 256 * 1024;
        let summary_data = self
            .client
            .fetch_range(&self.location, summary_offset, summary_fetch_size as u64)
            .await?;

        self.parse_mcap_summary_data(&summary_data)
    }

    /// Parse MCAP summary data from fetched bytes.
    ///
    /// This is public for testing purposes only.
    pub fn parse_mcap_summary_data(
        &self,
        data: &[u8],
    ) -> Result<HashMap<u16, ChannelInfo>, FatalError> {
        const RECORD_HEADER_LEN: usize = 9; // opcode (1) + length (8)

        let mut schemas: HashMap<u16, SummarySchemaInfo> = HashMap::new();
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut pos = 0;

        while pos + RECORD_HEADER_LEN <= data.len() {
            let opcode = data[pos];
            let length = u64::from_le_bytes(
                data[pos + 1..pos + 9]
                    .try_into()
                    .expect("RECORD_HEADER_LEN ensures 8 bytes"),
            ) as usize;
            pos += RECORD_HEADER_LEN;

            if pos + length > data.len() {
                break;
            }

            let body = &data[pos..pos + length];
            pos += length;

            match opcode {
                OP_SCHEMA => {
                    let schema = self.parse_schema_record(body)?;
                    schemas.insert(schema.id, schema);
                }
                OP_CHANNEL => {
                    self.parse_channel_record(body, &schemas, &mut channels)?;
                }
                OP_MESSAGE_INDEX | OP_CHUNK_INDEX | OP_ATTACHMENT | OP_ATTACHMENT_INDEX
                | OP_METADATA | OP_METADATA_INDEX | OP_STATISTICS | OP_SUMMARY_OFFSET
                | OP_HEADER | OP_FOOTER | OP_DATA_END | OP_CHUNK | OP_MESSAGE => {
                    // Ignore these for channel discovery
                }
                _ => break, // Unknown opcode, stop parsing
            }
        }

        Ok(channels)
    }

    /// Parse a Schema record from summary data.
    ///
    /// This is public for testing purposes only.
    pub fn parse_schema_record(&self, body: &[u8]) -> Result<SummarySchemaInfo, FatalError> {
        const SCHEMA_MIN_LEN: usize = 4;

        if body.len() < SCHEMA_MIN_LEN {
            return Err(FatalError::invalid_format(
                "MCAP Schema record",
                body.to_vec(),
            ));
        }

        let id = u16::from_le_bytes(
            body[0..2]
                .try_into()
                .expect("CHANNEL_MIN_LEN ensures 2 bytes for id"),
        );
        let name_len = u16::from_le_bytes(
            body[2..4]
                .try_into()
                .expect("CHANNEL_MIN_LEN ensures 2 bytes for name_len"),
        ) as usize;

        if body.len() < 4 + name_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema name",
                body.to_vec(),
            ));
        }

        let name = String::from_utf8(body[4..4 + name_len].to_vec()).map_err(|_| {
            FatalError::invalid_format("MCAP Schema name (invalid UTF-8)", body.to_vec())
        })?;

        let offset = 4 + name_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding length",
                body.to_vec(),
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("Length check ensures 2 bytes for encoding_len"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Schema encoding",
                body.to_vec(),
            ));
        }

        let encoding = String::from_utf8(body[offset + 2..offset + 2 + encoding_len].to_vec())
            .map_err(|_| {
                FatalError::invalid_format("MCAP Schema encoding (invalid UTF-8)", body.to_vec())
            })?;

        let data_start = offset + 2 + encoding_len;
        let data = body[data_start..].to_vec();

        Ok(SummarySchemaInfo {
            id,
            name,
            encoding,
            data,
        })
    }

    /// Parse a Channel record from summary data.
    ///
    /// This is public for testing purposes only.
    pub fn parse_channel_record(
        &self,
        body: &[u8],
        schemas: &HashMap<u16, SummarySchemaInfo>,
        channels: &mut HashMap<u16, ChannelInfo>,
    ) -> Result<(), FatalError> {
        const CHANNEL_MIN_LEN: usize = 4;

        if body.len() < CHANNEL_MIN_LEN {
            return Err(FatalError::invalid_format(
                "MCAP Channel record",
                body.to_vec(),
            ));
        }

        let id = u16::from_le_bytes(
            body[0..2]
                .try_into()
                .expect("CHANNEL_MIN_LEN ensures 2 bytes for id"),
        );
        let topic_len = u16::from_le_bytes(
            body[2..4]
                .try_into()
                .expect("CHANNEL_MIN_LEN ensures 2 bytes for topic_len"),
        ) as usize;

        if body.len() < 4 + topic_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel topic",
                body.to_vec(),
            ));
        }

        let topic = String::from_utf8(body[4..4 + topic_len].to_vec()).map_err(|_| {
            FatalError::invalid_format("MCAP Channel topic (invalid UTF-8)", body.to_vec())
        })?;

        let offset = 4 + topic_len;
        if body.len() < offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel encoding length",
                body.to_vec(),
            ));
        }

        let encoding_len = u16::from_le_bytes(
            body[offset..offset + 2]
                .try_into()
                .expect("Length check ensures 2 bytes for encoding_len"),
        ) as usize;
        if body.len() < offset + 2 + encoding_len {
            return Err(FatalError::invalid_format(
                "MCAP Channel encoding",
                body.to_vec(),
            ));
        }

        let message_encoding = String::from_utf8(
            body[offset + 2..offset + 2 + encoding_len].to_vec(),
        )
        .map_err(|_| {
            FatalError::invalid_format("MCAP Channel encoding (invalid UTF-8)", body.to_vec())
        })?;

        let schema_offset = offset + 2 + encoding_len;
        if body.len() < schema_offset + 2 {
            return Err(FatalError::invalid_format(
                "MCAP Channel schema_id",
                body.to_vec(),
            ));
        }

        let schema_id = u16::from_le_bytes(
            body[schema_offset..schema_offset + 2]
                .try_into()
                .expect("Length check ensures 2 bytes for schema_id"),
        );

        let schema = schemas.get(&schema_id);
        let schema_text = schema.and_then(|s| String::from_utf8(s.data.clone()).ok());
        let schema_data = schema.map(|s| s.data.clone());
        let schema_encoding = schema.map(|s| s.encoding.clone());

        let message_type = schema.map(|s| s.name.clone()).unwrap_or_default();

        channels.insert(
            id,
            ChannelInfo {
                id,
                topic,
                message_type,
                encoding: message_encoding,
                schema: schema_text,
                schema_data,
                schema_encoding,
                message_count: 0,
                callerid: None,
            },
        );

        Ok(())
    }

    /// Scan MCAP file from beginning to find metadata (fallback when no footer).
    async fn scan_mcap_for_metadata(
        &mut self,
        file_size: u64,
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        const INITIAL_SCAN_LIMIT: usize = 10 * 1024 * 1024; // 10MB
        const ADDITIONAL_SCAN_LIMIT: usize = 50 * 1024 * 1024; // 50MB

        let initial_limit = INITIAL_SCAN_LIMIT.min(file_size as usize);
        let data = self
            .client
            .fetch_range(&self.location, 0, initial_limit as u64)
            .await?;

        let mut adapter = McapS3Adapter::new();
        let initial_parse_failed = if let Err(e) = adapter.process_chunk(&data) {
            tracing::warn!(
                context = "scan_mcap_for_metadata",
                location = ?self.location,
                error = %e,
                "Failed to parse initial MCAP chunk for channel discovery"
            );
            true
        } else {
            false
        };

        let channels = adapter.channels();
        if !channels.is_empty() {
            return Ok((channels, 0));
        }

        // Try fetching more data
        let additional_limit =
            ADDITIONAL_SCAN_LIMIT.min(file_size.saturating_sub(initial_limit as u64) as usize);
        if additional_limit > 0 {
            let additional_data = self
                .client
                .fetch_range(
                    &self.location,
                    initial_limit as u64,
                    additional_limit as u64,
                )
                .await?;

            let _additional_parse_failed = if let Err(e) = adapter.process_chunk(&additional_data) {
                tracing::warn!(
                    context = "scan_mcap_for_metadata",
                    location = ?self.location,
                    error = %e,
                    "Failed to parse additional MCAP chunk for channel discovery"
                );
                true
            } else {
                false
            };
            return Ok((adapter.channels(), 0));
        }

        // Both initial and additional scans failed to find any channels
        if initial_parse_failed {
            return Err(FatalError::invalid_format(
                "MCAP file - unable to parse any records for channel discovery",
                data[..data.len().min(100)].to_vec(),
            ));
        }

        Ok((HashMap::new(), 0))
    }

    /// Initialize BAG reader.
    async fn initialize_bag(
        &mut self,
        file_size: u64,
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // For BAG files, use the existing header parsing approach
        // BAG files typically have connection records in the header/index section
        let header_data = self
            .client
            .fetch_header(&self.location, self.config.header_scan_limit())
            .await?;

        let (channels, stream_position) = self.parse_bag_header(&header_data)?;
        if !channels.is_empty() {
            return Ok((channels, stream_position));
        }

        // Some BAG fixtures place connection records beyond the initial scan window.
        // Fall back to a bounded streaming metadata pass without preloading the full
        // object into memory.
        let scanned_channels = self.scan_bag_for_channels(file_size).await?;
        Ok((scanned_channels, 0))
    }

    async fn scan_bag_for_channels(
        &self,
        file_size: u64,
    ) -> Result<HashMap<u16, ChannelInfo>, FatalError> {
        let mut parser = StreamingBagParser::new();
        let mut offset = 0_u64;

        while offset < file_size {
            let remaining = file_size - offset;
            let chunk_size = (self.config.max_chunk_size() as u64).min(remaining);
            if chunk_size == 0 {
                break;
            }

            let chunk = self
                .client
                .fetch_range(&self.location, offset, chunk_size)
                .await?;
            if chunk.is_empty() {
                break;
            }

            parser.parse_chunk(&chunk).map_err(|e| {
                FatalError::io_error(format!(
                    "Failed to stream-scan BAG metadata for channel discovery: {e}"
                ))
            })?;

            offset += chunk.len() as u64;
        }

        Ok(parser.channels())
    }

    /// Initialize RRD reader.
    async fn initialize_rrd(
        &mut self,
        _file_size: u64,
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // For RRD files, parse the header to discover channels
        // RRD files have a fixed-size header with format metadata
        let header_data = self
            .client
            .fetch_header(&self.location, 128) // RRD header is 32 bytes
            .await?;

        self.parse_rrd_header(&header_data)
    }

    /// Parse RRD header to discover channels.
    fn parse_rrd_header(
        &self,
        data: &[u8],
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        use crate::io::formats::rrd::constants::{RRD_MAGIC, STREAM_HEADER_SIZE};

        if data.len() < STREAM_HEADER_SIZE {
            return Err(FatalError::invalid_format(
                "RRD header",
                data[..data.len().min(20)].to_vec(),
            ));
        }

        let magic = &data[0..4];
        if magic != RRD_MAGIC {
            return Err(FatalError::invalid_format(
                "RRD magic (expected RRF2)",
                magic.to_vec(),
            ));
        }

        // Use streaming parser to discover channels
        let mut parser = StreamingRrdParser::new();
        parser.parse_chunk(data).map_err(|e| {
            FatalError::io_error(format!(
                "Failed to parse RRD header for channel discovery: {e}"
            ))
        })?;

        Ok((parser.channels().clone(), 0))
    }

    /// Parse MCAP header to discover channels.
    ///
    /// This is a simple method used for testing. For production use,
    /// prefer the two-tier approach (`try_mcap_footer_first` + `scan_mcap_for_metadata`).
    pub fn parse_mcap_header(
        &self,
        data: &[u8],
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // MCAP magic: \x89MCAP (8 bytes total as defined in mcap::constants::MCAP_MAGIC)
        if data.len() < MCAP_MAGIC.len() {
            return Err(FatalError::invalid_format(
                "MCAP header (8 bytes minimum)",
                data.to_vec(),
            ));
        }

        let magic = &data[0..MCAP_MAGIC.len()];
        if magic != MCAP_MAGIC {
            return Err(FatalError::invalid_format(
                "MCAP magic (\\x89MCAP)",
                magic.to_vec(),
            ));
        }

        // Use mcap crate-based adapter to discover channels
        let mut adapter = McapS3Adapter::new();
        // Parse the header data to discover channels
        if let Err(e) = adapter.process_chunk(data) {
            return Err(FatalError::io_error(format!(
                "Failed to parse MCAP header for channel discovery: {e}"
            )));
        }
        Ok((adapter.channels(), 0))
    }

    /// Parse BAG header to discover channels.
    fn parse_bag_header(
        &self,
        data: &[u8],
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        use crate::io::formats::bag::stream::BAG_MAGIC_PREFIX;

        // BAG magic: #ROSBAG V
        if data.len() < BAG_MAGIC_PREFIX.len() {
            return Err(FatalError::invalid_format(
                "BAG header (9 bytes minimum)",
                data.to_vec(),
            ));
        }

        let magic = &data[0..BAG_MAGIC_PREFIX.len()];
        if magic != BAG_MAGIC_PREFIX {
            return Err(FatalError::invalid_format(
                "BAG magic (#ROSBAG V)",
                magic.to_vec(),
            ));
        }

        // Use streaming parser to discover connections
        let mut parser = StreamingBagParser::new();
        // Parse the header data to discover connections
        parser.parse_chunk(data).map_err(|e| {
            FatalError::io_error(format!(
                "Failed to parse BAG header for channel discovery: {e}"
            ))
        })?;
        Ok((parser.channels(), 0))
    }

    /// Get the current reader state.
    #[must_use]
    pub fn state(&self) -> &S3ReaderState {
        &self.state
    }

    /// Get the S3 location.
    #[must_use]
    pub fn location(&self) -> &S3Location {
        &self.location
    }

    /// Get the file format.
    #[must_use]
    pub fn format(&self) -> crate::io::metadata::FileFormat {
        self.format
    }

    /// Get the channels discovered during header scan.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match &self.state {
            S3ReaderState::Ready { channels, .. } => channels,
            _ => EMPTY_CHANNELS.get_or_init(HashMap::new),
        }
    }

    /// Create an iterator over messages in the file.
    #[must_use]
    pub fn iter_messages(&self) -> S3MessageStream<'_> {
        S3MessageStream::new(self)
    }

    /// Check if the reader has more messages.
    #[must_use]
    pub fn has_more(&self) -> bool {
        !matches!(self.state, S3ReaderState::Eof | S3ReaderState::Error(_))
    }
}

impl FormatReader for S3Reader {
    #[cfg(feature = "remote")]
    async fn open_from_transport(
        _transport: Box<dyn crate::io::transport::Transport>,
        _path: String,
    ) -> crate::Result<Self>
    where
        Self: Sized,
    {
        // S3Reader requires async initialization and S3-specific configuration
        // It cannot be created from a generic transport
        // Use S3Reader::open() or S3Reader::open_with_config() instead
        Err(CodecError::unsupported(
            "S3Reader requires S3-specific initialization. Use S3Reader::open() or S3Reader::open_with_config() instead.",
        ))
    }

    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match &self.state {
            S3ReaderState::Ready { channels, .. } => channels,
            _ => EMPTY_CHANNELS.get_or_init(HashMap::new),
        }
    }

    fn message_count(&self) -> u64 {
        // Streaming reader doesn't pre-count messages
        // Returns 0 as the count is unknown until full iteration
        0
    }

    fn start_time(&self) -> Option<u64> {
        // Streaming reader doesn't track time bounds during header scan
        // This could be enhanced in Phase 5 by tracking timestamps in the streaming parser
        None
    }

    fn end_time(&self) -> Option<u64> {
        // Streaming reader doesn't track time bounds during header scan
        None
    }

    fn path(&self) -> &str {
        self.location.key()
    }

    fn format(&self) -> crate::io::metadata::FileFormat {
        self.format
    }

    fn file_size(&self) -> u64 {
        match &self.state {
            S3ReaderState::Ready { file_size, .. } => *file_size,
            _ => 0,
        }
    }

    fn iter_raw_boxed(&self) -> crate::Result<crate::io::traits::RawMessageIter<'_>> {
        Ok(Box::new(S3RawMessageIter::new(self)))
    }

    fn decoded_with_timestamp_boxed(
        &self,
    ) -> crate::Result<Box<dyn crate::io::traits::DecodedMessageIterator + Send + Sync + '_>> {
        Ok(Box::new(S3DecodedMessageSyncIter::new(self)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Empty channel map singleton.
static EMPTY_CHANNELS: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();

/// Test-only constructor for creating `S3Reader` instances directly.
///
/// This is public for testing purposes only. Normal usage should use
/// `S3Reader::open()` or `S3Reader::open_with_config()`.
pub struct S3ReaderConstructor {
    pub location: S3Location,
    pub config: S3ReaderConfig,
    pub client: S3Client,
}

impl S3ReaderConstructor {
    #[must_use]
    pub fn new_mcap() -> Self {
        Self {
            location: S3Location::new("test-bucket", "test.mcap"),
            config: S3ReaderConfig::default(),
            client: S3Client::default_client().expect("failed to create default S3 client"),
        }
    }

    #[must_use]
    pub fn build(&self) -> S3Reader {
        S3Reader {
            location: self.location.clone(),
            config: self.config.clone(),
            client: self.client.clone(),
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        }
    }

    #[must_use]
    pub fn build_bag(&self) -> S3Reader {
        S3Reader {
            location: S3Location::new("test-bucket", "test.bag"),
            config: self.config.clone(),
            client: self.client.clone(),
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        }
    }
}

/// Async stream over messages in an S3 file.
///
/// This stream fetches data in chunks as needed, providing constant
/// memory usage regardless of file size. Uses async iteration pattern
/// to fetch from S3 without blocking.
///
/// This stream borrows from the parent `S3Reader`, avoiding unnecessary
/// cloning of client, location, and config.
pub struct S3MessageStream<'a> {
    /// Reference to the parent reader
    reader: &'a S3Reader,

    /// Format-specific streaming parser state
    mcap_adapter: Option<McapS3Adapter>,
    bag_parser: Option<StreamingBagParser>,
    rrd_parser: Option<StreamingRrdParser>,
    channels: HashMap<u16, ChannelInfo>,

    /// Current chunk of message data being processed
    pending_messages: VecDeque<ParsedMessage>,

    /// Current stream position
    stream_position: u64,

    /// File size (cached from reader to avoid repeated access)
    file_size: u64,

    /// Whether we've reached EOF
    eof: bool,
}

/// Parsed message from MCAP, BAG, or RRD format.
enum ParsedMessage {
    Mcap(crate::io::formats::mcap::s3_adapter::MessageRecord),
    Bag(BagMessageRecord),
    Rrd(RrdMessageRecord),
}

impl ParsedMessage {
    /// Get the channel ID for this message.
    fn channel_id(&self) -> u32 {
        match self {
            ParsedMessage::Mcap(m) => u32::from(m.channel_id),
            ParsedMessage::Bag(b) => b.conn_id,
            ParsedMessage::Rrd(_r) => 0,
        }
    }

    /// Get the message data.
    fn data(self) -> Vec<u8> {
        match self {
            ParsedMessage::Mcap(m) => m.data,
            ParsedMessage::Bag(b) => b.data,
            ParsedMessage::Rrd(r) => r.data,
        }
    }

    /// Convert to a raw message with timing metadata.
    fn into_raw(self) -> RawMessage {
        match self {
            ParsedMessage::Mcap(m) => {
                RawMessage::new(m.channel_id, m.log_time, m.publish_time, m.data)
                    .with_sequence(m.sequence)
            }
            ParsedMessage::Bag(b) => {
                RawMessage::new(b.conn_id as u16, b.log_time, b.log_time, b.data)
            }
            ParsedMessage::Rrd(r) => {
                RawMessage::new(0, r.index, r.index, r.data).with_sequence(r.index)
            }
        }
    }
}

impl<'a> S3MessageStream<'a> {
    /// Create a new message stream.
    fn new(reader: &'a S3Reader) -> Self {
        let (channels, stream_position, file_size) = match &reader.state {
            S3ReaderState::Ready {
                channels,
                stream_position,
                file_size,
            } => (channels.clone(), *stream_position, *file_size),
            _ => (HashMap::new(), 0, 0),
        };

        let (mcap_adapter, bag_parser, rrd_parser) = match reader.format {
            crate::io::metadata::FileFormat::Mcap => {
                // Adapter already initialized during header scan, create a new one for streaming
                (Some(McapS3Adapter::new()), None, None)
            }
            crate::io::metadata::FileFormat::Bag => (None, Some(StreamingBagParser::new()), None),
            crate::io::metadata::FileFormat::Rrd => (None, None, Some(StreamingRrdParser::new())),
            _ => (None, None, None),
        };

        Self {
            reader,
            mcap_adapter,
            bag_parser,
            rrd_parser,
            channels,
            pending_messages: VecDeque::new(),
            stream_position,
            file_size,
            eof: false,
        }
    }
}

impl Stream for S3MessageStream<'_> {
    type Item = Result<(ChannelInfo, Vec<u8>), FatalError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Return pending message if available, filtering out unknown channels
        while let Some(msg) = self.pending_messages.pop_front() {
            let channel_id = msg.channel_id() as u16;
            let data = msg.data();

            if let Some(channel_info) = self.channels.get(&channel_id).cloned() {
                return Poll::Ready(Some(Ok((channel_info, data))));
            }
            tracing::warn!(
                context = "S3MessageStream",
                channel_id,
                "Unknown channel ID"
            );
        }

        // Check if we've reached EOF
        if self.eof || self.stream_position >= self.file_size {
            return Poll::Ready(None);
        }

        // Mark EOF - use next_message() for proper async chunk fetching
        Poll::Ready(None)
    }
}

// Block on the stream for synchronous usage
impl S3MessageStream<'_> {
    /// Get the next raw message with channel metadata.
    async fn next_raw_message(&mut self) -> Option<Result<(RawMessage, ChannelInfo), FatalError>> {
        loop {
            // Return pending message if available, filtering out unknown channels
            if let Some(msg) = self.pending_messages.pop_front() {
                let channel_id = msg.channel_id() as u16;

                if let Some(channel_info) = self.channels.get(&channel_id).cloned() {
                    return Some(Ok((msg.into_raw(), channel_info)));
                }
                tracing::warn!(
                    context = "S3MessageStream",
                    channel_id,
                    "Unknown channel ID"
                );
                continue;
            }

            // No more pending messages - check if we should fetch more or return EOF
            if self.eof || self.stream_position >= self.file_size {
                return None;
            }

            // Fetch next chunk
            let remaining = self.file_size - self.stream_position;
            let chunk_size = (self.reader.config.max_chunk_size() as u64).min(remaining);

            if chunk_size == 0 {
                self.eof = true;
                return None;
            }

            match self
                .reader
                .client
                .fetch_range(&self.reader.location, self.stream_position, chunk_size)
                .await
            {
                Ok(chunk_data) if chunk_data.is_empty() => {
                    self.eof = true;
                    return None;
                }
                Ok(chunk_data) => {
                    if let Err(e) = self.parse_chunk(&chunk_data) {
                        self.eof = true;
                        return Some(Err(e));
                    }

                    self.stream_position += chunk_data.len() as u64;
                    self.eof = self.stream_position >= self.file_size;
                }
                Err(e) => {
                    self.eof = true;
                    return Some(Err(e));
                }
            }
        }
    }

    /// Get the next message synchronously (blocking).
    ///
    /// This method is provided for convenience when async runtime is available.
    /// In an async context, use `StreamExt::next()` instead.
    pub async fn next_message(&mut self) -> Option<Result<(ChannelInfo, Vec<u8>), FatalError>> {
        self.next_raw_message()
            .await
            .map(|result| result.map(|(raw, channel)| (channel, raw.data)))
    }
}

impl S3MessageStream<'_> {
    fn parse_chunk(&mut self, chunk_data: &[u8]) -> Result<(), FatalError> {
        match self.reader.format {
            crate::io::metadata::FileFormat::Mcap => {
                if let Some(ref mut adapter) = self.mcap_adapter {
                    match adapter.process_chunk(chunk_data) {
                        Ok(msgs) => {
                            self.pending_messages
                                .extend(msgs.into_iter().map(ParsedMessage::Mcap));
                        }
                        Err(e) => {
                            tracing::warn!(
                                context = "S3MessageStream::parse_chunk",
                                location = ?self.reader.location,
                                offset = self.stream_position,
                                error = %e,
                                "MCAP parse error"
                            );
                            return Err(e);
                        }
                    }
                }
            }
            crate::io::metadata::FileFormat::Bag => {
                if let Some(ref mut parser) = self.bag_parser {
                    match parser.parse_chunk(chunk_data) {
                        Ok(msgs) => {
                            // BAG connections may appear after the initial header scan,
                            // so merge channels discovered during streaming to avoid
                            // dropping messages with newly seen connection IDs.
                            self.channels.extend(parser.channels());
                            self.pending_messages
                                .extend(msgs.into_iter().map(ParsedMessage::Bag));
                        }
                        Err(e) => {
                            tracing::warn!(
                                context = "S3MessageStream::parse_chunk",
                                location = ?self.reader.location,
                                offset = self.stream_position,
                                error = %e,
                                "BAG parse error"
                            );
                            return Err(e);
                        }
                    }
                }
            }
            crate::io::metadata::FileFormat::Rrd => {
                if let Some(ref mut parser) = self.rrd_parser {
                    match parser.parse_chunk(chunk_data) {
                        Ok(msgs) => {
                            self.channels.extend(parser.channels().clone());
                            self.pending_messages
                                .extend(msgs.into_iter().map(ParsedMessage::Rrd));
                        }
                        Err(e) => {
                            tracing::warn!(
                                context = "S3MessageStream::parse_chunk",
                                location = ?self.reader.location,
                                offset = self.stream_position,
                                error = %e,
                                "RRD parse error"
                            );
                            return Err(e);
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

/// Synchronous wrapper over `S3MessageStream` raw iteration.
struct S3RawMessageIter<'a> {
    stream: S3MessageStream<'a>,
    finished: bool,
}

impl<'a> S3RawMessageIter<'a> {
    fn new(reader: &'a S3Reader) -> Self {
        Self {
            stream: S3MessageStream::new(reader),
            finished: false,
        }
    }
}

impl Iterator for S3RawMessageIter<'_> {
    type Item = crate::Result<(RawMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let runtime = crate::io::reader::shared_runtime();
        match runtime.block_on(self.stream.next_raw_message()) {
            Some(Ok(item)) => Some(Ok(item)),
            Some(Err(err)) => {
                self.finished = true;
                Some(Err(err.into()))
            }
            None => {
                self.finished = true;
                None
            }
        }
    }
}

/// Synchronous wrapper over `S3MessageStream` decoded iteration.
struct S3DecodedMessageIter<'a> {
    raw_iter: S3RawMessageIter<'a>,
    format: crate::io::metadata::FileFormat,
    cdr_decoder: Arc<CdrDecoder>,
    proto_decoder: Arc<ProtobufDecoder>,
    json_decoder: Arc<JsonDecoder>,
    schema_cache: HashMap<String, crate::schema::MessageSchema>,
}

impl<'a> S3DecodedMessageIter<'a> {
    fn new(reader: &'a S3Reader) -> Self {
        Self {
            raw_iter: S3RawMessageIter::new(reader),
            format: reader.format,
            cdr_decoder: Arc::new(CdrDecoder::new()),
            proto_decoder: Arc::new(ProtobufDecoder::new()),
            json_decoder: Arc::new(JsonDecoder::new()),
            schema_cache: HashMap::new(),
        }
    }

    fn get_or_parse_schema(
        &mut self,
        message_type: &str,
        schema_definition: &str,
    ) -> std::result::Result<crate::schema::MessageSchema, CodecError> {
        let cache_key = format!("{message_type}\n{schema_definition}");
        if let Some(schema) = self.schema_cache.get(&cache_key) {
            return Ok(schema.clone());
        }

        let schema = crate::schema::parse_schema(message_type, schema_definition)
            .map_err(|e| CodecError::parse(message_type, format!("Failed to parse schema: {e}")))?;
        self.schema_cache.insert(cache_key, schema.clone());
        Ok(schema)
    }

    fn decode_message(
        &mut self,
        raw_msg: &RawMessage,
        channel_info: &ChannelInfo,
    ) -> crate::Result<DecodedMessage> {
        match self.format {
            crate::io::metadata::FileFormat::Bag => {
                let schema = channel_info.schema.as_deref().ok_or_else(|| {
                    CodecError::parse(
                        &channel_info.message_type,
                        "No schema available (message_definition not found in connection)",
                    )
                })?;

                let parsed_schema = self.get_or_parse_schema(&channel_info.message_type, schema)?;

                self.cdr_decoder
                    .decode_headerless_ros1(
                        &parsed_schema,
                        &raw_msg.data,
                        Some(&channel_info.message_type),
                    )
                    .map_err(|e| {
                        CodecError::parse(
                            &channel_info.message_type,
                            format!(
                                "Decode failed for topic '{}' with log_time {}: {}",
                                channel_info.topic, raw_msg.log_time, e
                            ),
                        )
                    })
            }
            crate::io::metadata::FileFormat::Rrd => {
                let mut decoded = DecodedMessage::new();
                decoded.insert("data".to_string(), CodecValue::Bytes(raw_msg.data.clone()));
                Ok(decoded)
            }
            crate::io::metadata::FileFormat::Mcap | crate::io::metadata::FileFormat::Unknown => {
                match channel_info.encoding.as_str() {
                    "protobuf" => self
                        .proto_decoder
                        .decode(&raw_msg.data)
                        .map_err(|e| CodecError::parse("Protobuf", e.to_string())),
                    "json" => {
                        let json_str = std::str::from_utf8(&raw_msg.data).map_err(|e| {
                            CodecError::parse("JSON", format!("Invalid UTF-8: {e}"))
                        })?;
                        self.json_decoder
                            .decode(json_str)
                            .map_err(|e| CodecError::parse("JSON", e.to_string()))
                    }
                    _ => {
                        let schema = channel_info.schema.as_deref().ok_or_else(|| {
                            CodecError::parse(
                                &channel_info.message_type,
                                "No schema available for CDR decode",
                            )
                        })?;
                        let parsed_schema =
                            self.get_or_parse_schema(&channel_info.message_type, schema)?;
                        self.cdr_decoder
                            .decode(
                                &parsed_schema,
                                &raw_msg.data,
                                Some(&channel_info.message_type),
                            )
                            .map_err(|e| {
                                CodecError::parse(
                                    "CDR",
                                    format!("{}: {}", channel_info.message_type, e),
                                )
                            })
                    }
                }
            }
        }
    }
}

impl Iterator for S3DecodedMessageIter<'_> {
    type Item = crate::Result<(TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (raw_msg, channel_info) = match self.raw_iter.next()? {
            Ok(item) => item,
            Err(err) => return Some(Err(err)),
        };

        let decoded = match self.decode_message(&raw_msg, &channel_info) {
            Ok(msg) => msg,
            Err(err) => return Some(Err(err)),
        };

        Some(Ok((
            TimestampedDecodedMessage {
                message: decoded,
                log_time: raw_msg.log_time,
                publish_time: raw_msg.publish_time,
            },
            channel_info,
        )))
    }
}

/// Sync wrapper for decoded iteration.
struct S3DecodedMessageSyncIter<'a> {
    inner: Mutex<S3DecodedMessageIter<'a>>,
}

impl<'a> S3DecodedMessageSyncIter<'a> {
    fn new(reader: &'a S3Reader) -> Self {
        Self {
            inner: Mutex::new(S3DecodedMessageIter::new(reader)),
        }
    }
}

impl Iterator for S3DecodedMessageSyncIter<'_> {
    type Item = crate::Result<(TimestampedDecodedMessage, ChannelInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        let iter = match self.inner.get_mut() {
            Ok(iter) => iter,
            Err(poisoned) => poisoned.into_inner(),
        };
        iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_reader_state_display() {
        assert_eq!(format!("{}", S3ReaderState::Initial), "Initial");
        assert_eq!(format!("{}", S3ReaderState::Eof), "End of file");
        assert_eq!(
            format!("{}", S3ReaderState::Error("test".to_string())),
            "Error: test"
        );
    }

    #[test]
    fn test_parse_mcap_header_valid() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid MCAP header (using the actual MCAP_MAGIC constant)
        let data = MCAP_MAGIC.to_vec();

        let result = reader.parse_mcap_header(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_mcap_header_parse_failure_propagates() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid magic + malformed Schema record to trigger adapter parse error
        let mut data = MCAP_MAGIC.to_vec();
        data.push(OP_SCHEMA);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0x00);

        let result = reader.parse_mcap_header(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mcap_header_invalid_magic() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Invalid MCAP header
        let data = b"INVALID_MAGIC";

        let result = reader.parse_mcap_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mcap_header_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Too short (less than 8 bytes)
        let data = b"\x89\x4D\x43\x41\x50";

        let result = reader.parse_mcap_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bag_header_valid() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        };

        // Valid BAG header - #ROSBAG V2.0\n
        let data = b"#ROSBAG V2.0\n";

        let result = reader.parse_bag_header(data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_bag_header_invalid_magic() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        };

        // Invalid BAG header
        let data = b"INVALID_MAGIC";

        let result = reader.parse_bag_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_reader_getters() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert_eq!(reader.location().bucket(), "bucket");
        assert_eq!(reader.format(), crate::io::metadata::FileFormat::Mcap);
        assert!(matches!(reader.state(), S3ReaderState::Initial));
    }

    #[test]
    fn test_message_stream_new() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 1000,
                file_size: 10000,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let stream = S3MessageStream::new(&reader);
        assert_eq!(stream.stream_position, 1000);
    }

    // =========================================================================
    // parse_mcap_footer tests
    // =========================================================================

    #[test]
    fn test_parse_mcap_footer_valid() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid MCAP footer data (summary_offset + summary_section_start + summary_crc)
        // summary_offset = 1000 (8 bytes)
        // summary_section_start = 500 (8 bytes)
        // summary_crc = 0x12345678 (4 bytes)
        let mut data = vec![0u8; 20];
        data[0..8].copy_from_slice(&1000u64.to_le_bytes());
        data[8..16].copy_from_slice(&500u64.to_le_bytes());
        data[16..20].copy_from_slice(&0x78563412u32.to_le_bytes());

        let result = reader.parse_mcap_footer(&data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1000);
    }

    #[test]
    fn test_parse_mcap_footer_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Too short - less than 8 bytes
        let data = b"short";

        let result = reader.parse_mcap_footer(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mcap_footer_edge_cases() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // 8 bytes - value 42 in little endian
        let data = b"\x2a\x00\x00\x00\x00\x00\x00\x00";
        let result = reader.parse_mcap_footer(data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);

        // 7 bytes - too short
        let data = b"short7";
        let result = reader.parse_mcap_footer(data);
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_schema_record tests
    // =========================================================================

    #[test]
    fn test_parse_schema_record_valid() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid schema record: id (2) + name_len (2) + "Msg" (3) +
        //                     encoding_len (2) + "x2" (2) + data
        // Note: Everything after encoding is data
        let mut body = vec![0u8; 2]; // id = 0
        body.extend_from_slice(&(3u16).to_le_bytes()); // name_len = 3
        body.extend_from_slice(b"Msg"); // name (3 bytes)
        body.extend_from_slice(&(2u16).to_le_bytes()); // encoding_len = 2
        body.extend_from_slice(b"x2"); // encoding (2 bytes)
        body.extend_from_slice(b"data"); // data (4 bytes)

        let result = reader.parse_schema_record(&body);
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.id, 0);
        assert_eq!(schema.name, "Msg");
        assert_eq!(schema.encoding, "x2");
        assert_eq!(schema.data, b"data");
    }

    #[test]
    fn test_parse_schema_record_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Too short - less than 4 bytes
        let body = b"abc";

        let result = reader.parse_schema_record(&body[..]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_schema_record_incomplete_name() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Claims name_len = 10 but only provides 4 bytes
        let mut body = vec![0u8; 2];
        body.extend_from_slice(&(10u16).to_le_bytes());
        body.extend_from_slice(b"short");

        let result = reader.parse_schema_record(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_schema_record_invalid_utf8() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Invalid UTF-8 in name
        let mut body = vec![0u8; 2];
        body.extend_from_slice(&(4u16).to_le_bytes());
        body.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // Invalid UTF-8

        let result = reader.parse_schema_record(&body);
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_channel_record tests
    // =========================================================================

    #[test]
    fn test_parse_channel_record_valid() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid channel record: id (2) + topic_len (2) + "/topic" (6) +
        //                       encoding_len (2) + "cdr" (3) + schema_id (4)
        let schemas = HashMap::new();
        let mut channels = HashMap::new();

        let mut body = vec![1u8, 0u8]; // id = 1
        body.extend_from_slice(&(6u16).to_le_bytes()); // topic_len
        body.extend_from_slice(b"/topic"); // topic
        body.extend_from_slice(&(3u16).to_le_bytes()); // encoding_len
        body.extend_from_slice(b"cdr"); // encoding
        body.extend_from_slice(&(0u32).to_le_bytes()); // schema_id = 0

        let result = reader.parse_channel_record(&body, &schemas, &mut channels);
        assert!(result.is_ok());
        assert_eq!(channels.len(), 1);
        let channel = channels.get(&1).unwrap();
        assert_eq!(channel.topic, "/topic");
        assert_eq!(channel.encoding, "cdr");
    }

    #[test]
    fn test_parse_channel_record_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let schemas = HashMap::new();
        let mut channels = HashMap::new();

        // Too short - less than 4 bytes
        let body = b"abc";

        let result = reader.parse_channel_record(&body[..], &schemas, &mut channels);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_channel_record_incomplete_topic() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let schemas = HashMap::new();
        let mut channels = HashMap::new();

        // Claims topic_len = 10 but only provides 4 bytes
        let mut body = vec![1u8, 0u8]; // id = 1
        body.extend_from_slice(&(10u16).to_le_bytes()); // topic_len
        body.extend_from_slice(b"shrt"); // Not enough bytes

        let result = reader.parse_channel_record(&body, &schemas, &mut channels);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_channel_record_invalid_topic_utf8() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let schemas = HashMap::new();
        let mut channels = HashMap::new();

        // Invalid UTF-8 in topic
        let mut body = vec![1u8, 0u8]; // id = 1
        body.extend_from_slice(&(4u16).to_le_bytes()); // topic_len
        body.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // Invalid UTF-8

        let result = reader.parse_channel_record(&body, &schemas, &mut channels);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_channel_record_incomplete_encoding() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let schemas = HashMap::new();
        let mut channels = HashMap::new();

        // Valid topic but incomplete encoding
        let mut body = vec![1u8, 0u8]; // id = 1
        body.extend_from_slice(&(6u16).to_le_bytes()); // topic_len
        body.extend_from_slice(b"/topic"); // topic
        body.extend_from_slice(&(10u16).to_le_bytes()); // encoding_len
        body.extend_from_slice(b"shrt"); // Not enough bytes

        let result = reader.parse_channel_record(&body, &schemas, &mut channels);
        assert!(result.is_err());
    }

    // =========================================================================
    // RRD format detection and parsing tests
    // =========================================================================

    #[test]
    fn test_reader_rrd_format_detection() {
        let location = S3Location::new("bucket", "file.rrd");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location: location.clone(),
            config,
            client: S3Client::default_client().expect("failed to create default S3 client"),
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Rrd,
        };

        assert_eq!(reader.format(), crate::io::metadata::FileFormat::Rrd);
        assert!(location.is_rrd());
    }

    #[test]
    fn test_parse_bag_header_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        };

        // Too short - less than length of "#ROSBAG V"
        let data = b"#ROS";

        let result = reader.parse_bag_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bag_header_wrong_version() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        };

        // Wrong prefix entirely (not "#ROSBAG V")
        let data = b"#ROSDBAG V2.0\n";

        let result = reader.parse_bag_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_bag_header_parse_failure_propagates() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Bag,
        };

        // Valid BAG magic/version + oversized record header length (> 1MB)
        let mut data = b"#ROSBAG V2.0\n".to_vec();
        data.extend_from_slice(&(2 * 1024 * 1024u32).to_le_bytes());

        let result = reader.parse_bag_header(&data);
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_mcap_summary_data tests
    // =========================================================================

    #[test]
    fn test_parse_mcap_summary_data_empty() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let data = b"";
        let result = reader.parse_mcap_summary_data(data);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_mcap_summary_data_too_short_for_header() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Less than 9 bytes (opcode + length)
        let data = b"short";

        let result = reader.parse_mcap_summary_data(data);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_mcap_summary_data_unknown_opcode() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Unknown opcode (0xFF) with valid length should stop parsing
        let data = vec![0xFFu8, 10, 0, 0, 0, 0, 0, 0, 0]; // opcode + length
        // No body data needed for unknown opcode test

        let result = reader.parse_mcap_summary_data(&data);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_mcap_summary_data_incomplete_record() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // OP_SCHEMA (0x03) with length that exceeds data
        let mut data = vec![0x03u8]; // opcode
        data.extend_from_slice(&100u64.to_le_bytes()); // length = 100
        data.extend_from_slice(b"short"); // only 5 bytes of data

        let result = reader.parse_mcap_summary_data(&data);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_parse_mcap_summary_data_malformed_schema_fails_fast() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // OP_SCHEMA with body shorter than minimum (4 bytes)
        let mut data = vec![OP_SCHEMA];
        data.extend_from_slice(&3u64.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3]);

        let result = reader.parse_mcap_summary_data(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_mcap_summary_data_malformed_channel_fails_fast() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // OP_CHANNEL with body shorter than minimum (4 bytes)
        let mut data = vec![OP_CHANNEL];
        data.extend_from_slice(&3u64.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3]);

        let result = reader.parse_mcap_summary_data(&data);
        assert!(result.is_err());
    }

    // =========================================================================
    // parse_rrd_header tests
    // =========================================================================

    #[test]
    fn test_parse_rrd_header_valid() {
        use crate::io::formats::rrd::constants::RRD_MAGIC;
        use crate::io::formats::rrd::constants::SERIALIZER_PROTOBUF;
        use crate::io::formats::rrd::constants::STREAM_HEADER_SIZE;

        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.rrd");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Rrd,
        };

        // Valid RRD header
        let mut data = vec![0u8; STREAM_HEADER_SIZE];
        data[0..4].copy_from_slice(RRD_MAGIC);
        data[9] = SERIALIZER_PROTOBUF;

        let result = reader.parse_rrd_header(&data);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_rrd_header_too_short() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.rrd");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Rrd,
        };

        // Too short
        let data = b"short";

        let result = reader.parse_rrd_header(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rrd_header_invalid_magic() {
        use crate::io::formats::rrd::constants::STREAM_HEADER_SIZE;

        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.rrd");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Rrd,
        };

        // Invalid magic
        let mut data = vec![0u8; STREAM_HEADER_SIZE];
        data[0..4].copy_from_slice(b"BAD!");

        let result = reader.parse_rrd_header(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rrd_header_parse_failure_propagates() {
        use crate::io::formats::rrd::constants::STREAM_HEADER_SIZE;

        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.rrd");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Rrd,
        };

        // Valid magic and size, but non-zero reserved bytes should fail parser
        let mut data = vec![0u8; STREAM_HEADER_SIZE];
        data[0..4].copy_from_slice(b"RRF2");
        data[8] = 0; // compression off
        data[9] = 2; // protobuf serializer
        data[10] = 1; // reserved must be 0

        let result = reader.parse_rrd_header(&data);
        assert!(result.is_err());
    }

    // =========================================================================
    // ParsedMessage::channel_id tests
    // =========================================================================

    #[test]
    fn test_parsed_message_channel_id() {
        use crate::io::formats::bag::stream::BagMessageRecord;
        use crate::io::formats::mcap::s3_adapter::MessageRecord;
        use crate::io::formats::rrd::stream::{MessageKind, RrdMessageRecord};

        let mcap_msg = ParsedMessage::Mcap(MessageRecord {
            channel_id: 42,
            log_time: 0,
            publish_time: 0,
            data: vec![],
            sequence: 0,
        });
        assert_eq!(mcap_msg.channel_id(), 42);

        let bag_msg = ParsedMessage::Bag(BagMessageRecord {
            conn_id: 99,
            log_time: 0,
            data: vec![],
        });
        assert_eq!(bag_msg.channel_id(), 99);

        let rrd_msg = ParsedMessage::Rrd(RrdMessageRecord {
            kind: MessageKind::ArrowMsg,
            topic: "/test".to_string(),
            data: vec![],
            index: 5,
        });
        assert_eq!(rrd_msg.channel_id(), 0);
    }

    #[test]
    fn test_parsed_message_data() {
        use crate::io::formats::bag::stream::BagMessageRecord;
        use crate::io::formats::mcap::s3_adapter::MessageRecord;
        use crate::io::formats::rrd::stream::{MessageKind, RrdMessageRecord};

        let mcap_msg = ParsedMessage::Mcap(MessageRecord {
            channel_id: 1,
            log_time: 0,
            publish_time: 0,
            data: vec![1, 2, 3],
            sequence: 0,
        });
        assert_eq!(mcap_msg.data(), vec![1, 2, 3]);

        let bag_msg = ParsedMessage::Bag(BagMessageRecord {
            conn_id: 2,
            log_time: 0,
            data: vec![4, 5, 6],
        });
        assert_eq!(bag_msg.data(), vec![4, 5, 6]);

        let rrd_msg = ParsedMessage::Rrd(RrdMessageRecord {
            kind: MessageKind::ArrowMsg,
            topic: "/test".to_string(),
            data: vec![7, 8, 9],
            index: 0,
        });
        assert_eq!(rrd_msg.data(), vec![7, 8, 9]);
    }

    // =========================================================================
    // S3Reader has_more tests
    // =========================================================================

    #[test]
    fn test_reader_has_more_initial_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Initial state should have more
        assert!(reader.has_more());
    }

    #[test]
    fn test_reader_has_more_eof_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Eof,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert!(!reader.has_more());
    }

    #[test]
    fn test_reader_has_more_error_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Error("test error".to_string()),
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert!(!reader.has_more());
    }

    #[test]
    fn test_reader_has_more_ready_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 100,
                file_size: 1000,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Ready state should have more
        assert!(reader.has_more());
    }

    // =========================================================================
    // S3Reader channels method with different states
    // =========================================================================

    #[test]
    fn test_reader_channels_initial_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Initial state returns empty channels
        assert!(reader.channels().is_empty());
    }

    #[test]
    fn test_reader_channels_eof_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Eof,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert!(reader.channels().is_empty());
    }

    #[test]
    fn test_reader_channels_error_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Error("error".to_string()),
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert!(reader.channels().is_empty());
    }

    #[test]
    fn test_reader_channels_ready_state() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let mut channels = HashMap::new();
        channels.insert(1, ChannelInfo::new(1, "/test", "test/Msg"));

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels,
                stream_position: 0,
                file_size: 1000,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert_eq!(reader.channels().len(), 1);
        assert!(reader.channels().contains_key(&1));
    }

    // =========================================================================
    // S3ReaderConstructor tests
    // =========================================================================

    #[test]
    fn test_s3_reader_constructor_new_mcap() {
        let constructor = S3ReaderConstructor::new_mcap();
        assert_eq!(constructor.location.bucket(), "test-bucket");
        assert_eq!(constructor.location.key(), "test.mcap");
    }

    #[test]
    fn test_s3_reader_constructor_build() {
        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build();

        assert_eq!(reader.location().bucket(), "test-bucket");
        assert_eq!(reader.format(), crate::io::metadata::FileFormat::Mcap);
        assert!(matches!(reader.state(), S3ReaderState::Initial));
    }

    #[test]
    fn test_s3_reader_constructor_build_bag() {
        let constructor = S3ReaderConstructor::new_mcap();
        let reader = constructor.build_bag();

        assert_eq!(reader.location.key(), "test.bag");
        assert_eq!(reader.format(), crate::io::metadata::FileFormat::Bag);
    }

    // =========================================================================
    // S3ReaderState Ready Display
    // =========================================================================

    #[test]
    fn test_s3_reader_state_ready_display() {
        let state = S3ReaderState::Ready {
            channels: HashMap::new(),
            stream_position: 100,
            file_size: 1000,
        };
        assert_eq!(format!("{}", state), "Ready");
    }

    // =========================================================================
    // FormatReader trait implementation tests
    // =========================================================================

    #[test]
    fn test_s3_reader_format_reader_channels_empty() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Initial state returns empty channels through FormatReader trait
        assert!(crate::io::traits::FormatReader::channels(&reader).is_empty());
    }

    #[test]
    fn test_s3_reader_format_reader_message_count() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Streaming reader doesn't pre-count messages
        assert_eq!(crate::io::traits::FormatReader::message_count(&reader), 0);
    }

    #[test]
    fn test_s3_reader_format_reader_time_bounds() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Streaming reader doesn't track time bounds
        assert!(crate::io::traits::FormatReader::start_time(&reader).is_none());
        assert!(crate::io::traits::FormatReader::end_time(&reader).is_none());
    }

    #[test]
    fn test_s3_reader_format_reader_path() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "test/path/file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert_eq!(
            crate::io::traits::FormatReader::path(&reader),
            "test/path/file.mcap"
        );
    }

    #[test]
    fn test_s3_reader_format_reader_file_size() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 0,
                file_size: 5000,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        assert_eq!(crate::io::traits::FormatReader::file_size(&reader), 5000);
    }

    #[test]
    fn test_s3_reader_format_reader_file_size_initial() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Initial state returns 0
        assert_eq!(crate::io::traits::FormatReader::file_size(&reader), 0);
    }

    #[test]
    fn test_s3_reader_format_reader_as_any() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Should be able to downcast
        assert!(crate::io::traits::FormatReader::as_any(&reader).is::<S3Reader>());
    }

    #[test]
    fn test_s3_reader_format_reader_as_any_mut() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let mut reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Should be able to downcast mutably
        assert!(crate::io::traits::FormatReader::as_any_mut(&mut reader).is::<S3Reader>());
    }

    #[test]
    fn test_s3_reader_format_reader_iter_raw_boxed_empty() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 0,
                file_size: 0,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let mut iter = crate::io::traits::FormatReader::iter_raw_boxed(&reader)
            .expect("iter_raw_boxed should be supported");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_s3_reader_format_reader_decoded_with_timestamp_boxed_empty() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 0,
                file_size: 0,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let mut iter = crate::io::traits::FormatReader::decoded_with_timestamp_boxed(&reader)
            .expect("decoded_with_timestamp_boxed should be supported");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_s3_message_stream_parse_error_propagates() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.bag");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 0,
                file_size: 16,
            },
            format: crate::io::metadata::FileFormat::Bag,
        };

        let mut stream = S3MessageStream::new(&reader);
        let result = stream.parse_chunk(b"not-a-bag-stream");
        assert!(result.is_err());
    }

    // =========================================================================
    // iter_messages tests
    // =========================================================================

    #[test]
    fn test_iter_messages_creates_stream() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Ready {
                channels: HashMap::new(),
                stream_position: 0,
                file_size: 1000,
            },
            format: crate::io::metadata::FileFormat::Mcap,
        };

        let stream = reader.iter_messages();
        // Just verify it creates a stream with the right position
        assert_eq!(stream.stream_position, 0);
    }

    // =========================================================================
    // parse_channel_record with schema
    // =========================================================================

    #[test]
    fn test_parse_channel_record_with_schema() {
        use crate::io::s3::reader::SummarySchemaInfo;

        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Create a schema
        let mut schemas = HashMap::new();
        schemas.insert(
            1,
            SummarySchemaInfo {
                id: 1,
                name: "test_msgs/Msg".to_string(),
                encoding: "ros2msg".to_string(),
                data: b"int32 data".to_vec(),
            },
        );

        let mut channels = HashMap::new();

        // Channel record with schema_id = 1
        let mut body = vec![2u8, 0u8]; // id = 2
        body.extend_from_slice(&(6u16).to_le_bytes()); // topic_len
        body.extend_from_slice(b"/topic"); // topic
        body.extend_from_slice(&(3u16).to_le_bytes()); // encoding_len
        body.extend_from_slice(b"cdr"); // encoding
        body.extend_from_slice(&(1u16).to_le_bytes()); // schema_id = 1

        let result = reader.parse_channel_record(&body, &schemas, &mut channels);
        assert!(result.is_ok());
        let channel = channels.get(&2).unwrap();
        assert_eq!(channel.message_type, "test_msgs/Msg");
        assert_eq!(channel.topic, "/topic");
    }

    // =========================================================================
    // parse_schema_record edge cases
    // =========================================================================

    #[test]
    fn test_parse_schema_record_incomplete_encoding() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid name but incomplete encoding
        let mut body = vec![0u8; 2]; // id = 0
        body.extend_from_slice(&(4u16).to_le_bytes()); // name_len = 4
        body.extend_from_slice(b"Test"); // name
        body.extend_from_slice(&(10u16).to_le_bytes()); // encoding_len = 10
        body.extend_from_slice(b"short"); // only 5 bytes

        let result = reader.parse_schema_record(&body);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_schema_record_invalid_encoding_utf8() {
        let client = S3Client::default_client().unwrap();
        let location = S3Location::new("bucket", "file.mcap");
        let config = S3ReaderConfig::default();

        let reader = S3Reader {
            location,
            config,
            client,
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        };

        // Valid name but invalid UTF-8 in encoding
        let mut body = vec![0u8; 2]; // id = 0
        body.extend_from_slice(&(4u16).to_le_bytes()); // name_len = 4
        body.extend_from_slice(b"Test"); // name
        body.extend_from_slice(&(4u16).to_le_bytes()); // encoding_len = 4
        body.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // Invalid UTF-8

        let result = reader.parse_schema_record(&body);
        assert!(result.is_err());
    }
}
