// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader implementation.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;

use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_ATTACHMENT, OP_ATTACHMENT_INDEX, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX,
    OP_DATA_END, OP_FOOTER, OP_HEADER, OP_MESSAGE, OP_MESSAGE_INDEX, OP_METADATA,
    OP_METADATA_INDEX, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};
use crate::io::metadata::ChannelInfo;
use crate::io::s3::{
    client::S3Client, config::S3ReaderConfig, error::FatalError, location::S3Location,
};
// Re-export streaming parsers from format modules
use crate::io::formats::bag::stream::{BagMessageRecord, StreamingBagParser};
use crate::io::formats::mcap::stream::{MessageRecord, StreamingMcapParser};
use crate::io::formats::rrd::stream::{RrdMessageRecord, StreamingRrdParser};
use crate::io::s3::StreamingParser;
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
            S3ReaderState::Error(msg) => write!(f, "Error: {}", msg),
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
            return Ok(HashMap::new());
        }

        // Parse footer to extract summary_offset
        let summary_offset = match self.parse_mcap_footer(&tail_data[footer_start..]) {
            Ok(offset) => offset,
            Err(_) => {
                // Footer parsing failed, fall back to scanning
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
        // Footer structure (from MCAP spec):
        // summary_offset: u64 (8 bytes)
        // summary_section_start: u64 (8 bytes)
        // summary_crc: u32 (4 bytes)
        // ... (other fields we don't need)
        // Total minimum: 20 bytes

        if data.len() < 8 {
            return Err(FatalError::invalid_format("MCAP footer", data.to_vec()));
        }

        Ok(u64::from_le_bytes(data[0..8].try_into().unwrap()))
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
        let mut schemas: HashMap<u16, SummarySchemaInfo> = HashMap::new();
        let mut channels: HashMap<u16, ChannelInfo> = HashMap::new();
        let mut pos = 0;

        while pos + 9 <= data.len() {
            let opcode = data[pos];
            let length = u64::from_le_bytes(data[pos + 1..pos + 9].try_into().unwrap());
            pos += 9;

            if pos + length as usize > data.len() {
                break;
            }

            let body = &data[pos..pos + length as usize];

            match opcode {
                OP_SCHEMA => {
                    if let Ok(schema) = self.parse_schema_record(body) {
                        schemas.insert(schema.id, schema);
                    }
                }
                OP_CHANNEL => {
                    if self
                        .parse_channel_record(body, &schemas, &mut channels)
                        .is_ok()
                    {
                        // Channel added
                    }
                }
                OP_MESSAGE_INDEX | OP_CHUNK_INDEX | OP_ATTACHMENT | OP_ATTACHMENT_INDEX
                | OP_METADATA | OP_METADATA_INDEX | OP_STATISTICS | OP_SUMMARY_OFFSET
                | OP_HEADER | OP_FOOTER | OP_DATA_END | OP_CHUNK | OP_MESSAGE => {
                    // Ignore these for channel discovery
                }
                _ => {
                    // Unknown opcode, stop parsing
                    break;
                }
            }

            pos += length as usize;
        }

        Ok(channels)
    }

    /// Parse a Schema record from summary data.
    ///
    /// This is public for testing purposes only.
    pub fn parse_schema_record(&self, body: &[u8]) -> Result<SummarySchemaInfo, FatalError> {
        if body.len() < 4 {
            return Err(FatalError::invalid_format(
                "MCAP Schema record",
                body.to_vec(),
            ));
        }

        let id = u16::from_le_bytes(body[0..2].try_into().unwrap());
        let name_len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;

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

        let encoding_len =
            u16::from_le_bytes(body[offset..offset + 2].try_into().unwrap()) as usize;
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
        if body.len() < 4 {
            return Err(FatalError::invalid_format(
                "MCAP Channel record",
                body.to_vec(),
            ));
        }

        let id = u16::from_le_bytes(body[0..2].try_into().unwrap());
        let topic_len = u16::from_le_bytes(body[2..4].try_into().unwrap()) as usize;

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

        let encoding_len =
            u16::from_le_bytes(body[offset..offset + 2].try_into().unwrap()) as usize;
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

        let schema_id =
            u16::from_le_bytes(body[schema_offset..schema_offset + 2].try_into().unwrap());

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
        // Fetch larger initial portion for scanning
        // For files without summary, we need to scan through records
        // Fetch up to 10MB which should cover most schemas/channels
        let scan_limit = 10 * 1024 * 1024;
        let scan_limit = scan_limit.min(file_size) as usize;

        let data = self
            .client
            .fetch_range(&self.location, 0, scan_limit as u64)
            .await?;

        // Use streaming parser to collect channels
        let mut parser = StreamingMcapParser::new();
        let _ = parser.parse_chunk(&data);

        let channels = parser.channels();

        if channels.is_empty() {
            // Try fetching even more data
            let additional_limit = 50 * 1024 * 1024; // 50MB more
            let additional_limit =
                additional_limit.min(file_size.saturating_sub(scan_limit as u64)) as usize;

            if additional_limit > 0 {
                let additional_data = self
                    .client
                    .fetch_range(&self.location, scan_limit as u64, additional_limit as u64)
                    .await?;

                let _ = parser.parse_chunk(&additional_data);
                let channels = parser.channels();

                return Ok((channels, 0));
            }
        }

        Ok((channels, 0))
    }

    /// Initialize BAG reader.
    async fn initialize_bag(
        &mut self,
        _file_size: u64,
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // For BAG files, use the existing header parsing approach
        // BAG files typically have connection records in the header/index section
        let header_data = self
            .client
            .fetch_header(&self.location, self.config.header_scan_limit())
            .await?;

        self.parse_bag_header(&header_data)
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
        let _ = parser.parse_chunk(data);

        Ok((parser.channels().clone(), 0))
    }

    /// Parse MCAP header to discover channels.
    ///
    /// This is a simple method used for testing. For production use,
    /// prefer the two-tier approach (try_mcap_footer_first + scan_mcap_for_metadata).
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

        // Use streaming parser to discover channels
        let mut parser = StreamingMcapParser::new();
        // Parse the header data to discover channels
        let _ = parser.parse_chunk(data);
        Ok((parser.channels(), 0))
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
        let _ = parser.parse_chunk(data);
        Ok((parser.channels(), 0))
    }

    /// Get the current reader state.
    pub fn state(&self) -> &S3ReaderState {
        &self.state
    }

    /// Get the S3 location.
    pub fn location(&self) -> &S3Location {
        &self.location
    }

    /// Get the file format.
    pub fn format(&self) -> crate::io::metadata::FileFormat {
        self.format
    }

    /// Get the channels discovered during header scan.
    pub fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match &self.state {
            S3ReaderState::Ready { channels, .. } => channels,
            _ => empty_channels(),
        }
    }

    /// Create an iterator over messages in the file.
    pub fn iter_messages(&self) -> S3MessageStream<'_> {
        S3MessageStream::new(self)
    }

    /// Check if the reader has more messages.
    pub fn has_more(&self) -> bool {
        !matches!(self.state, S3ReaderState::Eof | S3ReaderState::Error(_))
    }
}

impl FormatReader for S3Reader {
    fn channels(&self) -> &HashMap<u16, ChannelInfo> {
        match &self.state {
            S3ReaderState::Ready { channels, .. } => channels,
            _ => empty_channels(),
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// Empty channel map constant - use OnceLock for lazy initialization
fn empty_channels() -> &'static HashMap<u16, ChannelInfo> {
    use std::sync::OnceLock;
    static EMPTY: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// Test-only constructor for creating S3Reader instances directly.
///
/// This is public for testing purposes only. Normal usage should use
/// `S3Reader::open()` or `S3Reader::open_with_config()`.
pub struct S3ReaderConstructor {
    pub location: S3Location,
    pub config: S3ReaderConfig,
    pub client: S3Client,
}

impl S3ReaderConstructor {
    pub fn new_mcap() -> Self {
        Self {
            location: S3Location::new("test-bucket", "test.mcap"),
            config: S3ReaderConfig::default(),
            client: S3Client::default_client().unwrap(),
        }
    }

    pub fn build(&self) -> S3Reader {
        S3Reader {
            location: self.location.clone(),
            config: self.config.clone(),
            client: self.client.clone(),
            state: S3ReaderState::Initial,
            format: crate::io::metadata::FileFormat::Mcap,
        }
    }

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
/// This stream borrows from the parent S3Reader, avoiding unnecessary
/// cloning of client, location, and config.
pub struct S3MessageStream<'a> {
    /// Reference to the parent reader
    reader: &'a S3Reader,

    /// Format-specific streaming parser state
    mcap_parser: Option<StreamingMcapParser>,
    bag_parser: Option<StreamingBagParser>,
    rrd_parser: Option<StreamingRrdParser>,
    channels: HashMap<u16, ChannelInfo>,

    /// Current chunk of message data being processed
    pending_messages: Vec<ParsedMessage>,

    /// Current stream position
    stream_position: u64,

    /// File size (cached from reader to avoid repeated access)
    file_size: u64,

    /// Whether we've reached EOF
    eof: bool,
}

/// Parsed message from MCAP, BAG, or RRD format.
enum ParsedMessage {
    Mcap(MessageRecord),
    Bag(BagMessageRecord),
    Rrd(RrdMessageRecord),
}

impl ParsedMessage {
    /// Get the channel ID for this message.
    fn channel_id(&self) -> u32 {
        match self {
            ParsedMessage::Mcap(m) => m.channel_id as u32,
            ParsedMessage::Bag(b) => b.conn_id,
            ParsedMessage::Rrd(r) => r.index as u32, // RRF2 uses message index
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

    /// Get the log time.
    #[allow(dead_code)]
    fn log_time(&self) -> u64 {
        match self {
            ParsedMessage::Mcap(m) => m.log_time,
            ParsedMessage::Bag(b) => b.log_time,
            ParsedMessage::Rrd(r) => r.index, // RRF2 uses message index as timestamp
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

        let (mcap_parser, bag_parser, rrd_parser) = match reader.format {
            crate::io::metadata::FileFormat::Mcap => {
                // Parser already initialized during header scan, create a new one for streaming
                (Some(StreamingMcapParser::new()), None, None)
            }
            crate::io::metadata::FileFormat::Bag => (None, Some(StreamingBagParser::new()), None),
            crate::io::metadata::FileFormat::Rrd => (None, None, Some(StreamingRrdParser::new())),
            _ => (None, None, None),
        };

        Self {
            reader,
            mcap_parser,
            bag_parser,
            rrd_parser,
            channels,
            pending_messages: Vec::new(),
            stream_position,
            file_size,
            eof: false,
        }
    }
}

impl<'a> Stream for S3MessageStream<'a> {
    type Item = Result<(ChannelInfo, Vec<u8>), FatalError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // This is a simplified implementation that processes data synchronously.
        // A fully async version would use a background task to fetch chunks.
        // For now, use next_message() instead which properly fetches chunks.

        // Try to return a pending message, filtering out unknown channels
        while let Some(msg) = self.pending_messages.pop() {
            let channel_id = msg.channel_id();
            let data = msg.data();

            // Find channel info - skip message if channel not found
            if let Some(channel_info) = self.channels.get(&(channel_id as u16)).cloned() {
                return Poll::Ready(Some(Ok((channel_info, data))));
            }
            // Channel not found - log warning and continue to next message
            tracing::warn!(
                context = "S3MessageStream",
                channel_id,
                "Unknown channel ID, skipping message"
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
impl<'a> S3MessageStream<'a> {
    /// Get the next message synchronously (blocking).
    ///
    /// This method is provided for convenience when async runtime is available.
    /// In an async context, use `StreamExt::next()` instead.
    pub async fn next_message(&mut self) -> Option<Result<(ChannelInfo, Vec<u8>), FatalError>> {
        loop {
            // Return pending message if available, filtering out unknown channels
            if let Some(msg) = self.pending_messages.pop() {
                let channel_id = msg.channel_id();
                let data = msg.data();

                // Find channel info - skip message if channel not found
                if let Some(channel_info) = self.channels.get(&(channel_id as u16)).cloned() {
                    return Some(Ok((channel_info, data)));
                }
                // Channel not found - log warning and continue to next message
                tracing::warn!(
                    context = "S3MessageStream",
                    channel_id,
                    "Unknown channel ID, skipping message"
                );
                // Continue loop to try next message
                continue;
            }

            // No more pending messages - check if we should fetch more or return EOF
            if self.eof || self.stream_position >= self.file_size {
                return None;
            }

            // Fetch next chunk
            let remaining = self.file_size - self.stream_position;
            // Convert remaining to usize for chunk size calculation
            // Use saturating conversion to avoid panic on overflow
            let remaining_usize =
                remaining.min(self.reader.config.max_chunk_size() as u64) as usize;
            let chunk_size = self.reader.config.max_chunk_size().min(remaining_usize) as u64;

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
                Ok(chunk_data) => {
                    if chunk_data.is_empty() {
                        self.eof = true;
                        return None;
                    }

                    // Parse the chunk based on format
                    match self.reader.format {
                        crate::io::metadata::FileFormat::Mcap => {
                            if let Some(ref mut parser) = self.mcap_parser
                                && let Ok(msgs) = parser.parse_chunk(&chunk_data)
                            {
                                for msg in msgs {
                                    self.pending_messages.push(ParsedMessage::Mcap(msg));
                                }
                            }
                        }
                        crate::io::metadata::FileFormat::Bag => {
                            if let Some(ref mut parser) = self.bag_parser
                                && let Ok(msgs) = parser.parse_chunk(&chunk_data)
                            {
                                for msg in msgs {
                                    self.pending_messages.push(ParsedMessage::Bag(msg));
                                }
                            }
                        }
                        crate::io::metadata::FileFormat::Rrd => {
                            if let Some(ref mut parser) = self.rrd_parser
                                && let Ok(msgs) = parser.parse_chunk(&chunk_data)
                            {
                                for msg in msgs {
                                    self.pending_messages.push(ParsedMessage::Rrd(msg));
                                }
                            }
                        }
                        _ => {}
                    }

                    self.stream_position += chunk_data.len() as u64;

                    // If file is exhausted, mark EOF
                    if self.stream_position >= self.file_size {
                        self.eof = true;
                    }
                }
                Err(e) => {
                    self.eof = true;
                    return Some(Err(e));
                }
            }
            // Loop back to process the messages we just added
        }
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
        let mut data = MCAP_MAGIC.to_vec();
        data.extend_from_slice(b"some extra data");

        let result = reader.parse_mcap_header(&data);
        assert!(result.is_ok());
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

    #[test]
    fn test_parsed_message_log_time() {
        use crate::io::formats::bag::stream::BagMessageRecord;
        use crate::io::formats::mcap::stream::MessageRecord;
        use crate::io::formats::rrd::stream::{MessageKind, RrdMessageRecord};

        // MCAP message has timestamp
        let mcap_msg = MessageRecord {
            channel_id: 1,
            log_time: 12345,
            publish_time: 12340,
            data: vec![1, 2, 3],
            sequence: 5,
        };
        let parsed = ParsedMessage::Mcap(mcap_msg);
        assert_eq!(parsed.log_time(), 12345);

        // BAG message has timestamp
        let bag_msg = BagMessageRecord {
            conn_id: 2,
            log_time: 67890,
            data: vec![4, 5, 6],
        };
        let parsed = ParsedMessage::Bag(bag_msg);
        assert_eq!(parsed.log_time(), 67890);

        // RRD message uses index as timestamp (RRF2 format limitation)
        let rrd_msg = RrdMessageRecord {
            kind: MessageKind::ArrowMsg,
            topic: "/entity".to_string(),
            data: vec![7, 8, 9],
            index: 42,
        };
        let parsed = ParsedMessage::Rrd(rrd_msg);
        assert_eq!(parsed.log_time(), 42); // Uses index as timestamp
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
            client: S3Client::default_client().unwrap(),
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

    // =========================================================================
    // parse_rrd_header tests
    // =========================================================================

    #[test]
    fn test_parse_rrd_header_valid() {
        use crate::io::formats::rrd::constants::RRD_MAGIC;
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

    // =========================================================================
    // ParsedMessage::channel_id tests
    // =========================================================================

    #[test]
    fn test_parsed_message_channel_id() {
        use crate::io::formats::bag::stream::BagMessageRecord;
        use crate::io::formats::mcap::stream::MessageRecord;
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
        assert_eq!(rrd_msg.channel_id(), 5);
    }

    #[test]
    fn test_parsed_message_data() {
        use crate::io::formats::bag::stream::BagMessageRecord;
        use crate::io::formats::mcap::stream::MessageRecord;
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
