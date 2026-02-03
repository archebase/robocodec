// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader implementation.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::stream::Stream;

use crate::io::formats::mcap::constants::{
    MCAP_MAGIC, OP_ATTACHMENT, OP_ATTACHMENT_INDEX, OP_CHANNEL, OP_CHUNK, OP_CHUNK_INDEX,
    OP_DATA_END, OP_FOOTER, OP_HEADER, OP_MESSAGE, OP_MESSAGE_INDEX, OP_METADATA,
    OP_METADATA_INDEX, OP_SCHEMA, OP_STATISTICS, OP_SUMMARY_OFFSET,
};
use crate::io::metadata::ChannelInfo;
use crate::io::s3::{
    bag_stream::{BagMessageRecord, StreamingBagParser},
    client::S3Client,
    config::S3ReaderConfig,
    error::FatalError,
    location::S3Location,
    mcap_stream::{MessageRecord, StreamingMcapParser},
};
use crate::io::traits::FormatReader;

/// State machine for S3 streaming reader.
///
/// The reader progresses through states as it:
/// 1. Fetches and parses the header (discovers channels)
/// 2. Streams message data
/// 3. Reaches end of file
#[derive(Debug, Clone)]
pub enum S3ReaderState {
    /// Initial state - about to fetch first chunk
    Initial,

    /// Scanning header for metadata
    ScanningHeader {
        /// Buffer containing header data
        buffer: BytesMut,
        /// Number of bytes read so far
        bytes_read: u64,
    },

    /// Ready to stream messages (index built)
    Ready {
        /// Channel info discovered during scan
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
struct SummarySchemaInfo {
    id: u16,
    name: String,
    encoding: String,
    data: Vec<u8>,
}

impl fmt::Display for S3ReaderState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            S3ReaderState::Initial => write!(f, "Initial"),
            S3ReaderState::ScanningHeader { .. } => write!(f, "Scanning header"),
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
        } else {
            return Err(FatalError::InvalidFormat {
                expected: "MCAP or BAG file",
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
            _ => {
                return Err(FatalError::InvalidFormat {
                    expected: "MCAP or BAG",
                    found: vec![],
                })
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
        if let Ok(channels) = self.try_mcap_footer_first(file_size).await {
            if !channels.is_empty() {
                return Ok((channels, 0));
            }
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
    fn parse_mcap_footer(&self, data: &[u8]) -> Result<u64, FatalError> {
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
    fn parse_mcap_summary_data(
        &self,
        data: &[u8],
    ) -> Result<HashMap<u16, ChannelInfo>, FatalError> {
        // Local schema representation for summary parsing
        #[derive(Clone)]
        struct SchemaInfo {
            id: u16,
            name: String,
            encoding: String,
            data: Vec<u8>,
        }

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
                    if let Ok(_) = self.parse_channel_record(body, &schemas, &mut channels) {
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
    fn parse_schema_record(&self, body: &[u8]) -> Result<SummarySchemaInfo, FatalError> {
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
    fn parse_channel_record(
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
        use crate::io::s3::bag_stream::BAG_MAGIC_PREFIX;

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

// Empty channel map constant - use lazy_static for const HashMap
fn empty_channels() -> &'static HashMap<u16, ChannelInfo> {
    use std::sync::OnceLock;
    static EMPTY: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// Async stream over messages in an S3 file.
///
/// This stream fetches data in chunks as needed, providing constant
/// memory usage regardless of file size. Uses async iteration pattern
/// to fetch from S3 without blocking.
pub struct S3MessageStream<'a> {
    /// Reference to the parent reader (cloning necessary fields)
    client: S3Client,
    location: S3Location,
    config: S3ReaderConfig,
    format: crate::io::metadata::FileFormat,
    file_size: u64,

    /// Format-specific streaming parser state
    mcap_parser: Option<StreamingMcapParser>,
    bag_parser: Option<StreamingBagParser>,
    channels: HashMap<u16, ChannelInfo>,

    /// Current chunk of message data being processed
    pending_messages: Vec<ParsedMessage>,

    /// Current stream position
    stream_position: u64,

    /// Whether we've reached EOF
    eof: bool,

    /// Phantom lifetime
    _phantom: std::marker::PhantomData<&'a ()>,
}

/// Parsed message from either MCAP or BAG format.
enum ParsedMessage {
    Mcap(MessageRecord),
    Bag(BagMessageRecord),
}

impl ParsedMessage {
    /// Get the channel ID for this message.
    fn channel_id(&self) -> u32 {
        match self {
            ParsedMessage::Mcap(m) => m.channel_id as u32,
            ParsedMessage::Bag(b) => b.conn_id,
        }
    }

    /// Get the message data.
    fn data(self) -> Vec<u8> {
        match self {
            ParsedMessage::Mcap(m) => m.data,
            ParsedMessage::Bag(b) => b.data,
        }
    }

    /// Get the log time.
    #[allow(dead_code)]
    fn log_time(&self) -> u64 {
        match self {
            ParsedMessage::Mcap(m) => m.log_time,
            ParsedMessage::Bag(b) => b.log_time,
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

        let (mcap_parser, bag_parser) = match reader.format {
            crate::io::metadata::FileFormat::Mcap => {
                // Parser already initialized during header scan, create a new one for streaming
                (Some(StreamingMcapParser::new()), None)
            }
            crate::io::metadata::FileFormat::Bag => (None, Some(StreamingBagParser::new())),
            _ => (None, None),
        };

        Self {
            client: reader.client.clone(),
            location: reader.location.clone(),
            config: reader.config.clone(),
            format: reader.format,
            file_size,
            mcap_parser,
            bag_parser,
            channels,
            pending_messages: Vec::new(),
            stream_position,
            eof: false,
            _phantom: std::marker::PhantomData,
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
            let remaining_usize = remaining.min(self.config.max_chunk_size() as u64) as usize;
            let chunk_size = self.config.max_chunk_size().min(remaining_usize) as u64;

            if chunk_size == 0 {
                self.eof = true;
                return None;
            }

            match self
                .client
                .fetch_range(&self.location, self.stream_position, chunk_size)
                .await
            {
                Ok(chunk_data) => {
                    if chunk_data.is_empty() {
                        self.eof = true;
                        return None;
                    }

                    // Parse the chunk based on format
                    match self.format {
                        crate::io::metadata::FileFormat::Mcap => {
                            if let Some(ref mut parser) = self.mcap_parser {
                                if let Ok(msgs) = parser.parse_chunk(&chunk_data) {
                                    for msg in msgs {
                                        self.pending_messages.push(ParsedMessage::Mcap(msg));
                                    }
                                }
                            }
                        }
                        crate::io::metadata::FileFormat::Bag => {
                            if let Some(ref mut parser) = self.bag_parser {
                                if let Ok(msgs) = parser.parse_chunk(&chunk_data) {
                                    for msg in msgs {
                                        self.pending_messages.push(ParsedMessage::Bag(msg));
                                    }
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
}
