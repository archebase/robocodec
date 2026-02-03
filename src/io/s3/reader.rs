// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

//! S3 streaming reader implementation.

use std::collections::HashMap;
use std::fmt;

use bytes::BytesMut;

use crate::io::formats::mcap::constants::MCAP_MAGIC;
use crate::io::metadata::ChannelInfo;
use crate::io::s3::{
    client::S3Client, config::S3ReaderConfig, error::FatalError, location::S3Location,
};

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
///
/// # Example
///
/// ```no_run
/// use robocodec::io::s3::{S3Location, S3Reader};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let location = S3Location::new("my-bucket", "path/to/file.mcap");
/// let reader = S3Reader::open(location).await?;
///
/// println!("Channels: {}", reader.channels().len());
///
/// for result in reader.iter_messages() {
///     let (channel, data) = result?;
///     println!("{}: {} bytes", channel.topic, data.len());
/// }
/// # Ok(())
/// # }
/// ```
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
        // Fetch header data
        let header_data = self
            .client
            .fetch_header(&self.location, self.config.header_scan_limit)
            .await?;

        // Parse format-specific header
        let (channels, file_size) = match self.format {
            crate::io::metadata::FileFormat::Mcap => self.parse_mcap_header(&header_data)?,
            crate::io::metadata::FileFormat::Bag => self.parse_bag_header(&header_data)?,
            _ => {
                return Err(FatalError::InvalidFormat {
                    expected: "MCAP or BAG",
                    found: vec![],
                })
            }
        };

        // Get file size
        let file_size = if file_size == 0 {
            self.client.object_size(&self.location).await?
        } else {
            file_size
        };

        // Move to ready state
        self.state = S3ReaderState::Ready {
            channels,
            stream_position: header_data.len() as u64,
            file_size,
        };

        Ok(())
    }

    /// Parse MCAP header to discover channels.
    fn parse_mcap_header(
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

        // For now, return empty channel map - will be populated by full streaming parser
        // In Phase 2, this will scan for Channel and Schema records
        Ok((HashMap::new(), 0))
    }

    /// Parse BAG header to discover channels.
    fn parse_bag_header(
        &self,
        data: &[u8],
    ) -> Result<(HashMap<u16, ChannelInfo>, u64), FatalError> {
        // BAG magic: #LOGA or #ROSB
        if data.len() < 8 {
            return Err(FatalError::invalid_format(
                "BAG header (8 bytes minimum)",
                data.to_vec(),
            ));
        }

        let header = std::str::from_utf8(&data[..8]).unwrap_or("");
        if !header.starts_with("#LOG") && !header.starts_with("#ROS") {
            return Err(FatalError::invalid_format(
                "BAG header (#LOGA or version header)",
                data[..8].to_vec(),
            ));
        }

        // For now, return empty channel map - will be populated by full streaming parser
        // In Phase 3, this will scan for Connection records
        Ok((HashMap::new(), 0))
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

// Empty channel map constant - use lazy_static for const HashMap
fn empty_channels() -> &'static HashMap<u16, ChannelInfo> {
    use std::sync::OnceLock;
    static EMPTY: OnceLock<HashMap<u16, ChannelInfo>> = OnceLock::new();
    EMPTY.get_or_init(HashMap::new)
}

/// Streaming iterator over messages in an S3 file.
///
/// This iterator fetches data in chunks as needed, providing constant
/// memory usage regardless of file size.
pub struct S3MessageStream<'a> {
    /// Reference to the parent reader
    reader: &'a S3Reader,
    /// Current chunk of message data
    current_chunk: Option<Vec<u8>>,
    /// Current position within chunk
    chunk_offset: usize,
    /// Current stream position
    stream_position: u64,
    /// Mark fields as used to suppress warnings (will be used in Phase 2/3)
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> S3MessageStream<'a> {
    /// Create a new message stream.
    fn new(reader: &'a S3Reader) -> Self {
        let stream_position = match &reader.state {
            S3ReaderState::Ready {
                stream_position, ..
            } => *stream_position,
            _ => 0,
        };

        Self {
            reader,
            current_chunk: None,
            chunk_offset: 0,
            stream_position,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a> Iterator for S3MessageStream<'a> {
    type Item = Result<(ChannelInfo, Vec<u8>), FatalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // For Phase 1, return EOF immediately
        // In Phase 2/3, this will:
        // 1. Fetch next chunk if current is exhausted
        // 2. Parse messages from chunk
        // 3. Return (ChannelInfo, message_data) tuples
        None
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

        // Valid BAG header
        let data = b"#LOGA 2.0some extra data";

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
