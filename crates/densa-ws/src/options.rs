use std::fmt;
use std::time::Duration;

use arrow_schema::SchemaRef;

use crate::stream::Stream;

/// What the stream's messages hold.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MessageFormat {
    /// Text messages of JSON objects, one or more per message separated by whitespace (not
    /// a top-level array), read with `schema`: each object is a row, its fields the columns
    /// of the same name (others are ignored, missing ones are null). A message is read
    /// whole or skipped whole.
    Json {
        /// The rows' columns.
        schema: SchemaRef,
    },
    /// Text messages of JSON objects, as [`Json`](Self::Json), read with the columns of the
    /// first message that has any: each field of its objects is a column, in the order they
    /// come, typed by its values there: numbers `Float64` (one message cannot tell a field
    /// of whole numbers from one whose fractions have not come yet, and Arrow would cut
    /// fractions off; whole numbers up to 2^53 stay exact), text, booleans, nested values as
    /// structs and lists, and a field null in all of them text. The columns stay for the
    /// stream's life, across reconnections: fields that appear later are ignored, numbers
    /// and booleans in a text column are kept as text, and a message whose values do not
    /// fit is skipped whole. Give the columns ([`Json`](Self::Json)) for other types.
    JsonInferred,
    /// Binary messages carrying an Arrow IPC stream: a schema message, then record batches,
    /// split across messages in any way. Each connection starts a new IPC stream; text
    /// messages between them are skipped.
    ArrowIpc,
}

/// How to read a WebSocket stream: [`StreamOptions::json`] or [`StreamOptions::arrow_ipc`],
/// then the settings that differ, then [`connect`](Self::connect).
///
/// Its `Debug` output leaves header values out: they are often tokens.
#[derive(Clone, PartialEq)]
pub struct StreamOptions {
    pub(crate) url: String,
    pub(crate) format: MessageFormat,
    pub(crate) batch_rows: usize,
    pub(crate) max_delay: Duration,
    pub(crate) retry: (Duration, Duration),
    pub(crate) on_connect: Vec<String>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) max_message: usize,
}

/// The most rows a batch holds.
pub const MAX_BATCH_ROWS: usize = 1 << 20;

impl StreamOptions {
    /// A stream of JSON rows of `schema` from `url` (`ws://` or `wss://`).
    pub fn json(url: impl Into<String>, schema: SchemaRef) -> Self {
        Self::new(url, MessageFormat::Json { schema })
    }

    /// A stream of JSON rows from `url` (`ws://` or `wss://`) whose columns are read from
    /// its first message: see [`MessageFormat::JsonInferred`].
    pub fn json_inferred(url: impl Into<String>) -> Self {
        Self::new(url, MessageFormat::JsonInferred)
    }

    /// A stream of Arrow IPC from `url` (`ws://` or `wss://`).
    pub fn arrow_ipc(url: impl Into<String>) -> Self {
        Self::new(url, MessageFormat::ArrowIpc)
    }

    fn new(url: impl Into<String>, format: MessageFormat) -> Self {
        Self {
            url: url.into(),
            format,
            batch_rows: 4_096,
            max_delay: Duration::from_millis(50),
            retry: (Duration::from_millis(500), Duration::from_secs(30)),
            on_connect: Vec::new(),
            headers: Vec::new(),
            max_message: 16 << 20,
        }
    }

    /// JSON rows gathered into batches of at most `rows` (from one to [`MAX_BATCH_ROWS`];
    /// 4,096 by default).
    #[must_use]
    pub fn batch_rows(mut self, rows: usize) -> Self {
        self.batch_rows = rows.clamp(1, MAX_BATCH_ROWS);
        self
    }

    /// Rows wait about `delay` to fill a batch before they are handed over anyway (50 ms by
    /// default; a little longer when the stream is quiet): a slow stream still shows its
    /// rows promptly. Clamped to 1 ms .. 10 s.
    #[must_use]
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay.clamp(Duration::from_millis(1), Duration::from_secs(10));
        self
    }

    /// After a lost connection, tries again after `first`, doubling the wait up to `most`
    /// (by default half a second, then up to 30 s). The wait starts over once a connection
    /// has delivered rows or stayed open for a few seconds.
    #[must_use]
    pub fn retry(mut self, first: Duration, most: Duration) -> Self {
        let first = first.max(Duration::from_millis(1));
        self.retry = (first, most.max(first));
        self
    }

    /// Sends `message` as text each time the connection opens (a subscription, say), in the
    /// order given.
    #[must_use]
    pub fn on_connect(mut self, message: impl Into<String>) -> Self {
        self.on_connect.push(message.into());
        self
    }

    /// Adds a header to the request that opens the connection (an `Authorization` token, a
    /// `Sec-WebSocket-Protocol`).
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Messages and frames of at most `bytes` (16 MiB by default, clamped to 1 KiB ..
    /// 1 GiB); a larger one ends the connection, which is opened again. An Arrow IPC body
    /// may span messages up to four times this.
    #[must_use]
    pub fn max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message = bytes.clamp(1 << 10, 1 << 30);
        self
    }

    /// Starts reading on a thread of its own, calling `wake` after each event it hands
    /// over (give it something cheap that asks the interface for a frame).
    ///
    /// # Errors
    /// [`StreamError::InvalidUrl`] if the URL is not a `ws://` or `wss://` URL with a host,
    /// [`StreamError::InvalidHeader`] for a header that cannot be sent,
    /// [`StreamError::TlsDisabled`] for `wss://` without the `tls` feature,
    /// [`StreamError::Schema`] if the JSON schema cannot be read into, [`StreamError::Io`]
    /// if the thread cannot start. A server that cannot be reached is not an error: the
    /// stream reports it and keeps trying.
    pub fn connect(self, wake: impl Fn() + Send + 'static) -> Result<Stream, StreamError> {
        Stream::spawn(self, wake)
    }
}

impl fmt::Debug for StreamOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<_> = self
            .headers
            .iter()
            .map(|(name, _)| format!("{name}: <hidden>"))
            .collect();
        f.debug_struct("StreamOptions")
            .field("url", &self.url)
            .field("format", &self.format)
            .field("batch_rows", &self.batch_rows)
            .field("max_delay", &self.max_delay)
            .field("retry", &self.retry)
            .field("on_connect", &self.on_connect)
            .field("headers", &headers)
            .field("max_message", &self.max_message)
            .finish()
    }
}

/// Why a stream could not be started.
#[derive(Debug)]
#[non_exhaustive]
pub enum StreamError {
    /// The URL is not a WebSocket URL with a host.
    InvalidUrl(String),
    /// A header name or value cannot be sent.
    InvalidHeader(String),
    /// A `wss://` URL, without the `tls` feature that reads it.
    TlsDisabled,
    /// The JSON schema cannot be decoded into (the reader's message).
    Schema(String),
    /// The reading thread could not be started.
    Io(std::io::Error),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(reason) => write!(f, "invalid WebSocket URL: {reason}"),
            Self::InvalidHeader(reason) => write!(f, "invalid header: {reason}"),
            Self::TlsDisabled => f.write_str("wss:// needs the tls feature"),
            Self::Schema(reason) => write!(f, "cannot read JSON into the schema: {reason}"),
            Self::Io(error) => write!(f, "could not start the stream: {error}"),
        }
    }
}

impl std::error::Error for StreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
