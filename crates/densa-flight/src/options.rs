use std::fmt;
use std::time::Duration;

use crate::stream::FlightStream;

/// What to ask the Flight service for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FlightRequest {
    /// `DoGet` of a ticket the service handed out (or one both sides agree on).
    Ticket(Vec<u8>),
    /// `GetFlightInfo` of a dataset named by a path, then `DoGet` of each of its endpoints.
    Path(Vec<String>),
    /// `GetFlightInfo` of an opaque command the service understands, then `DoGet` of each
    /// of its endpoints.
    Command(Vec<u8>),
    /// A Flight SQL query: `GetFlightInfo` of a `CommandStatementQuery`, then `DoGet` of
    /// each endpoint.
    #[cfg(feature = "sql")]
    Sql(String),
}

/// How to read a Flight service's data: [`FlightOptions::ticket`], [`path`](Self::path),
/// [`command`](Self::command) or [`sql`](Self::sql), then the settings that differ, then
/// [`connect`](Self::connect).
///
/// URLs are `grpc://` (or `grpc+tcp://`, `http://`) for plain connections and `grpc+tls://`
/// (or `https://`) for TLS, as Flight locations are written.
///
/// Its `Debug` output leaves header values out: they are often tokens.
#[derive(Clone, PartialEq, Eq)]
pub struct FlightOptions {
    pub(crate) url: String,
    pub(crate) request: FlightRequest,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) row_numbers: Option<String>,
    pub(crate) retry: Option<(Duration, Duration)>,
    pub(crate) max_message: usize,
}

impl FlightOptions {
    /// The data behind `ticket` at `url`.
    pub fn ticket(url: impl Into<String>, ticket: impl Into<Vec<u8>>) -> Self {
        Self::new(url, FlightRequest::Ticket(ticket.into()))
    }

    /// The dataset at `path` (`["sales", "2026"]`, say) at `url`.
    pub fn path<S: Into<String>>(
        url: impl Into<String>,
        path: impl IntoIterator<Item = S>,
    ) -> Self {
        let path = path.into_iter().map(Into::into).collect();
        Self::new(url, FlightRequest::Path(path))
    }

    /// The data `command` stands for at `url`.
    pub fn command(url: impl Into<String>, command: impl Into<Vec<u8>>) -> Self {
        Self::new(url, FlightRequest::Command(command.into()))
    }

    /// The rows of a Flight SQL `query` at `url`.
    #[cfg(feature = "sql")]
    pub fn sql(url: impl Into<String>, query: impl Into<String>) -> Self {
        Self::new(url, FlightRequest::Sql(query.into()))
    }

    fn new(url: impl Into<String>, request: FlightRequest) -> Self {
        Self {
            url: url.into(),
            request,
            headers: Vec::new(),
            row_numbers: None,
            retry: None,
            max_message: 64 << 20,
        }
    }

    /// Adds a header to every call to the service at the URL (an `authorization` token,
    /// say). Endpoints the service places on another host or port get no headers: a token
    /// is not handed to wherever a response points. Names are sent in lowercase, as gRPC
    /// has them.
    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Adds a column of row numbers named `name` (an unsigned integer from 0, never null, in
    /// the order rows arrive): a key for a table when the result has none.
    #[must_use]
    pub fn row_numbers(mut self, name: impl Into<String>) -> Self {
        self.row_numbers = Some(name.into());
        self
    }

    /// When a read fails in a way that may pass (the service unavailable or overloaded, the
    /// connection lost), asks again after `first`, doubling the wait up to `most` (for a
    /// stream that never ends, say); without it any failure ends the stream, and failures
    /// that would repeat (a query refused, no access, a message over the limit) always do.
    /// The wait starts over once a try has delivered rows. Each try starts the result over
    /// (and the row numbers from 0).
    #[must_use]
    pub fn retry(mut self, first: Duration, most: Duration) -> Self {
        let first = first.max(Duration::from_millis(1));
        self.retry = Some((first, most.max(first)));
        self
    }

    /// Messages of at most `bytes` (64 MiB by default, clamped to 1 KiB .. 2 GiB); a larger
    /// one fails the read.
    #[must_use]
    pub fn max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message = bytes.clamp(1 << 10, (1 << 31) - 1);
        self
    }

    /// Starts reading on a thread of its own, calling `wake` after each event it hands
    /// over (give it something cheap that asks the interface for a frame).
    ///
    /// # Errors
    /// [`RequestError::InvalidUrl`] if the URL is not a Flight URL with a host,
    /// [`RequestError::InvalidHeader`] for a header that cannot be sent,
    /// [`RequestError::TlsDisabled`] for a TLS URL without the `tls` feature,
    /// [`RequestError::Io`] if the thread cannot start. A service that cannot be reached is
    /// not an error here: the stream reports it.
    pub fn connect(self, wake: impl Fn() + Send + 'static) -> Result<FlightStream, RequestError> {
        FlightStream::spawn(self, wake)
    }
}

impl fmt::Debug for FlightOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<_> = self
            .headers
            .iter()
            .map(|(name, _)| format!("{name}: <hidden>"))
            .collect();
        f.debug_struct("FlightOptions")
            .field("url", &self.url)
            .field("request", &self.request)
            .field("headers", &headers)
            .field("row_numbers", &self.row_numbers)
            .field("retry", &self.retry)
            .field("max_message", &self.max_message)
            .finish()
    }
}

/// Why a Flight stream could not be started.
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestError {
    /// The URL is not a Flight URL with a host (or holds credentials, which go in a header).
    InvalidUrl(String),
    /// A header name or value cannot be sent.
    InvalidHeader(String),
    /// A TLS URL, without the `tls` feature that reads it.
    TlsDisabled,
    /// The reading thread could not be started.
    Io(std::io::Error),
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl(reason) => write!(f, "invalid Flight URL: {reason}"),
            Self::InvalidHeader(reason) => write!(f, "invalid header: {reason}"),
            Self::TlsDisabled => f.write_str("grpc+tls:// needs the tls feature"),
            Self::Io(error) => write!(f, "could not start the stream: {error}"),
        }
    }
}

impl std::error::Error for RequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
