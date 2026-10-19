use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use tungstenite::client::IntoClientRequest;
use tungstenite::handshake::client::Request;
use tungstenite::http::header::{HeaderName, HeaderValue};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Error as WsError, Message, WebSocket};

use crate::decode::{Decoder, Problem};
use crate::{StreamError, StreamOptions};

/// What happened on a stream, in order.
#[derive(Debug)]
#[non_exhaustive]
pub enum StreamEvent {
    /// The connection opened (again, after a [`Disconnected`](Self::Disconnected)).
    Connected,
    /// Rows read from the stream.
    Batch(RecordBatch),
    /// A message could not be read and was skipped whole; the stream goes on.
    Invalid {
        /// Why.
        reason: String,
        /// Whole rows of that message, read before the problem and dropped with it.
        rows_lost: usize,
    },
    /// The connection closed, could not open, or carried something it cannot go on from;
    /// the stream tries again after `retry_in`.
    Disconnected {
        /// Why.
        reason: String,
        /// How long until the next try.
        retry_in: Duration,
    },
    /// The stream stopped for good (its reading thread failed); nothing follows.
    Stopped {
        /// Why.
        reason: String,
    },
}

/// Events handed over ahead of the one taken: a slow taker holds the reading back.
const QUEUED: usize = 256;
/// How long opening a connection may take, across every address of the host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a write may block before the connection is given up.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest the thread goes without looking at whether it should stop.
const MAX_TICK: Duration = Duration::from_millis(100);
/// A connection open this long proved itself: the next wait after it is the first again.
const PROVEN: Duration = Duration::from_secs(5);

/// A WebSocket stream read on a thread of its own, its rows handed over as Arrow record
/// batches: made by [`StreamOptions::connect`].
///
/// The thread reconnects whenever the connection drops, sending the
/// [`on_connect`](StreamOptions::on_connect) messages again each time, and reports every
/// change as a [`StreamEvent`]. Dropping the stream stops the thread within a tenth of a
/// second while connected, or once a connection attempt in progress gives up (resolving a
/// name and opening a connection take up to 10 s).
#[derive(Debug)]
pub struct Stream {
    events: Receiver<StreamEvent>,
    stop: Arc<AtomicBool>,
    connected: bool,
    finished: bool,
}

impl Stream {
    pub(crate) fn spawn(
        options: StreamOptions,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, StreamError> {
        request(&options)?;
        let decoder = Decoder::new(&options.format, options.batch_rows, options.max_message)?;
        let (sender, events) = sync_channel(QUEUED);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let last_word = sender.clone();
        thread::Builder::new()
            .name("densa-ws stream".to_owned())
            .spawn(move || {
                let reader = Reader {
                    options,
                    decoder,
                    sender,
                    stop: stopping,
                    wake: &wake,
                };
                if catch_unwind(AssertUnwindSafe(|| reader.run())).is_err() {
                    let reason = "the reading thread panicked".to_owned();
                    let _ = last_word.try_send(StreamEvent::Stopped { reason });
                }
                drop(last_word);
                let _ = catch_unwind(AssertUnwindSafe(&wake));
            })
            .map_err(StreamError::Io)?;
        Ok(Self {
            events,
            stop,
            connected: false,
            finished: false,
        })
    }

    /// The next event, if one is ready: never waits.
    #[must_use]
    pub fn try_next(&mut self) -> Option<StreamEvent> {
        match self.events.try_recv() {
            Ok(event) => Some(self.seen(event)),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.finish();
                None
            }
        }
    }

    /// The next event, waiting at most `timeout` for it.
    #[must_use]
    pub fn wait_next(&mut self, timeout: Duration) -> Option<StreamEvent> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Some(self.seen(event)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                self.finish();
                None
            }
        }
    }

    /// Whether the connection was open as of the last event taken.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Whether the reading thread has ended (only when it fails, after a
    /// [`StreamEvent::Stopped`]) and every event has been taken.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn seen(&mut self, event: StreamEvent) -> StreamEvent {
        match &event {
            StreamEvent::Connected => self.connected = true,
            StreamEvent::Disconnected { .. } | StreamEvent::Stopped { .. } => {
                self.connected = false
            }
            _ => {}
        }
        event
    }

    fn finish(&mut self) {
        self.finished = true;
        self.connected = false;
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        // Relaxed: a plain flag the thread polls; nothing else is published through it.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// The request that opens the connection, checked: a WebSocket URL this build can open,
/// with every header given.
fn request(options: &StreamOptions) -> Result<Request, StreamError> {
    let url = &options.url;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|error| StreamError::InvalidUrl(error.to_string()))?;
    let uri = request.uri();
    if uri.host().is_none_or(str::is_empty) {
        return Err(StreamError::InvalidUrl(format!("{url} has no host")));
    }
    match uri.scheme_str() {
        Some("ws") => {}
        Some("wss") if cfg!(feature = "tls") => {}
        Some("wss") => return Err(StreamError::TlsDisabled),
        _ => {
            return Err(StreamError::InvalidUrl(format!(
                "{url} is not ws:// or wss://"
            )))
        }
    }
    for (name, value) in &options.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| StreamError::InvalidHeader(format!("{name:?}: {error}")))?;
        let value = HeaderValue::from_str(value)
            .map_err(|error| StreamError::InvalidHeader(format!("{name}: {error}")))?;
        request.headers_mut().append(name, value);
    }
    Ok(request)
}

/// Why reading a connection ended.
enum End {
    /// The stream was dropped.
    Stopped,
    /// The connection closed or failed; try again.
    Lost(String),
}

/// The reading thread's state.
struct Reader<'a, W: Fn()> {
    options: StreamOptions,
    decoder: Decoder,
    sender: SyncSender<StreamEvent>,
    stop: Arc<AtomicBool>,
    wake: &'a W,
}

impl<W: Fn()> Reader<'_, W> {
    fn run(mut self) {
        let mut attempt = 0_u32;
        loop {
            if self.stopped() {
                return;
            }
            let reason = match self.connect() {
                Ok(mut socket) => {
                    if !self.send(StreamEvent::Connected) {
                        return;
                    }
                    let opened = Instant::now();
                    let (end, delivered) = self.read(&mut socket);
                    if delivered || opened.elapsed() >= PROVEN {
                        attempt = 0;
                    }
                    match end {
                        End::Stopped => {
                            let _ = socket.close(None);
                            let _ = socket.flush();
                            return;
                        }
                        End::Lost(reason) => reason,
                    }
                }
                Err(reason) => reason,
            };
            let retry_in = self.backoff(attempt);
            attempt = attempt.saturating_add(1);
            if !self.send(StreamEvent::Disconnected { reason, retry_in }) || !self.sleep(retry_in) {
                return;
            }
        }
    }

    /// Opens the connection (trying each address of the host in turn), sends the
    /// subscriptions, and sets the read timeout that lets the thread hand over partial
    /// batches and notice it should stop.
    fn connect(&self) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, String> {
        let request = request(&self.options).map_err(|error| error.to_string())?;
        let uri = request.uri();
        let host = uri
            .host()
            .unwrap_or_default()
            .trim_matches(['[', ']'])
            .to_owned();
        let tls = uri.scheme_str() == Some("wss");
        let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
        let tcp = open(&host, port)?;
        tcp.set_nodelay(true).map_err(|error| error.to_string())?;
        tcp.set_write_timeout(Some(WRITE_TIMEOUT))
            .map_err(|error| error.to_string())?;
        // The handshake reads with a generous timeout of its own.
        tcp.set_read_timeout(Some(CONNECT_TIMEOUT))
            .map_err(|error| error.to_string())?;
        let limit = Some(self.options.max_message);
        let config = WebSocketConfig::default()
            .max_message_size(limit)
            .max_frame_size(limit);
        #[cfg(feature = "tls")]
        let handshake =
            tungstenite::client_tls_with_config(request, tcp, Some(config), Some(tls_connector()?));
        #[cfg(not(feature = "tls"))]
        let handshake = tungstenite::client::client_with_config(
            request,
            MaybeTlsStream::Plain(tcp),
            Some(config),
        );
        let (mut socket, _) = handshake.map_err(|error| format!("handshake failed: {error}"))?;
        let tick = self.options.max_delay.min(MAX_TICK);
        set_read_timeout(&socket, tick).map_err(|error| error.to_string())?;
        for message in &self.options.on_connect {
            socket
                .send(Message::text(message.as_str()))
                .map_err(|error| format!("cannot send a message: {error}"))?;
        }
        Ok(socket)
    }

    /// Reads messages until the connection ends or the stream is dropped; also whether it
    /// delivered rows.
    fn read(&mut self, socket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> (End, bool) {
        self.decoder.reset();
        let mut batches = Vec::new();
        let mut delivered = false;
        let mut waiting_since: Option<Instant> = None;
        loop {
            if self.stopped() {
                return (End::Stopped, delivered);
            }
            let read = match socket.read() {
                Ok(Message::Text(text)) => self.decoder.text(text.as_bytes(), &mut batches),
                Ok(Message::Binary(bytes)) => self.decoder.binary(&bytes, &mut batches),
                Ok(Message::Close(frame)) => {
                    // Sends the reply tungstenite queued.
                    let _ = socket.flush();
                    let reason = frame
                        .map(|frame| frame.reason.to_string())
                        .filter(|reason| !reason.is_empty())
                        .unwrap_or_else(|| "closed by the server".to_owned());
                    return (self.lost(reason), delivered);
                }
                Ok(_) => Ok(()),
                Err(WsError::Io(error))
                    if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
                {
                    Ok(())
                }
                Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => {
                    return (self.lost("connection closed".to_owned()), delivered);
                }
                Err(error) => return (self.lost(error.to_string()), delivered),
            };
            match read {
                Ok(()) => {}
                Err(Problem::Skipped { reason, rows_lost }) => {
                    if !self.send(StreamEvent::Invalid { reason, rows_lost }) {
                        return (End::Stopped, delivered);
                    }
                }
                Err(Problem::Broken(reason)) => return (self.lost(reason), delivered),
            }
            for batch in batches.drain(..) {
                delivered = true;
                if !self.send(StreamEvent::Batch(batch)) {
                    return (End::Stopped, delivered);
                }
            }
            if self.decoder.pending() == 0 {
                waiting_since = None;
            } else if waiting_since.get_or_insert_with(Instant::now).elapsed()
                >= self.options.max_delay
            {
                waiting_since = None;
                delivered = true;
                if !self.flush() {
                    return (End::Stopped, delivered);
                }
            }
        }
    }

    /// Hands over the rows read before a connection ended, then why it ended.
    fn lost(&mut self, reason: String) -> End {
        if self.flush() {
            End::Lost(reason)
        } else {
            End::Stopped
        }
    }

    /// Hands over the rows gathered so far. Whether the taker is still there.
    fn flush(&mut self) -> bool {
        match self.decoder.flush() {
            Ok(Some(batch)) => self.send(StreamEvent::Batch(batch)),
            Ok(None) => true,
            Err(Problem::Skipped { reason, rows_lost }) => {
                self.send(StreamEvent::Invalid { reason, rows_lost })
            }
            Err(Problem::Broken(reason)) => self.send(StreamEvent::Invalid {
                reason,
                rows_lost: 0,
            }),
        }
    }

    /// Hands `event` over and wakes the taker. Whether the taker is still there.
    fn send(&self, event: StreamEvent) -> bool {
        if self.sender.send(event).is_err() {
            return false;
        }
        (self.wake)();
        true
    }

    fn stopped(&self) -> bool {
        // Relaxed: see `Stream::drop`.
        self.stop.load(Ordering::Relaxed)
    }

    /// Waits `time`, looking at the stop flag as it goes. Whether to go on.
    fn sleep(&self, time: Duration) -> bool {
        let until = Instant::now() + time;
        while Instant::now() < until {
            if self.stopped() {
                return false;
            }
            thread::sleep(MAX_TICK.min(until.saturating_duration_since(Instant::now())));
        }
        !self.stopped()
    }

    /// The wait before try `attempt` (from 0): the first wait, doubled each time, capped.
    fn backoff(&self, attempt: u32) -> Duration {
        let (first, most) = self.options.retry;
        first
            .saturating_mul(2_u32.saturating_pow(attempt.min(20)))
            .min(most)
    }
}

/// A TCP connection to `host`, trying each of its addresses (IPv6 and IPv4 alike) until one
/// answers, within [`CONNECT_TIMEOUT`] overall.
fn open(host: &str, port: u16) -> Result<TcpStream, String> {
    let addresses: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|error| format!("cannot resolve {host}: {error}"))?
        .collect();
    let deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut last = format!("{host} has no address");
    for address in addresses {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match TcpStream::connect_timeout(&address, left) {
            Ok(tcp) => return Ok(tcp),
            Err(error) => last = format!("cannot connect to {host}:{port}: {error}"),
        }
    }
    Err(last)
}

/// A TLS client trusting the Mozilla roots, on the ring backend whatever else the process
/// enables.
#[cfg(feature = "tls")]
fn tls_connector() -> Result<tungstenite::Connector, String> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("TLS: {error}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tungstenite::Connector::Rustls(Arc::new(config)))
}

/// Sets the read timeout of the TCP stream under `socket`, TLS or not.
fn set_read_timeout(
    socket: &WebSocket<MaybeTlsStream<TcpStream>>,
    timeout: Duration,
) -> std::io::Result<()> {
    match socket.get_ref() {
        MaybeTlsStream::Plain(tcp) => tcp.set_read_timeout(Some(timeout)),
        #[cfg(feature = "tls")]
        MaybeTlsStream::Rustls(tls) => tls.sock.set_read_timeout(Some(timeout)),
        _ => Ok(()),
    }
}
