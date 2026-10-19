use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{
    sync_channel, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightClient, FlightDescriptor, FlightEndpoint, Location, Ticket};
use arrow_schema::{Fields, Schema, SchemaRef};
use futures::StreamExt;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Endpoint, Uri};
use tonic::Code;

use crate::numbers::RowNumbers;
use crate::options::FlightRequest;
use crate::{FlightOptions, RequestError};

/// What happened on a Flight stream, in order.
#[derive(Debug)]
#[non_exhaustive]
pub enum FlightEvent {
    /// The result's columns (with the row number column last, if asked for), before its
    /// first batch; again after each retry.
    Schema(SchemaRef),
    /// Rows of the result.
    Batch(RecordBatch),
    /// Every endpoint was read to its end; nothing follows.
    Finished,
    /// A read failed in a way that may pass, and is asked for again after `retry_in` (only
    /// with [`FlightOptions::retry`]). The result starts over: rows handed over before are
    /// handed over again, so key them (rows the source dropped meanwhile stay unless the
    /// table is cleared).
    Retrying {
        /// Why.
        reason: String,
        /// How long until the next try.
        retry_in: Duration,
    },
    /// The stream stopped for good; nothing follows.
    Failed {
        /// Why.
        reason: String,
    },
}

/// Events handed over ahead of the one taken: a slow taker holds the reading back.
const QUEUED: usize = 256;
/// How long opening a connection may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// HTTP/2 pings on a quiet connection, and how long one may go unanswered: a peer that
/// vanished is noticed even while a stream waits for rows.
const KEEP_ALIVE: Duration = Duration::from_secs(20);
/// The longest the thread goes without looking at whether it should stop.
const MAX_TICK: Duration = Duration::from_millis(100);
/// How often a full queue is looked at again: the runtime keeps serving the connection
/// (pings, flow control) meanwhile.
const QUEUE_FULL_TICK: Duration = Duration::from_millis(2);
/// The location that means "the connection this came from" (Flight 15+).
const REUSE_CONNECTION: &str = "arrow-flight-reuse-connection:";

/// An Arrow Flight result read on a thread of its own, its rows handed over as Arrow
/// record batches: made by [`FlightOptions::connect`].
///
/// The thread runs a small tokio runtime of its own. Dropping the stream stops it within a
/// tenth of a second, cancelling the call in progress.
#[derive(Debug)]
pub struct FlightStream {
    events: Receiver<FlightEvent>,
    stop: Arc<AtomicBool>,
    finished: bool,
}

impl FlightStream {
    pub(crate) fn spawn(
        options: FlightOptions,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, RequestError> {
        let origin = Target::parse(&options.url).map_err(RequestError::InvalidUrl)?;
        if origin.tls && cfg!(not(feature = "tls")) {
            return Err(RequestError::TlsDisabled);
        }
        let headers = headers(&options.headers)?;
        let (sender, events) = sync_channel(QUEUED);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let last_word = sender.clone();
        thread::Builder::new()
            .name("densa-flight stream".to_owned())
            .spawn(move || {
                let reader = Reader {
                    options,
                    origin,
                    headers,
                    sender,
                    stop: stopping,
                    wake: &wake,
                };
                let outcome = catch_unwind(AssertUnwindSafe(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map(|runtime| runtime.block_on(reader.run()))
                }));
                let reason = match outcome {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(format!("cannot start a runtime: {error}")),
                    Err(_) => Some("the reading thread panicked".to_owned()),
                };
                if let Some(reason) = reason {
                    // Blocks while the queue is full: the taker must learn why it ended. It
                    // fails at once if the stream was dropped.
                    let _ = last_word.send(FlightEvent::Failed { reason });
                }
                drop(last_word);
                let _ = catch_unwind(AssertUnwindSafe(&wake));
            })
            .map_err(RequestError::Io)?;
        Ok(Self {
            events,
            stop,
            finished: false,
        })
    }

    /// The next event, if one is ready: never waits.
    #[must_use]
    pub fn try_next(&mut self) -> Option<FlightEvent> {
        match self.events.try_recv() {
            Ok(event) => Some(self.seen(event)),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.finished = true;
                None
            }
        }
    }

    /// The next event, waiting at most `timeout` for it.
    #[must_use]
    pub fn wait_next(&mut self, timeout: Duration) -> Option<FlightEvent> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Some(self.seen(event)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
                None
            }
        }
    }

    /// Whether the result has ended: [`FlightEvent::Finished`] or [`FlightEvent::Failed`]
    /// was taken (or the reading thread ended without one).
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn seen(&mut self, event: FlightEvent) -> FlightEvent {
        if matches!(event, FlightEvent::Finished | FlightEvent::Failed { .. }) {
            self.finished = true;
        }
        event
    }
}

impl Drop for FlightStream {
    fn drop(&mut self) {
        // Relaxed: a plain flag the thread polls; nothing else is published through it.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Where to connect: an `http://` or `https://` URI for tonic.
#[derive(Debug, Clone)]
struct Target {
    uri: String,
    tls: bool,
    /// Host and port, lowercase: whether two targets are one service.
    authority: String,
}

impl Target {
    /// A Flight URL (`grpc://`, `grpc+tcp://`, `grpc+tls://`, `http://`, `https://`) with a
    /// host and no credentials.
    fn parse(url: &str) -> Result<Self, String> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| format!("{url} has no scheme"))?;
        let tls = match scheme.to_ascii_lowercase().as_str() {
            "grpc" | "grpc+tcp" | "http" => false,
            "grpc+tls" | "https" => true,
            _ => {
                return Err(format!(
                    "{url} is not grpc://, grpc+tls://, http:// or https://"
                ))
            }
        };
        let uri = format!("{}://{rest}", if tls { "https" } else { "http" });
        let parsed: Uri = uri.parse().map_err(|error| format!("{url}: {error}"))?;
        let authority = parsed.authority().map(|a| a.as_str()).unwrap_or_default();
        if authority.contains('@') {
            // A URL ends up in messages: credentials go in headers.
            return Err("a Flight URL cannot hold credentials: give them as a header".to_owned());
        }
        let host = parsed.host().unwrap_or_default();
        if host.is_empty() {
            return Err(format!("{url} has no host"));
        }
        let port = parsed.port_u16().unwrap_or(if tls { 443 } else { 80 });
        Ok(Self {
            authority: format!("{}:{port}", host.to_ascii_lowercase()),
            uri,
            tls,
        })
    }
}

/// Where an endpoint's data can be read.
enum Place {
    /// The service the request went to.
    Origin,
    /// Another service.
    At(Target),
}

/// The places an endpoint names, in the order given, or why it names none that can be used.
fn places(locations: &[Location]) -> Result<Vec<Place>, String> {
    if locations.is_empty() {
        return Ok(vec![Place::Origin]);
    }
    let mut places = Vec::new();
    let mut unusable = String::new();
    for location in locations {
        let reuse = location
            .uri
            .get(..REUSE_CONNECTION.len())
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case(REUSE_CONNECTION));
        if reuse {
            places.push(Place::Origin);
            continue;
        }
        match Target::parse(&location.uri) {
            Ok(target) => places.push(Place::At(target)),
            Err(reason) => unusable = reason,
        }
    }
    if places.is_empty() {
        return Err(format!("no usable location for an endpoint ({unusable})"));
    }
    Ok(places)
}

type Headers = Vec<(AsciiMetadataKey, AsciiMetadataValue)>;

fn headers(given: &[(String, String)]) -> Result<Headers, RequestError> {
    given
        .iter()
        .map(|(name, value)| {
            let key = AsciiMetadataKey::from_bytes(name.as_bytes())
                .map_err(|error| RequestError::InvalidHeader(format!("{name:?}: {error}")))?;
            // The value stays out of the message: it is often a secret.
            let value = AsciiMetadataValue::try_from(value.as_str()).map_err(|error| {
                RequestError::InvalidHeader(format!("the value of {name}: {error}"))
            })?;
            Ok((key, value))
        })
        .collect()
}

/// Why a try ended without finishing.
enum End {
    /// The stream was dropped.
    Stopped,
    /// Worth trying again (with [`FlightOptions::retry`]).
    Failed(String),
    /// Trying again would fail the same way.
    Fatal(String),
}

impl End {
    /// `error` from the service, by whether asking again could help.
    fn from_flight(what: &str, error: &FlightError) -> Self {
        let reason = describe(what, error);
        let passing = match error {
            FlightError::Tonic(status) => passing(status.code()),
            FlightError::ExternalError(_) => true,
            // Data the service sent that cannot be read: it would send it again.
            _ => false,
        };
        if passing {
            Self::Failed(reason)
        } else {
            Self::Fatal(reason)
        }
    }
}

/// Whether a call that ended with `code` may succeed when made again. A message over the
/// size limit is `OutOfRange` (tonic's own check): it would be over again.
fn passing(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Aborted
            | Code::Cancelled
            | Code::Unknown
            | Code::Internal
            | Code::ResourceExhausted
    )
}

/// The reading thread's state.
struct Reader<'a, W: Fn()> {
    options: FlightOptions,
    origin: Target,
    headers: Headers,
    sender: SyncSender<FlightEvent>,
    stop: Arc<AtomicBool>,
    wake: &'a W,
}

/// One try's state: connections by URI, and what was announced.
struct Try {
    clients: HashMap<String, FlightClient>,
    /// The first endpoint's columns, which every batch must have.
    columns: Option<Fields>,
    numbers: Option<RowNumbers>,
    delivered: bool,
}

impl<W: Fn()> Reader<'_, W> {
    async fn run(self) {
        let mut attempt = 0_u32;
        loop {
            let mut this = Try {
                clients: HashMap::new(),
                columns: None,
                numbers: None,
                delivered: false,
            };
            let reason = match self.read(&mut this).await {
                Ok(()) => {
                    self.send(FlightEvent::Finished).await;
                    return;
                }
                Err(End::Stopped) => return,
                Err(End::Fatal(reason)) => {
                    self.send(FlightEvent::Failed { reason }).await;
                    return;
                }
                Err(End::Failed(reason)) => reason,
            };
            let Some((first, most)) = self.options.retry else {
                self.send(FlightEvent::Failed { reason }).await;
                return;
            };
            if this.delivered {
                attempt = 0;
            }
            let retry_in = first
                .saturating_mul(2_u32.saturating_pow(attempt.min(20)))
                .min(most);
            attempt = attempt.saturating_add(1);
            if !self.send(FlightEvent::Retrying { reason, retry_in }).await
                || !self.sleep(retry_in).await
            {
                return;
            }
        }
    }

    /// One try: asks for the result and hands over every batch of every endpoint, in order.
    async fn read(&self, this: &mut Try) -> Result<(), End> {
        let origin = self.origin.clone();
        self.client(this, &origin).await?;
        let (endpoints, announced) = match &self.options.request {
            FlightRequest::Ticket(ticket) => {
                let ticket = Ticket::new(ticket.clone());
                (vec![FlightEndpoint::new().with_ticket(ticket)], None)
            }
            request => {
                let client = this.clients.get_mut(&origin.uri).ok_or_else(lost_client)?;
                let mut info = self
                    .wait(client.get_flight_info(descriptor(request)?))
                    .await?
                    .map_err(|error| End::from_flight("GetFlightInfo failed", &error))?;
                let endpoints = std::mem::take(&mut info.endpoint);
                (endpoints, Schema::try_from(info).ok().map(Arc::new))
            }
        };
        for endpoint in endpoints {
            let ticket = endpoint
                .ticket
                .ok_or_else(|| End::Fatal("an endpoint without a ticket".to_owned()))?;
            let places = places(&endpoint.location).map_err(End::Fatal)?;
            let uri = self.reach(this, places).await?;
            let client = this.clients.get_mut(&uri).ok_or_else(lost_client)?;
            let mut batches = self
                .wait(client.do_get(ticket))
                .await?
                .map_err(|error| End::from_flight("DoGet failed", &error))?;
            loop {
                let next = self.wait(batches.next()).await?;
                if this.columns.is_none() {
                    if let Some(schema) = batches.schema() {
                        self.announce(this, schema.clone()).await?;
                    }
                }
                let batch = match next {
                    None => break,
                    Some(Err(error)) => return Err(End::from_flight("reading failed", &error)),
                    Some(Ok(batch)) => batch,
                };
                self.deliver(this, batch).await?;
            }
        }
        if this.columns.is_none() {
            if let Some(schema) = announced {
                self.announce(this, schema).await?;
            }
        }
        Ok(())
    }

    /// Connects to the first of `places` that answers; the URI of its client.
    async fn reach(&self, this: &mut Try, places: Vec<Place>) -> Result<String, End> {
        let mut last = String::new();
        let mut tried = false;
        for place in places {
            let target = match place {
                Place::Origin => self.origin.clone(),
                // Data meant to be private does not go on in the clear.
                Place::At(target) if self.origin.tls && !target.tls => {
                    last = format!("{} is not TLS, unlike {}", target.uri, self.origin.uri);
                    continue;
                }
                Place::At(target) if target.tls && cfg!(not(feature = "tls")) => {
                    last = format!("{} needs the tls feature", target.uri);
                    continue;
                }
                Place::At(target) => target,
            };
            tried = true;
            match self.client(this, &target).await {
                Ok(()) => return Ok(target.uri),
                Err(End::Failed(reason)) => last = reason,
                Err(end) => return Err(end),
            }
        }
        // Places skipped without trying them would be skipped again.
        Err(if tried {
            End::Failed(last)
        } else {
            End::Fatal(last)
        })
    }

    /// Hands over `batch` (numbered, if asked for), checking it has the result's columns.
    async fn deliver(&self, this: &mut Try, batch: RecordBatch) -> Result<(), End> {
        let schema = batch.schema();
        match &this.columns {
            None => self.announce(this, schema.clone()).await?,
            Some(columns) if columns != schema.fields() => {
                return Err(End::Fatal(format!(
                    "a batch whose columns differ from the result's: {schema}"
                )))
            }
            Some(_) => {}
        }
        let batch = match &mut this.numbers {
            Some(numbers) => numbers.add(&batch).map_err(End::Fatal)?,
            None => batch,
        };
        if batch.num_rows() == 0 {
            return Ok(());
        }
        this.delivered = true;
        if self.send(FlightEvent::Batch(batch)).await {
            Ok(())
        } else {
            Err(End::Stopped)
        }
    }

    /// Hands over the result's schema (with row numbers, if asked for), and from then on
    /// numbers rows and checks columns by it.
    async fn announce(&self, this: &mut Try, schema: SchemaRef) -> Result<(), End> {
        this.numbers = match &self.options.row_numbers {
            Some(name) => Some(RowNumbers::new(name, &schema).map_err(End::Fatal)?),
            None => None,
        };
        this.columns = Some(schema.fields().clone());
        let shown = this.numbers.as_ref().map_or(schema, RowNumbers::schema);
        if self.send(FlightEvent::Schema(shown)).await {
            Ok(())
        } else {
            Err(End::Stopped)
        }
    }

    /// Makes sure `this` has a client of the service at `target`. The headers go only to
    /// the service the request was made to: a location elsewhere does not get its tokens.
    async fn client(&self, this: &mut Try, target: &Target) -> Result<(), End> {
        if this.clients.contains_key(&target.uri) {
            return Ok(());
        }
        let endpoint = Endpoint::from_shared(target.uri.clone())
            .map_err(|error| End::Fatal(describe(&target.uri, &error)))?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_nodelay(true)
            .http2_keep_alive_interval(KEEP_ALIVE)
            .keep_alive_timeout(KEEP_ALIVE)
            .keep_alive_while_idle(true);
        let endpoint = secure(endpoint, target)?;
        let channel = self.wait(endpoint.connect()).await?.map_err(|error| {
            End::Failed(describe(
                &format!("cannot connect to {}", target.uri),
                &error,
            ))
        })?;
        let inner =
            FlightServiceClient::new(channel).max_decoding_message_size(self.options.max_message);
        let mut client = FlightClient::new_from_inner(inner);
        if target.authority == self.origin.authority {
            for (key, value) in &self.headers {
                client.metadata_mut().insert(key.clone(), value.clone());
            }
        }
        this.clients.insert(target.uri.clone(), client);
        Ok(())
    }

    /// Runs `future` to its end, unless the stream is dropped first.
    async fn wait<F: Future>(&self, future: F) -> Result<F::Output, End> {
        let mut future = std::pin::pin!(future);
        loop {
            if let Ok(output) = tokio::time::timeout(MAX_TICK, future.as_mut()).await {
                return Ok(output);
            }
            if self.stopped() {
                return Err(End::Stopped);
            }
        }
    }

    /// Hands `event` over and wakes the taker. Whether the taker is still there. While the
    /// queue is full it waits without blocking the runtime, which keeps the connection
    /// alive.
    async fn send(&self, mut event: FlightEvent) -> bool {
        loop {
            match self.sender.try_send(event) {
                Ok(()) => {
                    (self.wake)();
                    return true;
                }
                Err(TrySendError::Disconnected(_)) => return false,
                // A dropped stream drops its receiver too: then this is Disconnected.
                Err(TrySendError::Full(back)) => {
                    event = back;
                    tokio::time::sleep(QUEUE_FULL_TICK).await;
                }
            }
        }
    }

    fn stopped(&self) -> bool {
        // Relaxed: see `FlightStream::drop`.
        self.stop.load(Ordering::Relaxed)
    }

    /// Waits `time`, looking at the stop flag as it goes. Whether to go on.
    async fn sleep(&self, time: Duration) -> bool {
        self.wait(tokio::time::sleep(time)).await.is_ok()
    }
}

/// A client that `client` or `reach` just made is in the map; this is never reached.
fn lost_client() -> End {
    End::Fatal("a connection went missing".to_owned())
}

/// The descriptor `GetFlightInfo` takes for `request`.
fn descriptor(request: &FlightRequest) -> Result<FlightDescriptor, End> {
    match request {
        FlightRequest::Path(path) => Ok(FlightDescriptor::new_path(path.clone())),
        FlightRequest::Command(command) => Ok(FlightDescriptor::new_cmd(command.clone())),
        #[cfg(feature = "sql")]
        FlightRequest::Sql(query) => {
            use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
            use prost::Message;
            let command = CommandStatementQuery {
                query: query.clone(),
                transaction_id: None,
            };
            Ok(FlightDescriptor::new_cmd(command.as_any().encode_to_vec()))
        }
        FlightRequest::Ticket(_) => Err(End::Fatal("a ticket has no descriptor".to_owned())),
    }
}

/// `endpoint` with TLS when `target` asks for it.
fn secure(endpoint: Endpoint, target: &Target) -> Result<Endpoint, End> {
    if !target.tls {
        return Ok(endpoint);
    }
    #[cfg(feature = "tls")]
    {
        let config = tonic::transport::ClientTlsConfig::new().with_webpki_roots();
        endpoint
            .tls_config(config)
            .map_err(|error| End::Fatal(describe("TLS", &error)))
    }
    #[cfg(not(feature = "tls"))]
    {
        Err(End::Fatal(format!("{} needs the tls feature", target.uri)))
    }
}

/// `what`, then `error` and every error under it (tonic's own messages say little).
fn describe(what: &str, error: &dyn Error) -> String {
    let mut text = format!("{what}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.ends_with(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}
