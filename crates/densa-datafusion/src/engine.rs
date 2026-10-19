use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::mpsc::{
    sync_channel, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError,
};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SQLOptions;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use futures::{FutureExt, StreamExt};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use crate::bridge::{batch_in, batches_out, schema_in};
use crate::numbers::RowNumbers;

/// Events handed over ahead of the one taken: a slow taker holds the query back.
const QUEUED: usize = 64;
/// How often a full queue is looked at again (the runtime's other work goes on meanwhile).
const QUEUE_FULL_TICK: Duration = Duration::from_millis(2);

/// A DataFusion session with tables registered by name, and the runtime its queries run on.
///
/// Queries run on the engine's own tokio runtime (one worker per core by default; see
/// [`Engine::with_threads`] to leave cores to the interface), so apps need none, and an app
/// that has one may use the engine from it too: registering blocks the calling thread but
/// never panics there, and dropping never waits for the runtime's threads.
pub struct Engine {
    runtime: Arc<Workers>,
    context: SessionContext,
    threads: usize,
}

/// The runtime, shut down without waiting when its last owner goes: an engine or a query
/// may be dropped from async code, where waiting for threads is not allowed.
struct Workers(Option<Runtime>);

impl Workers {
    fn get(&self) -> Result<&Runtime, EngineError> {
        self.0
            .as_ref()
            .ok_or_else(|| EngineError::Runtime("the runtime has shut down".to_owned()))
    }
}

impl Drop for Workers {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

impl Engine {
    /// An engine with a worker thread per core.
    ///
    /// # Errors
    /// [`EngineError::Runtime`] if the runtime's threads cannot start.
    pub fn new() -> Result<Self, EngineError> {
        let threads = std::thread::available_parallelism().map_or(4, usize::from);
        Self::with_threads(threads)
    }

    /// An engine with `threads` worker threads (at least one), which also bound how many
    /// partitions a query runs at once.
    ///
    /// # Errors
    /// [`EngineError::Runtime`] if the runtime's threads cannot start.
    pub fn with_threads(threads: usize) -> Result<Self, EngineError> {
        let threads = threads.max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads)
            .thread_name("densa-datafusion")
            .enable_all()
            .build()
            .map_err(|error| EngineError::Runtime(error.to_string()))?;
        let config = SessionConfig::new().with_target_partitions(threads);
        Ok(Self {
            runtime: Arc::new(Workers(Some(runtime))),
            context: SessionContext::new_with_config(config),
            threads,
        })
    }

    /// Registers the Parquet file (or directory of files) at `path` as table `name`, reading
    /// its metadata before returning.
    ///
    /// Names are taken as they are: in SQL, a name with capitals, dots or spaces needs double
    /// quotes (`"Sales 2026"`), as unquoted names are folded to lowercase.
    ///
    /// # Errors
    /// [`EngineError::Register`] if it cannot be read or the name is taken.
    pub fn register_parquet(&self, name: &str, path: impl AsRef<Path>) -> Result<(), EngineError> {
        let path = text(path.as_ref())?;
        let table = TableReference::bare(name);
        let registering =
            self.context
                .register_parquet(table, &path, ParquetReadOptions::default());
        self.block_on(registering)?
            .map_err(|error| EngineError::Register(error.to_string()))
    }

    /// Registers the CSV file (or directory of files) at `path` as table `name`, with a
    /// header line and column types inferred from the first records. Names are taken as for
    /// [`register_parquet`](Self::register_parquet).
    ///
    /// # Errors
    /// [`EngineError::Register`] if it cannot be read or the name is taken.
    pub fn register_csv(&self, name: &str, path: impl AsRef<Path>) -> Result<(), EngineError> {
        let path = text(path.as_ref())?;
        let table = TableReference::bare(name);
        let registering = self
            .context
            .register_csv(table, &path, CsvReadOptions::new());
        self.block_on(registering)?
            .map_err(|error| EngineError::Register(error.to_string()))
    }

    /// Registers `batches` of `schema` as table `name` (copied: later changes to the source
    /// are not seen; register again to query them), spread over the engine's threads so
    /// scans run in parallel. Names are taken as for
    /// [`register_parquet`](Self::register_parquet).
    ///
    /// # Errors
    /// [`EngineError::Register`] if a batch is not of `schema` or the name is taken.
    pub fn register_batches(
        &self,
        name: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<(), EngineError> {
        let (schema, batches) = batches_out(schema, batches).map_err(EngineError::Register)?;
        let mut partitions = vec![Vec::new(); self.threads.min(batches.len()).max(1)];
        let count = partitions.len();
        for (i, batch) in batches.into_iter().enumerate() {
            partitions[i % count].push(batch);
        }
        let table = MemTable::try_new(schema, partitions)
            .map_err(|error| EngineError::Register(error.to_string()))?;
        self.context
            .register_table(TableReference::bare(name), Arc::new(table))
            .map(|_| ())
            .map_err(|error| EngineError::Register(error.to_string()))
    }

    /// Forgets table `name`. Whether there was one.
    ///
    /// # Errors
    /// [`EngineError::Register`] if the catalog refuses (a name it cannot hold).
    pub fn deregister(&self, name: &str) -> Result<bool, EngineError> {
        self.context
            .deregister_table(TableReference::bare(name))
            .map(|table| table.is_some())
            .map_err(|error| EngineError::Register(error.to_string()))
    }

    /// A SQL query to run: set what differs, then [`Query::run`]. Queries only read unless
    /// [`Query::allow_changes`] is set.
    pub fn sql(&self, query: impl Into<String>) -> Query<'_> {
        Query {
            engine: self,
            query: query.into(),
            row_numbers: None,
            changes: false,
        }
    }

    /// Runs `future` on the runtime from a thread of its own, so the calling thread may be
    /// inside an async runtime (where blocking on another would panic).
    fn block_on<F>(&self, future: F) -> Result<F::Output, EngineError>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let handle = self.runtime.get()?.handle().clone();
        std::thread::scope(|scope| scope.spawn(move || handle.block_on(future)).join())
            .map_err(|_| EngineError::Register("registering panicked".to_owned()))
    }
}

/// A query about to run: made by [`Engine::sql`].
#[derive(Debug)]
#[must_use = "a query does nothing until it is run"]
pub struct Query<'a> {
    engine: &'a Engine,
    query: String,
    row_numbers: Option<String>,
    changes: bool,
}

impl Query<'_> {
    /// Adds a column of row numbers named `name` (an unsigned integer from 0, never null, in
    /// the order rows arrive): a key for a table when the result has none. A result that
    /// already has a column of that name fails.
    pub fn row_numbers(mut self, name: impl Into<String>) -> Self {
        self.row_numbers = Some(name.into());
        self
    }

    /// Lets the query change things: create or drop tables (`CREATE TABLE`, `CREATE
    /// EXTERNAL TABLE`, which reads any path), write files (`COPY ... TO`, `INSERT`), and
    /// change the session's settings (`SET`) for every query after it. Without it such a
    /// query fails: give it when the SQL comes from someone trusted with all of that.
    pub fn allow_changes(mut self) -> Self {
        self.changes = true;
        self
    }

    /// Runs the query in the background, calling `wake` after each event it hands over (give
    /// it something cheap that asks the interface for a frame). Unlike connecting to a
    /// service, running cannot fail here: planning errors (bad SQL, an unknown table) arrive
    /// as [`QueryEvent::Failed`]. Dropping the stream cancels the query.
    pub fn run(self, wake: impl Fn() + Send + 'static) -> QueryStream {
        let (sender, events) = sync_channel(QUEUED);
        let runtime = match self.engine.runtime.get() {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = sender.try_send(QueryEvent::Failed {
                    reason: error.to_string(),
                });
                return QueryStream {
                    events,
                    task: None,
                    _runtime: self.engine.runtime.clone(),
                    finished: false,
                };
            }
        };
        let context = self.engine.context.clone();
        let options = if self.changes {
            SQLOptions::new()
        } else {
            SQLOptions::new()
                .with_allow_ddl(false)
                .with_allow_dml(false)
                .with_allow_statements(false)
        };
        let job = Job {
            query: self.query,
            row_numbers: self.row_numbers,
            options,
        };
        let task = runtime.spawn(async move {
            let mut sender = Sender { sender, wake };
            let outcome = AssertUnwindSafe(job.run(&context, &mut sender))
                .catch_unwind()
                .await;
            let event = match outcome {
                Ok(Ok(true)) => QueryEvent::Finished,
                Ok(Ok(false)) => return,
                Ok(Err(reason)) => QueryEvent::Failed { reason },
                Err(_) => {
                    // The query or `wake` panicked: say so without calling `wake` the usual
                    // way, which may be what panicked.
                    let reason = "the query panicked".to_owned();
                    sender.send_last(QueryEvent::Failed { reason }).await;
                    return;
                }
            };
            sender.send(event).await;
        });
        QueryStream {
            events,
            task: Some(task),
            _runtime: self.engine.runtime.clone(),
            finished: false,
        }
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("session", &self.context.session_id())
            .field("threads", &self.threads)
            .finish_non_exhaustive()
    }
}

/// `path` as the text DataFusion takes.
fn text(path: &Path) -> Result<String, EngineError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| EngineError::Register(format!("{} is not valid Unicode", path.display())))
}

/// What a running query needs.
struct Job {
    query: String,
    row_numbers: Option<String>,
    options: SQLOptions,
}

impl Job {
    /// Plans and runs the query, handing over its schema and batches. Whether the taker
    /// stayed to the end.
    async fn run<W: Fn()>(
        &self,
        context: &SessionContext,
        sender: &mut Sender<W>,
    ) -> Result<bool, String> {
        let frame = context
            .sql_with_options(&self.query, self.options)
            .await
            .map_err(|error| error.to_string())?;
        let mut batches = frame
            .execute_stream()
            .await
            .map_err(|error| error.to_string())?;
        let schema = schema_in(&batches.schema())?;
        let mut numbers = match &self.row_numbers {
            Some(name) => Some(RowNumbers::new(name, &schema)?),
            None => None,
        };
        let shown = numbers.as_ref().map_or(schema, RowNumbers::schema);
        if !sender.send(QueryEvent::Schema(shown)).await {
            return Ok(false);
        }
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|error| error.to_string())?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = batch_in(&batch)?;
            let batch = match &mut numbers {
                Some(numbers) => numbers.add(&batch)?,
                None => batch,
            };
            if !sender.send(QueryEvent::Batch(batch)).await {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// The query's end of the queue.
struct Sender<W> {
    sender: SyncSender<QueryEvent>,
    wake: W,
}

impl<W: Fn()> Sender<W> {
    /// Hands `event` over and wakes the taker. Whether the taker is still there. While the
    /// queue is full it waits without holding a worker thread.
    async fn send(&mut self, event: QueryEvent) -> bool {
        let sent = self.queue(event).await;
        if sent {
            (self.wake)();
        }
        sent
    }

    /// Hands over the last event, waking the taker even if `wake` panics.
    async fn send_last(&mut self, event: QueryEvent) {
        if self.queue(event).await {
            let wake = &self.wake;
            let _ = std::panic::catch_unwind(AssertUnwindSafe(wake));
        }
    }

    async fn queue(&mut self, mut event: QueryEvent) -> bool {
        loop {
            match self.sender.try_send(event) {
                Ok(()) => return true,
                Err(TrySendError::Disconnected(_)) => return false,
                Err(TrySendError::Full(back)) => {
                    event = back;
                    tokio::time::sleep(QUEUE_FULL_TICK).await;
                }
            }
        }
    }
}

/// What happened to a query, in order.
#[derive(Debug)]
#[non_exhaustive]
pub enum QueryEvent {
    /// The result's columns, before its first batch.
    Schema(SchemaRef),
    /// Rows of the result.
    Batch(RecordBatch),
    /// The whole result was handed over; nothing follows.
    Finished,
    /// The query could not be planned or failed while running; nothing follows. The reason
    /// is DataFusion's, which may run over several lines.
    Failed {
        /// Why.
        reason: String,
    },
}

/// A query running in the background: made by [`Query::run`]. Dropping it cancels the
/// query.
#[derive(Debug)]
#[must_use = "dropping the stream cancels the query"]
pub struct QueryStream {
    events: Receiver<QueryEvent>,
    task: Option<JoinHandle<()>>,
    /// Keeps the runtime alive for the query, even if the engine is dropped first.
    _runtime: Arc<Workers>,
    finished: bool,
}

impl fmt::Debug for Workers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() {
            "Workers"
        } else {
            "Workers(shut down)"
        })
    }
}

impl QueryStream {
    /// The next event, if one is ready: never waits.
    #[must_use]
    pub fn try_next(&mut self) -> Option<QueryEvent> {
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
    pub fn wait_next(&mut self, timeout: Duration) -> Option<QueryEvent> {
        match self.events.recv_timeout(timeout) {
            Ok(event) => Some(self.seen(event)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
                None
            }
        }
    }

    /// Whether the query has ended: [`QueryEvent::Finished`] or [`QueryEvent::Failed`] was
    /// taken.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    fn seen(&mut self, event: QueryEvent) -> QueryEvent {
        if matches!(event, QueryEvent::Finished | QueryEvent::Failed { .. }) {
            self.finished = true;
        }
        event
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Why the engine could not start or take a table.
#[derive(Debug)]
#[non_exhaustive]
pub enum EngineError {
    /// The runtime's threads could not start.
    Runtime(String),
    /// A table could not be registered.
    Register(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(reason) => write!(f, "cannot start the query runtime: {reason}"),
            Self::Register(reason) => write!(f, "cannot register the table: {reason}"),
        }
    }
}

impl std::error::Error for EngineError {}
