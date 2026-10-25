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
use datafusion::datasource::file_format::options::ReadOptions;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use futures::{FutureExt, StreamExt};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use url::Url;

use crate::bridge::{batch_in, batches_out, schema_in};
use crate::numbers::RowNumbers;

/// Events handed over ahead of the one taken: a slow taker holds the query back.
const QUEUED: usize = 64;
/// How often a full queue is looked at again (the runtime's other work goes on meanwhile).
const QUEUE_FULL_TICK: Duration = Duration::from_millis(2);
/// The most a sorting partition sets aside to merge what it spilled: DataFusion's default.
const MERGE_ROOM: usize = 10 << 20;

/// A DataFusion session with tables registered by name, and the runtime its queries run on.
///
/// Queries run on the engine's own tokio runtime (one worker per core by default; see
/// [`Engine::with_threads`] to leave cores to the interface), so apps need none, and an app
/// that has one may use the engine from it too: registering blocks the calling thread but
/// never panics there, and dropping never waits for the runtime's threads.
pub struct Engine {
    pub(crate) runtime: Arc<Workers>,
    pub(crate) context: SessionContext,
    threads: usize,
    /// The memory its queries may hold between them, if limited.
    memory: Option<usize>,
}

/// The runtime, shut down without waiting when its last owner goes: an engine or a query
/// may be dropped from async code, where waiting for threads is not allowed.
pub(crate) struct Workers(Option<Runtime>);

impl Workers {
    fn get(&self) -> Result<&Runtime, EngineError> {
        self.0
            .as_ref()
            .ok_or_else(|| EngineError::Runtime("the runtime has shut down".to_owned()))
    }

    /// Runs `future` on the runtime from a thread of its own, so the calling thread may be
    /// inside an async runtime (where blocking on another would panic). `None` if it panicked.
    pub(crate) fn block_on<F>(&self, future: F) -> Result<Option<F::Output>, EngineError>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let handle = self.get()?.handle().clone();
        Ok(std::thread::scope(|scope| scope.spawn(move || handle.block_on(future)).join()).ok())
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
            memory: None,
        })
    }

    /// The engine with its queries held to `bytes` of memory between them (by default they
    /// are not held at all). Sorts and aggregates that would hold more spill to files in the
    /// temporary folder; what cannot spill (a join's table, say) fails, saying so. Tables
    /// already registered are kept.
    ///
    /// # Errors
    /// [`EngineError::Runtime`] if the memory pool cannot be made.
    pub fn with_memory_limit(mut self, bytes: usize) -> Result<Self, EngineError> {
        let state = self.context.state();
        // Each partition that sorts sets aside room to merge what it spilled (10 MiB unless
        // set): at most a quarter of the limit between them, so smaller limits still sort.
        let merge_room = (bytes / 4 / self.threads.max(1)).min(MERGE_ROOM);
        // Made anew, the default catalog would replace the shared one holding the tables.
        let config = state
            .config()
            .clone()
            .with_sort_spill_reservation_bytes(merge_room)
            .with_create_default_catalog_and_schema(false);
        // Shared out among the parts of a query that can spill, so one partition's sort
        // cannot take it all and leave the next none to start with.
        let pool = Arc::new(FairSpillPool::new(bytes));
        let runtime = RuntimeEnvBuilder::from_runtime_env(state.runtime_env())
            .with_memory_pool(pool)
            .build_arc()
            .map_err(|error| EngineError::Runtime(error.to_string()))?;
        let state = SessionStateBuilder::new_from_existing(state)
            .with_config(config)
            .with_runtime_env(runtime)
            .build();
        self.context = SessionContext::new_with_state(state);
        self.memory = Some(bytes);
        Ok(self)
    }

    /// Registers the Parquet file (or directory of files) at `path` as table `name`, reading
    /// its metadata before returning. A file is read whatever its name ends in, and a name
    /// is a name (`*`, `?` or `[` in it match nothing); a directory's files are those whose
    /// names end in `.parquet`.
    ///
    /// Names are taken as they are: in SQL, a name with capitals, dots or spaces needs double
    /// quotes (`"Sales 2026"`), as unquoted names are folded to lowercase.
    ///
    /// # Errors
    /// [`EngineError::Register`] if it cannot be read or the name is taken.
    pub fn register_parquet(&self, name: &str, path: impl AsRef<Path>) -> Result<(), EngineError> {
        let (url, extension) = listed(path.as_ref(), ".parquet")?;
        let options = ParquetReadOptions::default().file_extension(&extension);
        self.register_listed(name, url, self.listing(&options))
    }

    /// Registers the CSV file (or directory of files) at `path` as table `name`, with a
    /// header line and column types inferred from the first records. Files and names are
    /// taken as for [`register_parquet`](Self::register_parquet) (a directory's files are
    /// those whose names end in `.csv`).
    ///
    /// # Errors
    /// [`EngineError::Register`] if it cannot be read or the name is taken.
    pub fn register_csv(&self, name: &str, path: impl AsRef<Path>) -> Result<(), EngineError> {
        let (url, extension) = listed(path.as_ref(), ".csv")?;
        let options = CsvReadOptions::new().file_extension(&extension);
        self.register_listed(name, url, self.listing(&options))
    }

    /// `options` as the session's settings make them for a table.
    fn listing<'a>(&self, options: &impl ReadOptions<'a>) -> ListingOptions {
        let (config, table) = (
            self.context.copied_config(),
            self.context.copied_table_options(),
        );
        options.to_listing_options(&config, table)
    }

    /// Registers the files at `url`, read as `options` say, as table `name`.
    fn register_listed(
        &self,
        name: &str,
        url: ListingTableUrl,
        options: ListingOptions,
    ) -> Result<(), EngineError> {
        let table = TableReference::bare(name);
        let url = url.with_table_ref(table.clone());
        let context = self.context.clone();
        let registering = async move {
            let schema = options.infer_schema(&context.state(), &url).await?;
            let config = ListingTableConfig::new(url)
                .with_listing_options(options)
                .with_schema(schema);
            let cache = context
                .runtime_env()
                .cache_manager
                .get_file_statistic_cache();
            let listing = ListingTable::try_new(config)?.with_cache(cache);
            context.register_table(table, Arc::new(listing))?;
            Ok::<(), DataFusionError>(())
        };
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

    /// The columns `query` gives, without running it (planning reads the tables' schemas).
    ///
    /// # Errors
    /// [`EngineError::Query`] if the query cannot be planned (bad SQL, an unknown table) or
    /// would change things.
    pub fn schema(&self, query: &str) -> Result<SchemaRef, EngineError> {
        let context = self.context.clone();
        let planning = async move {
            let frame = context.sql_with_options(query, read_only()).await?;
            Ok::<_, datafusion::error::DataFusionError>(frame.schema().inner().clone())
        };
        let schema = self
            .runtime
            .block_on(planning)?
            .ok_or_else(|| EngineError::Query("planning panicked".to_owned()))?
            .map_err(|error| EngineError::Query(error.to_string()))?;
        schema_in(&schema).map_err(EngineError::Query)
    }

    /// Runs `future` on the runtime from a thread of its own (see `Workers::block_on`).
    fn block_on<F>(&self, future: F) -> Result<F::Output, EngineError>
    where
        F: Future + Send,
        F::Output: Send,
    {
        self.runtime
            .block_on(future)?
            .ok_or_else(|| EngineError::Register("registering panicked".to_owned()))
    }
}

/// Options that keep a query from changing anything: no tables made or dropped, no files
/// written, no settings changed.
pub(crate) fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
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
            read_only()
        };
        let job = Job {
            query: self.query,
            row_numbers: self.row_numbers,
            options,
            memory: self.engine.memory,
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

/// `path` as the text DataFusion takes. On Windows a verbatim path (`\\?\C:\data`, as
/// `std::fs::canonicalize` gives) loses its prefix: DataFusion makes a URL of the path, and
/// the URL parser panics on one.
fn text(path: &Path) -> Result<String, EngineError> {
    let text = path
        .to_str()
        .ok_or_else(|| EngineError::Register(format!("{} is not valid Unicode", path.display())))?;
    Ok(match cfg!(windows) {
        true => plain(text),
        false => text.to_owned(),
    })
}

/// Where `path` is, for DataFusion: a URL made of the path itself, so a name with `*`, `?` or
/// `[` is not taken for a pattern; and the end its files' names must have: none for a file
/// (named, so read whatever its name ends in), `folder_extension` for the files of a folder.
fn listed(path: &Path, folder_extension: &str) -> Result<(ListingTableUrl, String), EngineError> {
    let unreadable =
        |reason: String| EngineError::Register(format!("{}: {reason}", path.display()));
    let absolute = std::path::absolute(path).map_err(|error| unreadable(error.to_string()))?;
    let plain = text(&absolute)?;
    let folder = absolute.is_dir();
    let url = match folder {
        true => Url::from_directory_path(&plain),
        false => Url::from_file_path(&plain),
    }
    .map_err(|()| unreadable("no URL can be made of this path".to_owned()))?;
    let url = ListingTableUrl::try_new(url, None).map_err(|error| unreadable(error.to_string()))?;
    let extension = if folder { folder_extension } else { "" };
    Ok((url, extension.to_owned()))
}

/// A Windows path without a verbatim prefix: `\\?\C:\data` as `C:\data`, and
/// `\\?\UNC\server\share` as `\\server\share`.
fn plain(text: &str) -> String {
    if let Some(share) = text.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{share}");
    }
    text.strip_prefix(r"\\?\").unwrap_or(text).to_owned()
}

/// What a running query needs.
struct Job {
    query: String,
    row_numbers: Option<String>,
    options: SQLOptions,
    /// The memory the engine allows its queries, if limited.
    memory: Option<usize>,
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
            .map_err(|error| self.reason(error))?;
        let mut batches = frame
            .execute_stream()
            .await
            .map_err(|error| self.reason(error))?;
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
            let batch = batch.map_err(|error| self.reason(error))?;
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

    /// Why the query failed, as `error` says; one that needed more memory than the engine
    /// allows says how much it allows, and where that is set.
    fn reason(&self, error: DataFusionError) -> String {
        match (self.memory, error.find_root()) {
            (Some(bytes), DataFusionError::ResourcesExhausted(_)) => format!(
                "{error}: the query needs more than the {bytes} bytes of memory the engine \
                 allows its queries (Engine::with_memory_limit); ask for fewer rows or columns \
                 to be held at once, or allow more"
            ),
            _ => error.to_string(),
        }
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

/// Why the engine could not start, take a table or plan a query.
#[derive(Debug)]
#[non_exhaustive]
pub enum EngineError {
    /// The runtime's threads could not start.
    Runtime(String),
    /// A table could not be registered.
    Register(String),
    /// A query could not be planned (bad SQL, an unknown table or column), or would change
    /// things where only reading is allowed.
    Query(String),
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(reason) => write!(f, "cannot start the query runtime: {reason}"),
            Self::Register(reason) => write!(f, "cannot register the table: {reason}"),
            Self::Query(reason) => write!(f, "cannot plan the query: {reason}"),
        }
    }
}

impl std::error::Error for EngineError {}

#[cfg(test)]
mod tests {
    use super::plain;

    #[test]
    fn a_verbatim_windows_path_loses_its_prefix_and_others_stay() {
        assert_eq!(
            plain(r"\\?\C:\data\trips.parquet"),
            r"C:\data\trips.parquet"
        );
        assert_eq!(
            plain(r"\\?\UNC\server\share\trips"),
            r"\\server\share\trips"
        );
        assert_eq!(plain(r"C:\data\trips.parquet"), r"C:\data\trips.parquet");
        assert_eq!(plain(r"\\server\share\trips"), r"\\server\share\trips");
        assert_eq!(
            plain("/home/data/trips.parquet"),
            "/home/data/trips.parquet"
        );
    }
}
